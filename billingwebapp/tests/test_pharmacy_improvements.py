"""Tests for the security, GST, invoice-cancel, return and compliance fixes."""

import importlib
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta


class PharmacyImprovementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "improvements.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "improvements-test-secret"
        os.environ["CSRF_PROTECTION"] = "0"
        os.environ["APP_TIMEZONE"] = "Asia/Kolkata"
        os.environ["ENABLE_BACKGROUND_JOBS"] = "0"
        os.environ["APP_STORAGE_ROOT"] = os.path.join(cls.temp_dir.name, "uploads")
        os.environ["APP_BACKUP_ROOT"] = os.path.join(cls.temp_dir.name, "backups")

        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_dir not in sys.path:
            sys.path.insert(0, project_dir)
        if "app" in sys.modules:
            cls.app_module = importlib.reload(sys.modules["app"])
        else:
            cls.app_module = importlib.import_module("app")
        cls.app = cls.app_module.app
        cls.db = cls.app_module.db
        cls.app.config["TESTING"] = True

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def setUp(self):
        self.app.config["CSRF_PROTECTION"] = False
        self.client = self.app.test_client()
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()
            admin = self.app_module.User(
                username="admin", role="admin", access_profile="admin",
                can_manage_users=True, can_view_reports=True, can_invoice_action=True,
                can_edit_invoice=True, can_delete_invoice=True, can_view_medicine=True,
                can_add_medicine=True, can_edit_medicine=True, can_delete_medicine=True,
                can_view_stock_history=True, can_manage_purchases=True,
                can_view_audit_logs=True, can_view_profit_dashboard=True,
            )
            admin.set_password("Admin@123")
            self.db.session.add(admin)
            self.db.session.commit()

    # ---------------- helpers ----------------
    def login(self):
        response = self.client.post("/login", data={"username": "admin", "password": "Admin@123"})
        self.assertEqual(response.status_code, 302)

    def seed_stock(self, *, name="PARACETAMOL 650", batch="B123", qty=10, mrp=25.0, gst=5.0,
                   schedule="", rate=10.0, expiry="2027-12-31"):
        m = self.app_module
        vendor = m.Vendor(name=f"Vendor {name}")
        self.db.session.add(vendor)
        self.db.session.flush()
        med = m.Medicine(name=name, batch=batch, expiry=expiry, mrp=mrp, qty=qty,
                         discount_percent=0, is_active=True, gst_percent=gst,
                         schedule_type=schedule)
        self.db.session.add(med)
        self.db.session.flush()
        purchase = m.VendorPurchase(vendor_id=vendor.id, purchase_no=f"PB-{name[:3]}-{batch}",
                                    invoice_no=f"SUP-{name[:3]}-{batch}", purchase_date=datetime.utcnow(),
                                    payment_mode="CASH", payment_status="Paid", total_amount=qty * rate)
        self.db.session.add(purchase)
        self.db.session.flush()
        lot = m.VendorPurchaseItem(purchase_id=purchase.id, vendor_id=vendor.id, medicine_id=med.id,
                                   medicine_name=name, batch=batch, expiry=expiry, qty=qty, free_qty=0,
                                   remaining_qty=qty, purchase_rate=rate, mrp=mrp, gst_percent=gst,
                                   total_value=qty * rate)
        self.db.session.add(lot)
        self.db.session.commit()
        return med.id, lot.id

    def bill(self, name="PARACETAMOL 650", qty=2, batch="B123", doctor="Dr. Test"):
        return self.client.post("/billing", data={
            "customer": "Ravi Kumar", "mobile": "9876543210", "doctor": doctor,
            "gender": "MALE", "payment_mode": "CASH",
            "medicine_name": [name], "qty": [str(qty)], "batch_override[]": [batch],
        }, follow_redirects=True)

    def latest_invoice(self):
        return self.app_module.Invoice.query.order_by(self.app_module.Invoice.id.desc()).first()

    # ---------------- GST ----------------
    def test_inclusive_gst_split(self):
        split = self.app_module.split_inclusive_gst
        self.assertEqual(split(105, 5)["taxable_amount"], 100.0)
        self.assertEqual(split(105, 5)["cgst"], 2.5)
        self.assertEqual(split(105, 5)["sgst"], 2.5)
        self.assertEqual(split(112, 12)["gst_amount"], 12.0)
        self.assertEqual(split(100, 0)["gst_amount"], 0.0)

    def test_billing_uses_each_medicines_gst_rate_inclusively(self):
        with self.app.app_context():
            self.seed_stock(name="GLUCOMETER", batch="G1", mrp=1180.0, gst=18.0)
        self.login()
        response = self.bill(name="GLUCOMETER", qty=1, batch="G1")
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            inv = self.latest_invoice()
            item = self.app_module.InvoiceItem.query.filter_by(invoice_id=inv.id).first()
            # 1180 incl. 18% -> taxable 1000, GST 180 (CGST 90 + SGST 90)
            self.assertEqual(item.gst_percent, 18.0)
            self.assertAlmostEqual(item.gst_amount, 180.0, places=2)
            self.assertAlmostEqual(inv.cgst, 90.0, places=2)
            self.assertAlmostEqual(inv.sgst, 90.0, places=2)
            self.assertAlmostEqual(inv.total, 1180.0, places=2)

    # ---------------- Schedule H1 ----------------
    def test_schedule_h1_requires_doctor_and_appears_in_register(self):
        with self.app.app_context():
            self.seed_stock(name="ALPRAZOLAM 0.5", batch="X1", schedule="H1")
        self.login()
        blocked = self.bill(name="ALPRAZOLAM 0.5", qty=1, batch="X1", doctor="")
        self.assertIn("Doctor name is required", blocked.get_data(as_text=True))
        with self.app.app_context():
            self.assertIsNone(self.latest_invoice())
            med = self.app_module.Medicine.query.filter_by(batch="X1").first()
            self.assertEqual(med.qty, 10)
        ok = self.bill(name="ALPRAZOLAM 0.5", qty=1, batch="X1", doctor="Dr. Sharma")
        self.assertEqual(ok.status_code, 200)
        register = self.client.get("/reports/schedule-h1")
        html = register.get_data(as_text=True)
        self.assertIn("ALPRAZOLAM 0.5", html)
        self.assertIn("Dr. Sharma", html)

    # ---------------- Invoice cancel ----------------
    def test_cancel_invoice_keeps_record_restores_stock_and_lots(self):
        with self.app.app_context():
            med_id, lot_id = self.seed_stock()
        self.login()
        self.bill(qty=3)
        with self.app.app_context():
            inv_id = self.latest_invoice().id
            self.assertEqual(self.app_module.Medicine.query.get(med_id).qty, 7)
            self.assertEqual(self.app_module.VendorPurchaseItem.query.get(lot_id).remaining_qty, 7)

        # GET must not delete anything anymore.
        self.assertEqual(self.client.get(f"/delete-invoice/{inv_id}").status_code, 405)

        response = self.client.post(f"/invoice/cancel/{inv_id}", data={"reason": "Wrong patient"})
        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            inv = self.app_module.Invoice.query.get(inv_id)
            self.assertIsNotNone(inv, "cancelled invoice must stay in the database")
            self.assertTrue(inv.is_cancelled)
            self.assertEqual(inv.cancel_reason, "Wrong patient")
            self.assertEqual(self.app_module.InvoiceItem.query.filter_by(invoice_id=inv_id).count(), 1)
            self.assertEqual(self.app_module.Medicine.query.get(med_id).qty, 10)
            self.assertEqual(self.app_module.VendorPurchaseItem.query.get(lot_id).remaining_qty, 10)
            self.assertEqual(self.app_module.active_invoice_query().count(), 0)

        # Cancelling twice must not add stock twice.
        self.client.post(f"/invoice/cancel/{inv_id}", data={"reason": "again"})
        with self.app.app_context():
            self.assertEqual(self.app_module.Medicine.query.get(med_id).qty, 10)

        # Cancelled invoice cannot be returned.
        with self.app.app_context():
            inv_no = self.app_module.Invoice.query.get(inv_id).invoice_no
        resp = self.client.post("/return-medicine", data={
            "mode": "invoice", "invoice_no": inv_no, f"return_qty_1": "1", "payment_mode": "CASH",
        }, follow_redirects=True)
        self.assertIn("cancelled", resp.get_data(as_text=True).lower())

    # ---------------- Returns ----------------
    def _invoice_item(self):
        inv = self.latest_invoice()
        return inv, self.app_module.InvoiceItem.query.filter_by(invoice_id=inv.id).first()

    def test_damaged_return_goes_to_quarantine_not_stock(self):
        with self.app.app_context():
            med_id, lot_id = self.seed_stock()
        self.login()
        self.bill(qty=4)
        with self.app.app_context():
            inv, item = self._invoice_item()
            inv_no, item_id = inv.invoice_no, item.id
        response = self.client.post("/return-medicine", data={
            "mode": "invoice", "invoice_no": inv_no, f"return_qty_{item_id}": "2",
            f"disposition_{item_id}": "DAMAGED", f"reason_{item_id}": "Strip torn",
            "payment_mode": "CASH",
        })
        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 6, "damaged units must not become sellable")
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 6)
            q = m.QuarantineStock.query.one()
            self.assertEqual((q.qty, q.reason, q.status), (2, "DAMAGED", "PENDING"))
            ret = m.Return.query.one()
            self.assertEqual(ret.total_refund, 50.0)
            qid, ret_id = q.id, ret.id

        # Admin checks it and moves it back to stock -> stock and lot go up.
        self.client.post(f"/quarantine-stock/{qid}/restock")
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 8)
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 8)
            self.assertEqual(m.QuarantineStock.query.get(qid).status, "RESTOCKED")

        # Cancelling the return bill now reverses the restocked units.
        self.client.post(f"/return-bill/delete/{ret_id}")
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 6)
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 6)

    def test_good_return_restocks_and_uses_inclusive_gst(self):
        with self.app.app_context():
            med_id, lot_id = self.seed_stock(mrp=105.0)
        self.login()
        self.bill(qty=2)
        with self.app.app_context():
            inv, item = self._invoice_item()
            inv_no, item_id = inv.invoice_no, item.id
        self.client.post("/return-medicine", data={
            "mode": "invoice", "invoice_no": inv_no, f"return_qty_{item_id}": "1",
            f"disposition_{item_id}": "RESTOCK", "payment_mode": "CASH",
        })
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 9)
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 9)
            ret = m.Return.query.one()
            self.assertEqual(ret.total_refund, 105.0)
            self.assertEqual(ret.cgst, 2.5)
            self.assertEqual(ret.sgst, 2.5)

    def test_return_window_enforced_on_return_medicine_page(self):
        with self.app.app_context():
            self.seed_stock()
        self.login()
        self.bill(qty=2)
        with self.app.app_context():
            inv, item = self._invoice_item()
            inv.created_at = datetime.utcnow() - timedelta(days=30)
            self.db.session.commit()
            inv_no, item_id = inv.invoice_no, item.id
        blocked = self.client.post("/return-medicine", data={
            "mode": "invoice", "invoice_no": inv_no, f"return_qty_{item_id}": "1", "payment_mode": "CASH",
        }, follow_redirects=True)
        self.assertIn("within", blocked.get_data(as_text=True))
        with self.app.app_context():
            self.assertEqual(self.app_module.Return.query.count(), 0)
        allowed = self.client.post("/return-medicine", data={
            "mode": "invoice", "invoice_no": inv_no, f"return_qty_{item_id}": "1",
            "payment_mode": "CASH", "override_window": "1",
        })
        self.assertEqual(allowed.status_code, 302)
        with self.app.app_context():
            self.assertEqual(self.app_module.Return.query.count(), 1)

    def test_manual_return_refills_vendor_lot_and_cancel_reverses_it(self):
        with self.app.app_context():
            med_id, lot_id = self.seed_stock(qty=10)
            m = self.app_module
            # Simulate 5 units sold earlier (lot and stock both down).
            m.Medicine.query.get(med_id).qty = 5
            m.VendorPurchaseItem.query.get(lot_id).remaining_qty = 5
            self.db.session.commit()
        self.login()
        response = self.client.post("/return-medicine", data={
            "mode": "manual", "manual_customer": "Walk-in", "manual_payment_mode": "CASH",
            "manual_medicine_id": [str(med_id)], "manual_qty": ["2"], "manual_sold_qty": [""],
            "manual_selling_rate": ["25"], "manual_purchase_rate": [""], "manual_gst": [""],
            "manual_reason": ["Extra"], "manual_disposition": ["RESTOCK"],
        })
        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 7)
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 7, "lot must follow stock")
            ret_id = m.Return.query.one().id
        self.client.post(f"/return-bill/delete/{ret_id}")
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 5)
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 5)

    # ---------------- Stock safety ----------------
    def test_billing_never_takes_stock_below_zero(self):
        with self.app.app_context():
            med_id, _ = self.seed_stock(qty=2)
        self.login()
        response = self.bill(qty=3)
        self.assertIn("Not enough stock", response.get_data(as_text=True))
        with self.app.app_context():
            self.assertEqual(self.app_module.Medicine.query.get(med_id).qty, 2)
            self.assertIsNone(self.latest_invoice())

    def test_duplicate_medicine_batch_cannot_be_added(self):
        with self.app.app_context():
            self.seed_stock()
        self.login()
        self.client.post("/medicines/add", data={
            "name": "paracetamol 650", "batch": "b123", "expiry": "2028-01-31",
            "mrp": "25", "qty": "5", "discount_percent": "0", "pack_type": "Strip", "pack_qty": "10",
        })
        with self.app.app_context():
            self.assertEqual(self.app_module.Medicine.query.count(), 1)

    def test_merge_duplicate_rows(self):
        with self.app.app_context():
            m = self.app_module
            a = m.Medicine(name="PEN NEEDLE", batch="317B", expiry="2028-01-31", mrp=10, qty=4)
            b = m.Medicine(name="PEN NEEDLE", batch="317B", expiry="2028-01-31", mrp=10, qty=6)
            self.db.session.add_all([a, b])
            self.db.session.commit()
            a_id, b_id = a.id, b.id
        self.login()
        page = self.client.get("/data-health")
        self.assertIn("PEN NEEDLE", page.get_data(as_text=True))
        self.client.post("/data-health/merge-duplicate", data={"keep_id": a_id, "merge_id": b_id})
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(a_id).qty, 10)
            self.assertEqual(m.Medicine.query.get(b_id).qty, 0)
            self.assertFalse(m.Medicine.query.get(b_id).is_active)

    # ---------------- Security ----------------
    def test_csrf_blocks_post_without_token_and_allows_with_token(self):
        with self.app.app_context():
            self.seed_stock()
        self.login()
        self.app.config["CSRF_PROTECTION"] = True
        try:
            blocked = self.client.post("/billing/hold", data={"customer": "X"})
            self.assertEqual(blocked.status_code, 302)
            with self.client.session_transaction() as sess:
                token = sess.get("_csrf_token")
            if not token:
                self.client.get("/billing")
                with self.client.session_transaction() as sess:
                    token = sess.get("_csrf_token")
            self.assertTrue(token)
            with self.app.app_context():
                self.assertEqual(self.app_module.PendingBillStore.query.count(), 0)
            allowed = self.client.post("/billing/hold", data={"customer": "X", "_csrf_token": token})
            self.assertEqual(allowed.status_code, 302)
            self.assertIn("/pending-bills", allowed.headers.get("Location", ""))
            page = self.client.get("/billing").get_data(as_text=True)
            self.assertIn('name="csrf-token"', page)
        finally:
            self.app.config["CSRF_PROTECTION"] = False

    def test_delete_links_require_post(self):
        self.login()
        for url in ("/medicines/delete/1", "/delete-hold/1", "/vendor/delete/1", "/users/delete/2", "/vendor/purchase/delete/1"):
            self.assertEqual(self.client.get(url).status_code, 405, url)

    def test_production_refuses_weak_secret_key(self):
        module = self.app_module
        original_prod = module.IS_PROD
        original_key = os.environ.get("SECRET_KEY")
        try:
            module.IS_PROD = True
            os.environ["SECRET_KEY"] = "dev-secret"
            with self.assertRaises(RuntimeError):
                module.resolve_app_secret_key()
            os.environ["SECRET_KEY"] = "x" * 40
            self.assertEqual(module.resolve_app_secret_key(), "x" * 40)
        finally:
            module.IS_PROD = original_prod
            if original_key is None:
                os.environ.pop("SECRET_KEY", None)
            else:
                os.environ["SECRET_KEY"] = original_key

    # ---------------- Reports & pages ----------------
    def test_gst_report_and_new_pages_render(self):
        with self.app.app_context():
            self.seed_stock(mrp=105.0)
            self.seed_stock(name="OLD BATCH", batch="E1", expiry="2020-01-31")
        self.login()
        self.bill(qty=1)
        gst = self.client.get("/reports/gst")
        self.assertEqual(gst.status_code, 200)
        html = gst.get_data(as_text=True)
        self.assertIn("Rs 100.00", html)  # taxable value of a 105 sale at 5%
        self.assertEqual(self.client.get("/reports/gst?format=xlsx").status_code, 200)
        self.assertEqual(self.client.get("/reports/schedule-h1?format=xlsx").status_code, 200)
        expiry = self.client.get("/expiring-soon")
        self.assertIn("OLD BATCH", expiry.get_data(as_text=True))
        self.assertEqual(self.client.get("/quarantine-stock").status_code, 200)
        self.assertEqual(self.client.get("/data-health").status_code, 200)
        self.assertEqual(self.client.get("/medicines/add").status_code, 200)

    def test_expired_stock_write_off_keeps_lots_in_sync(self):
        with self.app.app_context():
            med_id, lot_id = self.seed_stock(name="OLD BATCH", batch="E1", expiry="2020-01-31", qty=4)
        self.login()
        self.client.post(f"/expired-stock/{med_id}/write-off")
        with self.app.app_context():
            m = self.app_module
            self.assertEqual(m.Medicine.query.get(med_id).qty, 0)
            self.assertEqual(m.VendorPurchaseItem.query.get(lot_id).remaining_qty, 0)


if __name__ == "__main__":
    unittest.main()
