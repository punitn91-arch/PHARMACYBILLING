import importlib
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timedelta


class EngineeringFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "engineering_test.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "engineering-test-secret"
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
        self.client = self.app.test_client()
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()
            self._seed_admin()

    def _seed_admin(self):
        admin = self.app_module.User(
            username="admin",
            role="admin",
            access_profile="admin",
            can_manage_users=True,
            can_view_reports=True,
            can_invoice_action=True,
            can_edit_invoice=True,
            can_delete_invoice=True,
            can_view_medicine=True,
            can_add_medicine=True,
            can_edit_medicine=True,
            can_delete_medicine=True,
            can_view_stock_history=True,
            can_manage_purchases=True,
            can_view_audit_logs=True,
            can_view_profit_dashboard=True,
        )
        admin.set_password("Admin@123")
        self.db.session.add(admin)
        self.db.session.commit()

    def login(self):
        return self.login_as("admin", "Admin@123")

    def login_as(self, username, password):
        response = self.client.post(
            "/login",
            data={"username": username, "password": password},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        return response

    def _seed_patient(self, name="Ravi Kumar", mobile="9876543210"):
        patient = self.app_module.Patient(name=name, mobile=mobile, gender="MALE", age=35)
        self.db.session.add(patient)
        self.db.session.flush()
        return patient

    def _seed_vendor_purchase_stack(self, *, medicine_name="PARACETAMOL 650", batch="B123", total_qty=10, purchase_chunks=None):
        if purchase_chunks is None:
            purchase_chunks = [(total_qty, 10.0, datetime.utcnow())]
        vendor = self.app_module.Vendor(name="Prime Distributor")
        self.db.session.add(vendor)
        self.db.session.flush()

        medicine = self.app_module.Medicine(
            name=medicine_name,
            batch=batch,
            expiry="2027-12-31",
            mrp=25.0,
            qty=total_qty,
            discount_percent=5,
            barcode="PARA650",
            reorder_level=10,
            is_active=True,
        )
        self.db.session.add(medicine)
        self.db.session.flush()

        purchase = self.app_module.VendorPurchase(
            vendor_id=vendor.id,
            purchase_no="PB-000001",
            invoice_no="SUP-1001",
            purchase_date=datetime.utcnow(),
            payment_mode="CASH",
            payment_status="Paid",
            total_amount=sum(qty * rate for qty, rate, _ in purchase_chunks),
            created_by="admin",
        )
        self.db.session.add(purchase)
        self.db.session.flush()

        for qty, rate, created_at in purchase_chunks:
            self.db.session.add(
                self.app_module.VendorPurchaseItem(
                    purchase_id=purchase.id,
                    vendor_id=vendor.id,
                    medicine_id=medicine.id,
                    medicine_name=medicine.name,
                    barcode=medicine.barcode,
                    batch=batch,
                    expiry=medicine.expiry,
                    qty=qty,
                    free_qty=0,
                    remaining_qty=qty,
                    purchase_rate=rate,
                    mrp=medicine.mrp,
                    total_value=qty * rate,
                    created_at=created_at,
                )
            )
        self.db.session.commit()
        return vendor, purchase, medicine

    def test_billing_flow_creates_invoice_and_updates_stock(self):
        with self.app.app_context():
            self._seed_patient()
            self._seed_vendor_purchase_stack(total_qty=10)

        self.login()
        response = self.client.post(
            "/billing",
            data={
                "customer": "Ravi Kumar",
                "mobile": "9876543210",
                "doctor": "Dr. Test",
                "gender": "MALE",
                "payment_mode": "CASH",
                "medicine_name": ["PARACETAMOL 650"],
                "qty": ["2"],
                "batch_override[]": ["B123"],
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"INV-", response.data)

        with self.app.app_context():
            invoice = self.app_module.Invoice.query.one()
            invoice_item = self.app_module.InvoiceItem.query.one()
            medicine = self.app_module.Medicine.query.filter_by(name="PARACETAMOL 650", batch="B123").one()
            purchase_item = self.app_module.VendorPurchaseItem.query.one()
            stock_history = self.app_module.StockHistory.query.filter_by(action="SALE").all()

            self.assertEqual(invoice.customer, "Ravi Kumar")
            self.assertEqual(invoice_item.qty, 2)
            self.assertEqual(medicine.qty, 8)
            self.assertEqual(purchase_item.remaining_qty, 8)
            self.assertEqual(len(stock_history), 1)

    def test_vendor_purchase_syncs_barcode_to_medicine_master(self):
        with self.app.app_context():
            vendor = self.app_module.Vendor(name="Sync Vendor", is_active=True)
            medicine = self.app_module.Medicine(
                name="CALCITAB",
                medicine_code="MED-CALCI",
                batch="C100",
                expiry="2027-12-31",
                mrp=120.0,
                qty=2,
                discount_percent=10,
                barcode="",
                reorder_level=5,
                is_active=True,
            )
            self.db.session.add(vendor)
            self.db.session.add(medicine)
            self.db.session.commit()
            vendor_id = vendor.id

        self.login()
        response = self.client.post(
            f"/vendor/{vendor_id}/purchase",
            data={
                "invoice_no": "SYNC-001",
                "purchase_date": date.today().isoformat(),
                "payment_mode": "CASH",
                "payment_status": "Paid",
                "paid_amount": "0",
                "medicine_name": ["CALCITAB"],
                "medicine_code": ["MED-CALCI"],
                "barcode": ["BARCODE-12345"],
                "composition": [""],
                "company": ["Test Pharma"],
                "distributor_name": ["Sync Vendor"],
                "pack_type": ["Box"],
                "pack_qty": ["1"],
                "batch": ["C100"],
                "expiry": ["12/2027"],
                "qty": ["5"],
                "free_qty": ["0"],
                "purchase_rate": ["80"],
                "mrp": ["120"],
                "gst_percent": ["0"],
                "discount_percent": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            medicine = self.app_module.Medicine.query.filter_by(name="CALCITAB", batch="C100").one()
            purchase_item = self.app_module.VendorPurchaseItem.query.one()
            self.assertEqual(medicine.medicine_code, "MED-CALCI")
            self.assertEqual(medicine.barcode, "BARCODE-12345")
            self.assertEqual(purchase_item.medicine_code, "MED-CALCI")
            self.assertEqual(purchase_item.barcode, "BARCODE-12345")

    def test_vendor_purchase_resolves_existing_medicine_name_from_code(self):
        with self.app.app_context():
            vendor = self.app_module.Vendor(name="Code Vendor", is_active=True)
            medicine = self.app_module.Medicine(
                name="CALCITAB D3 LIQUID 5 ML 1X1",
                medicine_code="MED-D3LQ",
                batch="OLD100",
                expiry="2027-12-31",
                mrp=140.0,
                qty=4,
                discount_percent=10,
                barcode="",
                reorder_level=5,
                is_active=True,
            )
            self.db.session.add(vendor)
            self.db.session.add(medicine)
            self.db.session.commit()
            vendor_id = vendor.id

        self.login()
        response = self.client.post(
            f"/vendor/{vendor_id}/purchase",
            data={
                "invoice_no": "SYNC-002",
                "purchase_date": date.today().isoformat(),
                "payment_mode": "CASH",
                "payment_status": "Paid",
                "paid_amount": "0",
                "medicine_name": [""],
                "medicine_code": ["MED-D3LQ"],
                "barcode": [""],
                "composition": [""],
                "company": ["Test Pharma"],
                "distributor_name": ["Code Vendor"],
                "pack_type": ["Bottle"],
                "pack_qty": ["1"],
                "batch": ["NEW200"],
                "expiry": ["12/2027"],
                "qty": ["5"],
                "free_qty": ["0"],
                "purchase_rate": ["80"],
                "mrp": ["140"],
                "gst_percent": ["0"],
                "discount_percent": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            new_batch = self.app_module.Medicine.query.filter_by(batch="NEW200").one()
            purchase_item = self.app_module.VendorPurchaseItem.query.order_by(
                self.app_module.VendorPurchaseItem.id.desc()
            ).first()
            self.assertEqual(new_batch.name, "CALCITAB D3 LIQUID 5 ML 1X1")
            self.assertEqual(new_batch.medicine_code, "MED-D3LQ")
            self.assertEqual(purchase_item.medicine_name, "CALCITAB D3 LIQUID 5 ML 1X1")
            self.assertEqual(purchase_item.medicine_code, "MED-D3LQ")

    def test_vendor_purchase_accepts_ajax_json_purchase_grid_payload(self):
        with self.app.app_context():
            vendor = self.app_module.Vendor(name="ERP Vendor", is_active=True)
            medicine = self.app_module.Medicine(
                name="DOLO 650",
                medicine_code="MED-DOLO650",
                batch="OLD-1",
                expiry="2027-12-31",
                mrp=35.0,
                qty=10,
                discount_percent=5,
                barcode="8900000000011",
                reorder_level=5,
                is_active=True,
                composition="Paracetamol 650",
                company="Micro Labs",
                pack_type="Strip",
                pack_qty=15,
            )
            self.db.session.add(vendor)
            self.db.session.add(medicine)
            self.db.session.commit()
            vendor_id = vendor.id

        self.login()
        payload = [
            {
                "medicine_code": "MED-DOLO650",
                "medicine_name": "DOLO 650",
                "barcode": "8900000000011",
                "batch": "NEW-1",
                "expiry": "12/2027",
                "pack_type": "Strip",
                "pack_qty": 15,
                "qty": 20,
                "free_qty": 2,
                "purchase_rate": 18,
                "mrp": 35,
                "gst_percent": 12,
                "discount_percent": 5,
                "composition": "Paracetamol 650",
                "company": "Micro Labs",
                "distributor_name": "ERP Vendor",
                "notes": "Fast lane item",
            }
        ]
        response = self.client.post(
            f"/vendor/{vendor_id}/purchase",
            data={
                "invoice_no": "ERP-001",
                "purchase_date": date.today().isoformat(),
                "payment_mode": "CASH",
                "payment_status": "Paid",
                "paid_amount": "0",
                "purchase_notes": "Main purchase note",
                "purchase_items_json": self.app_module.json.dumps(payload),
            },
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["purchase_no"][:3], "PB-")

        with self.app.app_context():
            purchase = self.app_module.VendorPurchase.query.one()
            purchase_item = self.app_module.VendorPurchaseItem.query.one()
            saved_medicine = self.app_module.Medicine.query.filter_by(batch="NEW-1").one()
            self.assertEqual(purchase.notes, "Main purchase note")
            self.assertEqual(purchase_item.notes, "Fast lane item")
            self.assertEqual(saved_medicine.medicine_code, "MED-DOLO650")
            self.assertEqual(saved_medicine.qty, 22)

    def test_vendor_form_renders_medicine_name_as_readonly_auto_fill(self):
        with self.app.app_context():
            vendor = self.app_module.Vendor(name="Readonly Vendor", is_active=True)
            self.db.session.add(vendor)
            self.db.session.commit()
            vendor_id = vendor.id

        self.login()
        response = self.client.get(f"/vendor/edit/{vendor_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'purchase_items_json', response.data)
        self.assertIn(b'Add Medicine', response.data)
        self.assertIn(b'Live Purchase Grid', response.data)
        self.assertIn(b'id="purchaseModal"', response.data)
        self.assertIn(b'Auto-filled from code', response.data)
        self.assertIn(b"purchase_items_json", response.data)

    def test_vendor_form_hides_address_bank_and_payment_sections_but_preserves_existing_values(self):
        with self.app.app_context():
            vendor = self.app_module.Vendor(
                name="Preserve Vendor",
                mobile="9999988888",
                email="vendor@example.com",
                gst_no="GST123",
                shop_name="Old Shop",
                area="Old Area",
                city="Old City",
                state="Old State",
                pincode="123456",
                address="Old Address",
                bank_name="Old Bank",
                account_holder_name="Old Holder",
                account_no="1234567890",
                ifsc="IFSC0001",
                upi="old@upi",
                vendor_type="Distributor",
                default_payment_mode="CASH",
                payment_status="Paid",
                attachment_ref="old-file.pdf",
                notes="Old vendor note",
                is_active=True,
            )
            self.db.session.add(vendor)
            self.db.session.commit()
            vendor_id = vendor.id

        self.login()
        get_response = self.client.get(f"/vendor/edit/{vendor_id}")
        self.assertEqual(get_response.status_code, 200)
        vendor_card_html = get_response.data.split(b'<div class="purchase-workspace">', 1)[0]
        self.assertNotIn(b"Address", vendor_card_html)
        self.assertNotIn(b"Bank Details", vendor_card_html)
        self.assertNotIn(b"Shop Name", vendor_card_html)
        self.assertNotIn(b"Account Holder Name", vendor_card_html)
        self.assertNotIn(b'name="vendor_type"', vendor_card_html)
        self.assertNotIn(b'name="default_payment_mode"', vendor_card_html)
        self.assertNotIn(b'name="payment_status"', vendor_card_html)
        self.assertNotIn(b'name="last_purchase_date"', vendor_card_html)
        self.assertNotIn(b'name="is_active"', vendor_card_html)
        self.assertNotIn(b"Attachments & Notes", vendor_card_html)
        self.assertNotIn(b'name="attachment_ref"', vendor_card_html)
        self.assertNotIn(b'name="attachment_file"', vendor_card_html)
        self.assertNotIn(b'name="notes"', vendor_card_html)

        post_response = self.client.post(
            f"/vendor/edit/{vendor_id}",
            data={
                "name": "Preserve Vendor Updated",
                "mobile": "9999988888",
                "email": "vendor@example.com",
                "gst_no": "GST123",
            },
            follow_redirects=False,
        )
        self.assertEqual(post_response.status_code, 302)

        with self.app.app_context():
            vendor = self.app_module.Vendor.query.get(vendor_id)
            self.assertEqual(vendor.shop_name, "Old Shop")
            self.assertEqual(vendor.area, "Old Area")
            self.assertEqual(vendor.city, "Old City")
            self.assertEqual(vendor.state, "Old State")
            self.assertEqual(vendor.pincode, "123456")
            self.assertEqual(vendor.address, "Old Address")
            self.assertEqual(vendor.bank_name, "Old Bank")
            self.assertEqual(vendor.account_holder_name, "Old Holder")
            self.assertEqual(vendor.account_no, "1234567890")
            self.assertEqual(vendor.ifsc, "IFSC0001")
            self.assertEqual(vendor.upi, "old@upi")
            self.assertEqual(vendor.vendor_type, "Distributor")
            self.assertEqual(vendor.default_payment_mode, "CASH")
            self.assertEqual(vendor.payment_status, "Paid")
            self.assertEqual(vendor.attachment_ref, "old-file.pdf")
            self.assertEqual(vendor.notes, "Old vendor note")
            self.assertTrue(vendor.is_active)

    def test_vendor_purchase_blocks_manual_name_when_code_missing_or_unknown(self):
        with self.app.app_context():
            vendor = self.app_module.Vendor(name="Strict Code Vendor", is_active=True)
            self.db.session.add(vendor)
            self.db.session.commit()
            vendor_id = vendor.id

        self.login()
        missing_code_response = self.client.post(
            f"/vendor/{vendor_id}/purchase",
            data={
                "invoice_no": "STRICT-001",
                "purchase_date": date.today().isoformat(),
                "payment_mode": "CASH",
                "payment_status": "Paid",
                "paid_amount": "0",
                "medicine_name": ["MANUAL MED"],
                "medicine_code": [""],
                "barcode": [""],
                "composition": [""],
                "company": ["Test Pharma"],
                "distributor_name": ["Strict Code Vendor"],
                "pack_type": ["Box"],
                "pack_qty": ["1"],
                "batch": ["MC100"],
                "expiry": ["12/2027"],
                "qty": ["2"],
                "free_qty": ["0"],
                "purchase_rate": ["50"],
                "mrp": ["70"],
                "gst_percent": ["0"],
                "discount_percent": ["0"],
            },
            follow_redirects=True,
        )
        self.assertEqual(missing_code_response.status_code, 200)
        self.assertIn(b"Medicine code is required for each purchase row", missing_code_response.data)

        unknown_code_response = self.client.post(
            f"/vendor/{vendor_id}/purchase",
            data={
                "invoice_no": "STRICT-002",
                "purchase_date": date.today().isoformat(),
                "payment_mode": "CASH",
                "payment_status": "Paid",
                "paid_amount": "0",
                "medicine_name": [""],
                "medicine_code": ["MED-UNKNOWN"],
                "barcode": [""],
                "composition": [""],
                "company": ["Test Pharma"],
                "distributor_name": ["Strict Code Vendor"],
                "pack_type": ["Box"],
                "pack_qty": ["1"],
                "batch": ["MC101"],
                "expiry": ["12/2027"],
                "qty": ["2"],
                "free_qty": ["0"],
                "purchase_rate": ["50"],
                "mrp": ["70"],
                "gst_percent": ["0"],
                "discount_percent": ["0"],
            },
            follow_redirects=True,
        )
        self.assertEqual(unknown_code_response.status_code, 200)
        self.assertIn(b"was not found in Medicine Master", unknown_code_response.data)

        with self.app.app_context():
            self.assertEqual(self.app_module.VendorPurchase.query.count(), 0)
            self.assertEqual(self.app_module.VendorPurchaseItem.query.count(), 0)
            self.assertEqual(self.app_module.Medicine.query.count(), 0)

    def test_return_flow_restores_stock_and_purchase_remaining(self):
        with self.app.app_context():
            patient = self._seed_patient()
            vendor, purchase, medicine = self._seed_vendor_purchase_stack(total_qty=3)
            purchase_item = self.app_module.VendorPurchaseItem.query.one()
            purchase_item.remaining_qty = 0
            medicine.qty = 0
            invoice = self.app_module.Invoice(
                invoice_no="INV-2001",
                patient_id=patient.id,
                customer=patient.name,
                mobile=patient.mobile,
                subtotal=75.0,
                total=75.0,
                payment_mode="CASH",
                created_by="admin",
                created_at=datetime.utcnow(),
            )
            self.db.session.add(invoice)
            self.db.session.flush()
            invoice_item = self.app_module.InvoiceItem(
                invoice_id=invoice.id,
                name=medicine.name,
                qty=3,
                price=25.0,
                amount=75.0,
                batch=medicine.batch,
                expiry=medicine.expiry,
                discount_percent=0,
                discount_amount=0,
                net_amount=75.0,
                cost_price=10.0,
                cost_amount=30.0,
            )
            self.db.session.add(invoice_item)
            self.db.session.flush()
            self.db.session.add(
                self.app_module.SalesAllocation(
                    invoice_item_id=invoice_item.id,
                    purchase_item_id=purchase_item.id,
                    qty=3,
                    cost_rate=10.0,
                    returned_qty=0,
                )
            )
            self.db.session.commit()
            invoice_item_id = invoice_item.id

        self.login()
        response = self.client.post(
            "/return-medicine",
            data={
                "invoice_no": "INV-2001",
                "payment_mode": "CASH",
                f"return_qty_{invoice_item_id}": "2",
                f"reason_{invoice_item_id}": "Damaged strip",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            ret = self.app_module.Return.query.one()
            return_item = self.app_module.ReturnItem.query.one()
            medicine = self.app_module.Medicine.query.filter_by(name="PARACETAMOL 650", batch="B123").one()
            purchase_item = self.app_module.VendorPurchaseItem.query.one()
            allocation = self.app_module.SalesAllocation.query.one()

            self.assertEqual(ret.invoice_no, "INV-2001")
            self.assertEqual(return_item.qty, 2)
            self.assertEqual(medicine.qty, 2)
            self.assertEqual(purchase_item.remaining_qty, 2)
            self.assertEqual(allocation.returned_qty, 2)

    def test_appointment_payment_flow_marks_paid_and_blocks_duplicate(self):
        with self.app.app_context():
            patient = self._seed_patient()
            appointment = self.app_module.Appointment(
                appointment_no="APT-1001",
                token_no=1,
                patient_id=patient.id,
                patient_name=patient.name,
                mobile=patient.mobile,
                gender="MALE",
                age=35,
                doctor_name="Dr. Test",
                appointment_date=date.today(),
                appointment_time=time(10, 0),
                payment_mode="ONLINE",
                payment_status="UNPAID",
                consultation_fee=500.0,
                status="BOOKED",
                created_by="admin",
            )
            self.db.session.add(appointment)
            self.db.session.commit()
            appointment_id = appointment.id

        self.login()
        first_response = self.client.post(f"/appointments/{appointment_id}/payment/paid", follow_redirects=False)
        self.assertEqual(first_response.status_code, 302)

        with self.app.app_context():
            appointment = self.app_module.Appointment.query.get(appointment_id)
            self.assertEqual(appointment.payment_status, "PAID")

        second_response = self.client.post(f"/appointments/{appointment_id}/payment/paid", follow_redirects=True)
        self.assertEqual(second_response.status_code, 200)
        self.assertIn(b"already marked as paid", second_response.data)

    def test_appointment_create_handles_legacy_integer_soft_delete_column(self):
        self.login()
        original_helper = self.app_module.appointment_soft_delete_uses_legacy_integer
        self.app_module.appointment_soft_delete_uses_legacy_integer = lambda: True
        try:
            response = self.client.post(
                "/appointments/add",
                data={
                    "patient_name": "Legacy Create",
                    "mobile": "9766655544",
                    "gender": "MALE",
                    "appointment_date": date.today().isoformat(),
                    "appointment_time": "09:45",
                    "payment_mode": "ONLINE",
                    "doctor_discount": "0",
                    "consultation_fee": "600",
                    "symptoms": "",
                    "previous_visit_notes": "",
                    "notes": "",
                },
                follow_redirects=False,
            )
        finally:
            self.app_module.appointment_soft_delete_uses_legacy_integer = original_helper

        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            appointment = self.app_module.Appointment.query.filter_by(patient_name="Legacy Create").first()
            self.assertIsNotNone(appointment)
            self.assertFalse(bool(appointment.is_deleted))

    def test_profit_report_accuracy(self):
        with self.app.app_context():
            patient = self._seed_patient()
            invoice = self.app_module.Invoice(
                invoice_no="INV-3001",
                patient_id=patient.id,
                customer=patient.name,
                mobile=patient.mobile,
                subtotal=300.0,
                total=300.0,
                payment_mode="CASH",
                created_by="admin",
                created_at=datetime.utcnow(),
            )
            self.db.session.add(invoice)
            self.db.session.flush()
            self.db.session.add(
                self.app_module.InvoiceItem(
                    invoice_id=invoice.id,
                    name="OMEPRAZOLE",
                    qty=3,
                    price=100.0,
                    amount=300.0,
                    batch="OM1",
                    expiry="2027-12-31",
                    net_amount=300.0,
                    cost_price=60.0,
                    cost_amount=180.0,
                )
            )
            ret = self.app_module.Return(
                invoice_id=invoice.id,
                invoice_no=invoice.invoice_no,
                customer=invoice.customer,
                mobile=invoice.mobile,
                total_refund=50.0,
                payment_mode="CASH",
                created_by="admin",
                created_at=datetime.utcnow(),
            )
            self.db.session.add(ret)
            self.db.session.flush()
            self.db.session.add(
                self.app_module.ReturnItem(
                    return_id=ret.id,
                    invoice_item_id=1,
                    medicine_name="OMEPRAZOLE",
                    qty=1,
                    price=50.0,
                    amount=50.0,
                    net_amount=50.0,
                    cost_price=30.0,
                    cost_amount=30.0,
                )
            )
            self.db.session.commit()

            today_iso = date.today().isoformat()
            summary, error = self.app_module.build_profit_report_summary(today_iso, today_iso)
            self.assertIsNone(error)
            self.assertEqual(summary["net_sales"], 250.0)
            self.assertEqual(summary["net_cogs"], 150.0)
            self.assertEqual(summary["gross_profit"], 100.0)

    def test_fifo_stock_deduction_uses_oldest_purchase_cost(self):
        with self.app.app_context():
            self._seed_patient()
            _, _, medicine = self._seed_vendor_purchase_stack(
                total_qty=10,
                purchase_chunks=[
                    (5, 10.0, datetime.utcnow() - timedelta(days=2)),
                    (5, 12.0, datetime.utcnow() - timedelta(days=1)),
                ],
            )

        self.login()
        response = self.client.post(
            "/billing",
            data={
                "customer": "Ravi Kumar",
                "mobile": "9876543210",
                "doctor": "Dr. Test",
                "gender": "MALE",
                "payment_mode": "CASH",
                "medicine_name": ["PARACETAMOL 650"],
                "qty": ["6"],
                "batch_override[]": ["B123"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            invoice_item = self.app_module.InvoiceItem.query.one()
            purchase_items = self.app_module.VendorPurchaseItem.query.order_by(self.app_module.VendorPurchaseItem.created_at.asc()).all()
            medicine = self.app_module.Medicine.query.filter_by(name="PARACETAMOL 650", batch="B123").one()

            self.assertAlmostEqual(invoice_item.cost_amount, 62.0, places=2)
            self.assertEqual(purchase_items[0].remaining_qty, 0)
            self.assertEqual(purchase_items[1].remaining_qty, 4)
            self.assertEqual(medicine.qty, 4)

    def test_dashboard_profit_cards_hidden_without_permission_and_visible_with_permission(self):
        with self.app.app_context():
            hidden_staff = self.app_module.User(
                username="staff_hidden",
                role="staff",
                access_profile="custom",
                can_invoice_action=True,
            )
            hidden_staff.set_password("Staff@123")
            visible_staff = self.app_module.User(
                username="staff_visible",
                role="staff",
                access_profile="custom",
                can_invoice_action=True,
                can_view_profit_dashboard=True,
            )
            visible_staff.set_password("Staff@123")
            self.db.session.add(hidden_staff)
            self.db.session.add(visible_staff)
            self.db.session.commit()

        self.login_as("staff_hidden", "Staff@123")
        hidden_response = self.client.get("/")
        self.assertEqual(hidden_response.status_code, 200)
        self.assertNotIn(b"Gross Profit Today", hidden_response.data)
        self.assertNotIn(b"Gross Profit This Month", hidden_response.data)

        self.client.get("/logout")
        self.login_as("staff_visible", "Staff@123")
        visible_response = self.client.get("/")
        self.assertEqual(visible_response.status_code, 200)
        self.assertIn(b"Gross Profit Today", visible_response.data)
        self.assertIn(b"Gross Profit This Month", visible_response.data)

    def test_invalid_login_creates_security_event(self):
        response = self.client.post(
            "/login",
            data={"username": "admin", "password": "WrongPass!"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Invalid credentials", response.data)

        with self.app.app_context():
            event = self.app_module.LoginSecurityEvent.query.order_by(
                self.app_module.LoginSecurityEvent.id.desc()
            ).first()
            self.assertIsNotNone(event)
            self.assertEqual(event.outcome, "FAILED")
            self.assertEqual(event.reason, "Invalid credentials")
            self.assertFalse(event.is_suspicious)

    def test_archived_user_is_soft_deleted_and_login_is_blocked(self):
        with self.app.app_context():
            staff = self.app_module.User(
                username="archivable_staff",
                role="staff",
                access_profile="billing_only",
                can_invoice_action=True,
            )
            staff.set_password("Staff@123")
            self.db.session.add(staff)
            self.db.session.commit()
            staff_id = staff.id

        self.login()
        archive_response = self.client.get(f"/users/delete/{staff_id}", follow_redirects=False)
        self.assertEqual(archive_response.status_code, 302)

        with self.app.app_context():
            archived_user = self.app_module.User.query.get(staff_id)
            self.assertFalse(archived_user.is_active)
            self.assertIsNotNone(archived_user.deleted_at)
            self.assertEqual(self.app_module.active_user_query().filter_by(username="archivable_staff").count(), 0)

        login_response = self.client.post(
            "/login",
            data={"username": "archivable_staff", "password": "Staff@123"},
            follow_redirects=True,
        )
        self.assertEqual(login_response.status_code, 200)
        self.assertIn(b"Invalid credentials", login_response.data)

        with self.app.app_context():
            event = self.app_module.LoginSecurityEvent.query.filter_by(
                username="archivable_staff",
                outcome="FAILED",
            ).order_by(self.app_module.LoginSecurityEvent.id.desc()).first()
            self.assertIsNotNone(event)
            self.assertTrue(event.is_suspicious)
            self.assertEqual(event.reason, "Attempted login to disabled user")

    def test_session_idle_timeout_logs_security_event(self):
        self.login()
        with self.client.session_transaction() as session_data:
            session_data["last_seen_at"] = (datetime.utcnow() - timedelta(hours=3)).isoformat()

        response = self.client.get("/medicines", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers.get("Location", ""))

        with self.app.app_context():
            event = self.app_module.LoginSecurityEvent.query.filter_by(
                outcome="SESSION_EXPIRED"
            ).order_by(self.app_module.LoginSecurityEvent.id.desc()).first()
            self.assertIsNotNone(event)
            self.assertEqual(event.reason, "Idle session timeout")

    def test_hold_bill_delete_soft_archives_record(self):
        with self.app.app_context():
            hold_bill = self.app_module.HoldBill(
                customer="Saved Patient",
                mobile="9000000000",
                doctor="Dr. Save",
                gender="MALE",
                data={"items": []},
            )
            self.db.session.add(hold_bill)
            self.db.session.commit()
            hold_bill_id = hold_bill.id

        self.login()
        response = self.client.get(f"/delete-hold/{hold_bill_id}", follow_redirects=False)
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            hold_bill = self.app_module.HoldBill.query.get(hold_bill_id)
            self.assertTrue(hold_bill.is_deleted)
            self.assertIsNotNone(hold_bill.deleted_at)
            self.assertEqual(self.app_module.active_hold_bill_query().filter_by(id=hold_bill_id).count(), 0)

    def test_appointment_delete_soft_archives_record(self):
        with self.app.app_context():
            patient = self._seed_patient(name="Delete Appt", mobile="9888877777")
            appointment = self.app_module.Appointment(
                appointment_no="APT-DEL-1",
                token_no=3,
                patient_id=patient.id,
                patient_name=patient.name,
                mobile=patient.mobile,
                gender="MALE",
                age=34,
                doctor_name="Dr. Delete",
                appointment_date=date.today(),
                appointment_time=time(12, 0),
                consultation_fee=400.0,
                payment_mode="CASH",
                payment_status="UNPAID",
                status="BOOKED",
                created_by="admin",
            )
            self.db.session.add(appointment)
            self.db.session.commit()
            appointment_id = appointment.id

        self.login()
        response = self.client.post(f"/appointments/delete/{appointment_id}", follow_redirects=False)
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            appointment = self.app_module.Appointment.query.get(appointment_id)
            self.assertTrue(appointment.is_deleted)
            self.assertIsNotNone(appointment.deleted_at)
            self.assertEqual(self.app_module.active_appointment_query().filter_by(id=appointment_id).count(), 0)

    def test_appointment_delete_async_returns_success_payload(self):
        with self.app.app_context():
            patient = self._seed_patient(name="Delete Async", mobile="9777766666")
            appointment = self.app_module.Appointment(
                appointment_no="APT-DEL-ASYNC",
                token_no=4,
                patient_id=patient.id,
                patient_name=patient.name,
                mobile=patient.mobile,
                gender="MALE",
                age=31,
                doctor_name="Dr. Delete",
                appointment_date=date.today(),
                appointment_time=time(1, 0),
                consultation_fee=500.0,
                payment_mode="ONLINE",
                payment_status="UNPAID",
                status="BOOKED",
                created_by="admin",
            )
            self.db.session.add(appointment)
            self.db.session.commit()
            appointment_id = appointment.id

        self.login()
        response = self.client.post(
            f"/appointments/delete/{appointment_id}",
            headers={"X-Requested-With": "XMLHttpRequest"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])

        with self.app.app_context():
            appointment = self.app_module.Appointment.query.get(appointment_id)
            self.assertTrue(appointment.is_deleted)
            self.assertEqual(self.app_module.active_appointment_query().filter_by(id=appointment_id).count(), 0)

    def test_user_edit_page_renders_and_updates_permissions(self):
        with self.app.app_context():
            staff_user = self.app_module.User(
                username="counter1",
                role="staff",
                access_profile="custom",
                can_view_reports=False,
                can_manage_purchases=False,
                can_view_profit_dashboard=False,
                is_active=True,
            )
            staff_user.set_password("Password@123")
            self.db.session.add(staff_user)
            self.db.session.commit()
            staff_user_id = staff_user.id

        self.login()
        get_response = self.client.get(f"/users/edit/{staff_user_id}")
        self.assertEqual(get_response.status_code, 200)
        self.assertIn(b"Dashboard Profit Cards", get_response.data)
        self.assertIn(b"Access Profile", get_response.data)

        response = self.client.post(
            f"/users/edit/{staff_user_id}",
            data={
                "username": "counter1",
                "role": "staff",
                "access_profile": "custom",
                "can_view_reports": "on",
                "can_manage_purchases": "on",
                "can_view_profit_dashboard": "on",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            updated_user = self.db.session.get(self.app_module.User, staff_user_id)
            self.assertTrue(updated_user.can_view_reports)
            self.assertTrue(updated_user.can_manage_purchases)
            self.assertTrue(updated_user.can_view_profit_dashboard)
            self.assertEqual(updated_user.access_profile, "custom")

    def test_user_edit_and_create_survive_legacy_integer_permission_storage(self):
        with self.app.app_context():
            staff_user = self.app_module.User(
                username="legacy_staff",
                role="staff",
                access_profile="custom",
                can_manage_purchases=False,
                can_view_reports=False,
                is_active=True,
            )
            staff_user.set_password("Password@123")
            self.db.session.add(staff_user)
            self.db.session.commit()
            staff_user_id = staff_user.id

        self.login()
        original_helper = self.app_module.user_boolean_storage_mode_map
        legacy_map = {"is_active": "integer"}
        legacy_map.update({field_name: "integer" for field_name in self.app_module.USER_PERMISSION_FIELDS})
        self.app_module.user_boolean_storage_mode_map = lambda: dict(legacy_map)
        try:
            edit_response = self.client.post(
                f"/users/edit/{staff_user_id}",
                data={
                    "username": "legacy_staff",
                    "role": "staff",
                    "access_profile": "custom",
                    "can_manage_purchases": "on",
                    "can_view_reports": "on",
                },
                follow_redirects=False,
            )
            self.assertEqual(edit_response.status_code, 302)

            add_response = self.client.post(
                "/users/add",
                data={
                    "username": "legacy_new",
                    "password": "Password@123",
                    "role": "staff",
                    "access_profile": "custom",
                    "can_view_reports": "on",
                    "can_manage_purchases": "on",
                },
                follow_redirects=False,
            )
            self.assertEqual(add_response.status_code, 302)
        finally:
            self.app_module.user_boolean_storage_mode_map = original_helper

        with self.app.app_context():
            updated_user = self.db.session.get(self.app_module.User, staff_user_id)
            created_user = self.app_module.active_user_query().filter_by(username="legacy_new").first()
            self.assertTrue(updated_user.can_manage_purchases)
            self.assertTrue(updated_user.can_view_reports)
            self.assertIsNotNone(created_user)
            self.assertTrue(created_user.can_manage_purchases)
            self.assertTrue(created_user.can_view_reports)

    def test_user_delete_survives_legacy_integer_is_active_storage(self):
        with self.app.app_context():
            staff_user = self.app_module.User(
                username="archivable_staff",
                role="staff",
                access_profile="custom",
                is_active=True,
            )
            staff_user.set_password("Password@123")
            self.db.session.add(staff_user)
            self.db.session.commit()
            staff_user_id = staff_user.id

        self.login()
        original_helper = self.app_module.user_boolean_storage_mode_map
        self.app_module.user_boolean_storage_mode_map = lambda: {"is_active": "integer"}
        try:
            response = self.client.get(f"/users/delete/{staff_user_id}", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
        finally:
            self.app_module.user_boolean_storage_mode_map = original_helper

        with self.app.app_context():
            archived_user = self.db.session.get(self.app_module.User, staff_user_id)
            self.assertIsNotNone(archived_user.deleted_at)
            self.assertEqual(self.app_module.active_user_query().filter_by(username="archivable_staff").count(), 0)

    def test_backup_snapshot_and_restore_drill(self):
        with self.app.app_context():
            upload_dirs = self.app.config["INFRA_UPLOAD_DIRECTORIES"]
            os.makedirs(upload_dirs["vendor_uploads"], exist_ok=True)
            with open(os.path.join(upload_dirs["vendor_uploads"], "sample.txt"), "w", encoding="utf-8") as handle:
                handle.write("backup-check")

            snapshot = self.app_module.build_backup_snapshot(
                self.app,
                upload_dirs=upload_dirs,
                keep_count=5,
                include_uploads=True,
            )
            self.assertTrue(os.path.exists(snapshot["manifest_path"]))
            self.assertTrue(os.path.exists(snapshot["restore_plan_path"]))

            drill = self.app_module.restore_backup_snapshot(
                self.app,
                snapshot_name=snapshot["snapshot_name"],
                include_uploads=True,
                dry_run=True,
            )
            self.assertTrue(drill["dry_run"])
            self.assertEqual(drill["snapshot_name"], snapshot["snapshot_name"])


if __name__ == "__main__":
    unittest.main()
