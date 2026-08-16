import importlib
import os
import sys
import tempfile
import unittest


class LabModuleTests(unittest.TestCase):
    """Regression coverage for the standalone lab-test catalog and billing flow."""

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "lab_module_test.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "lab-module-test-secret"
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
            self.db.session.remove()
            self.db.drop_all()
            self.db.create_all()
            self._seed_users()

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()

    def _seed_users(self):
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

        billing_staff = self.app_module.User(
            username="billingstaff",
            role="staff",
            access_profile="custom",
            can_invoice_action=True,
        )
        billing_staff.set_password("Staff@123")

        self.db.session.add_all([admin, billing_staff])
        self.db.session.commit()

    def _login_as(self, username, password):
        response = self.client.post(
            "/login",
            data={"username": username, "password": password},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        return response

    def _logout(self):
        self.client.get("/logout", follow_redirects=False)

    def _lab_test_payload(self, *, code="CBC", name="Complete Blood Count", price="450.00"):
        return {
            "test_code": code,
            "name": name,
            "category": "Hematology",
            "specimen_type": "Blood",
            "preparation": "No fasting required",
            "default_price": price,
            "is_active": "on",
        }

    def _create_lab_test(self, *, code="CBC", name="Complete Blood Count", price="450.00"):
        self._login_as("admin", "Admin@123")
        response = self.client.post(
            "/lab-tests/add",
            data=self._lab_test_payload(code=code, name=name, price=price),
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            lab_test = self.app_module.LabTest.query.filter_by(test_code=code).one()
            return lab_test.id

    def _bill_lab_test(self, lab_test_id, *, qty="1"):
        # Deliberately do not submit any rate/price field. The server must load
        # the active catalog rate using the selected test id, never trust UI data.
        return self.client.post(
            "/lab-billing",
            data={
                "customer": "Ravi Kumar",
                "patient_name": "Ravi Kumar",
                "mobile": "9876543210",
                "gender": "MALE",
                "doctor": "Dr. Test",
                "payment_mode": "CASH",
                "lab_test_id[]": [str(lab_test_id)],
                "qty[]": [qty],
            },
            follow_redirects=True,
        )

    def test_admin_can_add_lab_test_to_catalog(self):
        lab_test_id = self._create_lab_test()

        with self.app.app_context():
            lab_test = self.db.session.get(self.app_module.LabTest, lab_test_id)
            self.assertIsNotNone(lab_test)
            self.assertEqual(lab_test.test_code, "CBC")
            self.assertEqual(lab_test.name, "Complete Blood Count")
            self.assertEqual(float(lab_test.default_price), 450.0)
            self.assertTrue(lab_test.is_active)

    def test_billing_staff_uses_catalog_price_and_creates_no_pharmacy_rows(self):
        lab_test_id = self._create_lab_test(price="450.00")
        self._logout()
        self._login_as("billingstaff", "Staff@123")

        response = self._bill_lab_test(lab_test_id, qty="2")
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            lab_order = self.app_module.LabOrder.query.one()
            line = self.app_module.LabOrderItem.query.one()

            self.assertTrue(lab_order.order_no)
            self.assertEqual(line.lab_test_id, lab_test_id)
            self.assertEqual(line.test_name, "Complete Blood Count")
            self.assertEqual(float(line.unit_price), 450.0)
            self.assertEqual(int(line.qty), 2)
            self.assertEqual(self.app_module.Medicine.query.count(), 0)
            self.assertEqual(self.app_module.InvoiceItem.query.count(), 0)
            self.assertEqual(self.app_module.StockHistory.query.count(), 0)
            self.assertEqual(self.app_module.SalesAllocation.query.count(), 0)

    def test_lab_billing_and_order_history_render_for_billing_staff(self):
        lab_test_id = self._create_lab_test(price="350.00")
        billing_page = self.client.get("/lab-billing")
        self.assertEqual(billing_page.status_code, 200)
        self.assertIn(b"Create Lab Order", billing_page.data)
        self.assertIn(b"Complete Blood Count", billing_page.data)

        self._logout()
        self._login_as("billingstaff", "Staff@123")
        bill_response = self._bill_lab_test(lab_test_id)
        self.assertEqual(bill_response.status_code, 200)
        self.assertIn(b"LAB ORDER", bill_response.data)
        self.assertIn(b"Complete Blood Count", bill_response.data)
        self.assertIn(b"350.00", bill_response.data)

        orders_response = self.client.get("/lab-orders")
        self.assertEqual(orders_response.status_code, 200)
        self.assertIn(b"Lab Orders", orders_response.data)
        self.assertIn(b"LAB-", orders_response.data)

    def test_lab_catalog_and_orders_are_included_in_backup_snapshot(self):
        lab_test_id = self._create_lab_test(price="275.00")
        bill_response = self._bill_lab_test(lab_test_id)
        self.assertEqual(bill_response.status_code, 200)

        with self.app.app_context():
            snapshot = self.app_module.build_backup_snapshot(
                self.app,
                upload_dirs=self.app.config.get("INFRA_UPLOAD_DIRECTORIES", {}),
                keep_count=2,
                include_uploads=False,
            )
            counts = snapshot["manifest"]["table_counts"]
            self.assertEqual(counts.get("lab_test"), 1)
            self.assertEqual(counts.get("lab_order"), 1)
            self.assertEqual(counts.get("lab_order_item"), 1)

    def test_old_order_item_price_snapshot_survives_catalog_price_edit(self):
        lab_test_id = self._create_lab_test(price="450.00")
        self._logout()
        self._login_as("billingstaff", "Staff@123")
        bill_response = self._bill_lab_test(lab_test_id)
        self.assertEqual(bill_response.status_code, 200)

        self._logout()
        self._login_as("admin", "Admin@123")
        edit_response = self.client.post(
            f"/lab-tests/{lab_test_id}/edit",
            data=self._lab_test_payload(price="625.00"),
            follow_redirects=True,
        )
        self.assertEqual(edit_response.status_code, 200)

        with self.app.app_context():
            lab_test = self.db.session.get(self.app_module.LabTest, lab_test_id)
            line = self.app_module.LabOrderItem.query.one()
            self.assertEqual(float(lab_test.default_price), 625.0)
            self.assertEqual(float(line.unit_price), 450.0)

    def test_inactive_lab_test_cannot_be_billed(self):
        lab_test_id = self._create_lab_test()
        deactivate_response = self.client.post(
            f"/lab-tests/{lab_test_id}/deactivate",
            follow_redirects=True,
        )
        self.assertEqual(deactivate_response.status_code, 200)

        with self.app.app_context():
            lab_test = self.db.session.get(self.app_module.LabTest, lab_test_id)
            self.assertFalse(lab_test.is_active)

        self._logout()
        self._login_as("billingstaff", "Staff@123")
        billing_response = self._bill_lab_test(lab_test_id)
        self.assertEqual(billing_response.status_code, 200)

        with self.app.app_context():
            self.assertEqual(self.app_module.LabOrder.query.count(), 0)
            self.assertEqual(self.app_module.LabOrderItem.query.count(), 0)

    def test_billing_staff_cannot_access_or_change_lab_catalog(self):
        self._login_as("billingstaff", "Staff@123")

        list_response = self.client.get("/lab-tests", follow_redirects=False)
        add_response = self.client.post(
            "/lab-tests/add",
            data=self._lab_test_payload(),
            follow_redirects=False,
        )

        self.assertIn(list_response.status_code, (302, 403))
        self.assertIn(add_response.status_code, (302, 403))
        with self.app.app_context():
            self.assertEqual(self.app_module.LabTest.query.count(), 0)


if __name__ == "__main__":
    unittest.main()
