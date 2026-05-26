import importlib
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, time


class SystemSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "test_app.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_TIMEZONE"] = "Asia/Kolkata"
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

        with cls.app.app_context():
            cls.db.drop_all()
            cls.db.create_all()

            admin = cls.app_module.User(
                username="admin",
                role="admin",
                access_profile="admin",
                can_manage_users=True,
                can_view_reports=True,
                can_invoice_action=True,
                can_view_medicine=True,
                can_add_medicine=True,
                can_edit_medicine=True,
                can_delete_medicine=True,
                can_view_profit_dashboard=True,
                can_manage_purchases=True,
                can_view_audit_logs=True,
            )
            admin.set_password("Admin@123")
            cls.db.session.add(admin)

            patient = cls.app_module.Patient(
                name="Ravi Kumar",
                mobile="9876543210",
                gender="MALE",
                age=34,
            )
            cls.db.session.add(patient)
            cls.db.session.flush()

            medicine = cls.app_module.Medicine(
                name="PARACETAMOL 650",
                batch="B123",
                expiry="2027-12-31",
                mrp=25.0,
                qty=120,
                discount_percent=5,
                barcode="PARA650",
                reorder_level=10,
                is_active=True,
            )
            cls.db.session.add(medicine)

            invoice = cls.app_module.Invoice(
                invoice_no="INV-1001",
                patient_id=patient.id,
                customer=patient.name,
                mobile=patient.mobile,
                total=250.0,
                payment_mode="CASH",
                created_by="admin",
                created_at=datetime.utcnow(),
            )
            cls.db.session.add(invoice)

            appointment = cls.app_module.Appointment(
                appointment_no="APT-1001",
                token_no=1,
                patient_id=patient.id,
                patient_name=patient.name,
                mobile=patient.mobile,
                age=34,
                gender="MALE",
                doctor_name="Dr. Test",
                appointment_date=date.today(),
                appointment_time=time(10, 0),
                payment_mode="ONLINE",
                payment_status="PAID",
                consultation_fee=500.0,
                status="COMPLETED",
                created_by="admin",
            )
            cls.db.session.add(appointment)
            cls.db.session.commit()

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def setUp(self):
        self.client = self.app.test_client()

    def login(self):
        response = self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

    def test_health_and_readiness_routes(self):
        health_response = self.client.get("/healthz")
        self.assertEqual(health_response.status_code, 200)
        self.assertEqual(health_response.json["status"], "ok")
        self.assertIn("database", health_response.json)

        ready_response = self.client.get("/readyz")
        self.assertEqual(ready_response.status_code, 200)
        self.assertTrue(ready_response.json["ready"])

    def test_system_center_renders_for_admin(self):
        self.login()
        response = self.client.get("/system-center")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"System Center", response.data)
        self.assertIn(b"Database & Runtime", response.data)
        self.assertIn(b"Backup & Restore", response.data)
        self.assertIn(b"Security Watch", response.data)

    def test_global_search_returns_seeded_records(self):
        self.login()
        response = self.client.get("/api/global-search?q=para")
        self.assertEqual(response.status_code, 200)
        groups = response.json["groups"]
        keys = {group["key"] for group in groups}
        self.assertIn("medicines", keys)

        response = self.client.get("/api/global-search?q=Ravi")
        self.assertEqual(response.status_code, 200)
        groups = response.json["groups"]
        keys = {group["key"] for group in groups}
        self.assertIn("patients", keys)
        self.assertIn("appointments", keys)

    def test_billing_page_exposes_barcode_billing_ui(self):
        self.login()
        response = self.client.get("/billing")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Scan Barcode", response.data)
        self.assertIn(b"barcodeBillingInput", response.data)
        self.assertIn(b"PARA650", response.data)

    def test_dashboard_renders_premium_analytics_blocks(self):
        self.login()
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Sales Trend", response.data)
        self.assertIn(b"Dead Stock Watch", response.data)

    def test_invoices_page_renders_premium_controls(self):
        self.login()
        response = self.client.get("/invoices")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Invoice / Patient / Mobile", response.data)
        self.assertIn(b"High Value", response.data)

    def test_appointments_page_renders_shortcut_filters(self):
        self.login()
        response = self.client.get("/appointments")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"This Week", response.data)
        self.assertIn(b"Alt + P", response.data)

    def test_medicines_page_renders_barcode_filter(self):
        self.login()
        response = self.client.get("/medicines")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Barcode Missing", response.data)


if __name__ == "__main__":
    unittest.main()
