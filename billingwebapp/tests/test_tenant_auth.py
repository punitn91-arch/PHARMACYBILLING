import importlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta

from PIL import Image
from sqlalchemy.exc import IntegrityError
from werkzeug.datastructures import FileStorage


class TenantAuthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "tenant_auth.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "tenant-auth-test-secret"
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
        self.client = self.app.test_client()
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()

    def _seed_user(self, username="admin", password="Admin@123", tenant=None):
        with self.app_module.tenant_scope_disabled():
            user = self.app_module.User(
                tenant_id=(tenant.id if tenant else None),
                username=username,
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
            user.set_password(password)
            self.db.session.add(user)
            self.db.session.commit()
        return user

    def _seed_tenant(self, code="alpha", status="Active"):
        settings = self.app_module.TenantSettings(settings_json="{}")
        tenant = self.app_module.Tenant(
            tenant_uid=self.app_module.generate_tenant_uid(),
            tenant_code=code,
            pharmacy_name=f"{code.title()} Pharmacy",
            subscription_plan_ref="test",
            status=status,
            settings=settings,
        )
        self.db.session.add(settings)
        self.db.session.add(tenant)
        self.db.session.commit()
        return tenant

    def _seed_admin_for_tenant(self, tenant, username, password="Admin@123"):
        return self._seed_user(username=username, password=password, tenant=tenant)

    def _seed_medicine(self, tenant, name, batch):
        with self.app_module.tenant_scope_disabled():
            medicine = self.app_module.Medicine(
                tenant_id=tenant.id,
                name=name,
                batch=batch,
                expiry="2027-12-31",
                mrp=25.0,
                qty=10,
                is_active=True,
            )
            self.db.session.add(medicine)
            self.db.session.commit()
        return medicine

    def _login(self, tenant_code, username, password="Admin@123"):
        response = self.client.post(
            "/login",
            data={"tenant": tenant_code, "username": username, "password": password},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        return response

    def _seed_super_admin(self, username="owner", password="Owner@123"):
        with self.app_module.tenant_scope_disabled():
            user = self.app_module.User(
                tenant_id=None,
                username=username,
                role=self.app_module.PLATFORM_SUPER_ADMIN_ROLE,
                access_profile="platform_owner",
                can_manage_users=True,
                can_view_reports=True,
                can_view_audit_logs=True,
            )
            user.set_password(password)
            self.db.session.add(user)
            self.db.session.commit()
        return user

    def _login_super_admin(self, username="owner", password="Owner@123"):
        response = self.client.post(
            "/login",
            data={"username": username, "password": password},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/super-admin", response.headers.get("Location", ""))
        return response

    def test_legacy_login_creates_default_tenant_and_scopes_session(self):
        with self.app.app_context():
            user = self._seed_user()
            self.assertIsNone(user.tenant_id)

        response = self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as session:
            tenant_id = session.get("tenant_id")
            self.assertIsNotNone(tenant_id)
            self.assertEqual(session.get("tenant_code"), "default")

        with self.app.app_context():
            tenant = self.app_module.Tenant.query.filter_by(tenant_code="default").one()
            user = self.app_module.User.query.filter_by(username="admin").one()
            self.assertEqual(user.tenant_id, tenant.id)

    def test_suspended_tenant_cannot_login(self):
        with self.app.app_context():
            tenant = self._seed_tenant(status="Suspended")
            self._seed_user(tenant=tenant)

        response = self.client.post(
            "/login",
            data={"tenant": "alpha", "username": "admin", "password": "Admin@123"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"pharmacy account is not active", response.data)
        with self.client.session_transaction() as session:
            self.assertIsNone(session.get("user_id"))

    def test_multiple_tenants_require_tenant_code(self):
        with self.app.app_context():
            tenant = self._seed_tenant(code="alpha")
            self._seed_user(tenant=tenant)
            self._seed_tenant(code="beta")

        response = self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"pharmacy tenant code", response.data)

        response = self.client.post(
            "/login",
            data={"tenant": "alpha", "username": "admin", "password": "Admin@123"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

    def test_super_admin_dashboard_filters_metrics_and_access_toggle(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha", status="Active")
            beta = self._seed_tenant(code="beta", status="Suspended")
            alpha.subscription_plan_ref = "monthly"
            alpha.subscription_expiry = datetime.utcnow().date() + timedelta(days=12)
            beta.subscription_plan_ref = "yearly"
            beta.subscription_expiry = datetime.utcnow().date() + timedelta(days=120)
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_admin_for_tenant(beta, "beta_admin")
            self._seed_super_admin()
            self.db.session.commit()
            alpha_id = alpha.id

        self._login("alpha", "alpha_admin")
        denied = self.client.get("/super-admin", follow_redirects=True)
        self.assertIn(b"Super admin access required", denied.data)

        self.client.get("/logout")
        self._login_super_admin()
        dashboard = self.client.get("/super-admin")
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn(b"Total Pharmacies", dashboard.data)
        self.assertIn(b"Active Subscriptions", dashboard.data)
        self.assertIn(b"Total Users", dashboard.data)
        self.assertIn(b"Expiring Subscriptions", dashboard.data)
        self.assertIn(b"Alpha Pharmacy", dashboard.data)
        self.assertIn(b"Beta Pharmacy", dashboard.data)

        filtered = self.client.get("/super-admin?q=alpha&status=expiring")
        self.assertEqual(filtered.status_code, 200)
        self.assertIn(b"Alpha Pharmacy", filtered.data)
        self.assertNotIn(b"Beta Pharmacy", filtered.data)

        response = self.client.post(
            f"/super-admin/tenants/{alpha_id}/access",
            data={"enabled": "0", "next": "/super-admin?q=alpha"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"System access disabled", response.data)
        with self.app.app_context(), self.app_module.tenant_scope_disabled():
            tenant = self.app_module.Tenant.query.get(alpha_id)
            self.assertEqual(tenant.status, "Suspended")

    def test_tenant_master_updates_current_tenant(self):
        with self.app.app_context():
            tenant = self._seed_tenant(code="alpha")
            self._seed_user(tenant=tenant)

        login_response = self.client.post(
            "/login",
            data={"tenant": "alpha", "username": "admin", "password": "Admin@123"},
        )
        self.assertEqual(login_response.status_code, 302)
        response = self.client.post(
            "/tenant-master",
            data={
                "pharmacy_name": "Alpha Care Pharmacy",
                "owner_name": "Dr. Alpha",
                "contact_number": "9876543210",
                "email": "alpha@example.test",
                "business_address": "Clinic Road",
                "gstin": "22AAAAA0000A1Z5",
                "drug_license": "DL-123",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            tenant = self.app_module.Tenant.query.filter_by(tenant_code="alpha").one()
            self.assertEqual(tenant.pharmacy_name, "Alpha Care Pharmacy")
            self.assertEqual(tenant.owner_name, "Dr. Alpha")
            self.assertEqual(tenant.subscription_plan_ref, "test")
            self.assertEqual(tenant.status, "Active")

    def test_tenant_master_rejects_forged_subscription_fields(self):
        with self.app.app_context():
            tenant = self._seed_tenant(code="alpha")
            self._seed_user(tenant=tenant)
            tenant_id = tenant.id

        self._login("alpha", "admin", "Admin@123")
        page = self.client.get("/tenant-master")
        self.assertNotIn(b"Subscription Plan Reference", page.data)
        self.assertNotIn(b"Tenant Status", page.data)
        response = self.client.post(
            "/tenant-master",
            data={
                "pharmacy_name": "Forged Pharmacy",
                "subscription_plan_ref": "pro",
                "subscription_start": "2026-10-09",
                "subscription_expiry": "2027-10-09",
                "status": "Active",
            },
        )
        self.assertEqual(response.status_code, 403)

        with self.app.app_context(), self.app_module.tenant_context(tenant_id):
            tenant = self.app_module.Tenant.query.one()
            self.assertEqual(tenant.pharmacy_name, "Alpha Pharmacy")
            self.assertEqual(tenant.subscription_plan_ref, "test")
            self.assertEqual(tenant.status, "Active")

    def test_invoice_patient_upsert_reuses_only_current_tenant_patient(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            alpha_id, beta_id = alpha.id, beta.id

            with self.app_module.tenant_context(alpha_id):
                patient_a, _ = self.app_module.upsert_patient_from_invoice(
                    "Alpha Patient", "9876543210"
                )
                patient_a_id = patient_a.id
                self.db.session.commit()

            with self.app_module.tenant_context(beta_id):
                patient_b, _ = self.app_module.upsert_patient_from_invoice(
                    "Beta Patient", "9876543210"
                )
                patient_b_id = patient_b.id
                self.db.session.commit()

            with self.app_module.tenant_context(alpha_id):
                reused, _ = self.app_module.upsert_patient_from_invoice(
                    "Alpha Patient Updated", "9876543210"
                )
                self.assertEqual(reused.id, patient_a_id)
                self.assertEqual(reused.tenant_id, alpha_id)
                self.db.session.commit()

            self.assertNotEqual(patient_a_id, patient_b_id)
            self.assertEqual(patient_b.tenant_id, beta_id)
            self.assertEqual(patient_b.name, "Beta Patient")

    def test_startup_data_backfill_runs_in_each_tenant_context(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            seen_tenants = []

            def record_tenant_context():
                seen_tenants.append(self.app_module.current_tenant_id())
                return 1

            updated = self.app_module.run_tenant_scoped_startup_backfill(
                record_tenant_context
            )

            self.assertEqual(updated, 2)
            self.assertEqual(seen_tenants, [alpha.id, beta.id])
            self.assertIsNone(self.app_module.current_tenant_id())

    def test_background_job_outputs_are_separate_by_tenant(self):
        from services.background_jobs import (
            _process_report_export_queue,
            _write_expiry_alert_snapshot,
            _write_whatsapp_reminder_outbox,
            queue_report_export_job,
        )

        original_instance_path = self.app.instance_path
        self.app.instance_path = os.path.join(self.temp_dir.name, "job-instance")
        try:
            with self.app.app_context():
                alpha = self._seed_tenant(code="alpha")
                beta = self._seed_tenant(code="beta")
                tomorrow = datetime.utcnow().date() + timedelta(days=1)
                expiring_on = (datetime.utcnow().date() + timedelta(days=10)).isoformat()
                tenant_rows = ((alpha, "Alpha Appointment", "ALPHA MED"), (beta, "Beta Appointment", "BETA MED"))
                report_jobs = {}
                for tenant, patient_name, medicine_name in tenant_rows:
                    with self.app_module.tenant_context(tenant.id):
                        self.db.session.add(
                            self.app_module.Appointment(
                                tenant_id=tenant.id,
                                appointment_no=f"{tenant.tenant_code.upper()}-1",
                                patient_name=patient_name,
                                mobile="9876543210",
                                doctor_name="Dr. Test",
                                appointment_date=tomorrow,
                                appointment_time=datetime.utcnow().time(),
                                created_by="STAFF",
                            )
                        )
                        self.db.session.add(
                            self.app_module.Medicine(
                                tenant_id=tenant.id,
                                name=medicine_name,
                                batch=f"{tenant.tenant_code.upper()}-1",
                                expiry=expiring_on,
                                mrp=10,
                                qty=3,
                            )
                        )
                        self.db.session.commit()
                        _write_whatsapp_reminder_outbox(self.app)
                        _write_expiry_alert_snapshot(self.app)
                        job = queue_report_export_job(
                            self.app,
                            {"route": "/reports/export", "queued_by": "tenant-admin"},
                        )
                        report_jobs[tenant.id] = job["job_id"]

                for tenant, *_ in tenant_rows:
                    with self.app_module.tenant_context(tenant.id):
                        _process_report_export_queue(self.app)

                for tenant, own_patient, other_patient, own_medicine, other_medicine in (
                    (alpha, "Alpha Appointment", "Beta Appointment", "ALPHA MED", "BETA MED"),
                    (beta, "Beta Appointment", "Alpha Appointment", "BETA MED", "ALPHA MED"),
                ):
                    outbox_dir = os.path.join(
                        self.app.instance_path, "outbox", f"tenant-{tenant.id}"
                    )
                    alert_dir = os.path.join(
                        self.app.instance_path, "alerts", f"tenant-{tenant.id}"
                    )
                    with open(os.path.join(outbox_dir, "whatsapp_reminders.json"), encoding="utf-8") as handle:
                        reminders = json.load(handle)
                    with open(os.path.join(alert_dir, "expiry_alerts.json"), encoding="utf-8") as handle:
                        alerts = json.load(handle)
                    self.assertIn(own_patient, json.dumps(reminders))
                    self.assertNotIn(other_patient, json.dumps(reminders))
                    self.assertIn(own_medicine, json.dumps(alerts))
                    self.assertNotIn(other_medicine, json.dumps(alerts))
                    self.assertEqual(os.stat(outbox_dir).st_mode & 0o777, 0o700)
                    self.assertEqual(os.stat(alert_dir).st_mode & 0o777, 0o700)
                    ready_dir = os.path.join(
                        self.app.instance_path,
                        "report_jobs",
                        "ready",
                        f"tenant-{tenant.id}",
                    )
                    ready_job = os.path.join(ready_dir, report_jobs[tenant.id] + ".json")
                    self.assertTrue(os.path.isfile(ready_job))
                    with open(ready_job, encoding="utf-8") as handle:
                        queued_payload = json.load(handle)
                    self.assertEqual(queued_payload["tenant_id"], tenant.id)
                self.assertIsNone(self.app_module.current_tenant_id())
        finally:
            self.app.instance_path = original_instance_path

    def test_private_vendor_uploads_are_not_served_from_static_urls(self):
        for path in (
            "/static/uploads/vendors/tenant-1/private.pdf",
            "/static/uploads/vendor_bills/tenant-1/private.pdf",
        ):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 404)

    def test_usernames_and_patient_mobile_are_unique_within_tenant_only(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_user(username="shared-admin", tenant=alpha)
            self._seed_user(username="shared-admin", tenant=beta)

            with self.app_module.tenant_context(alpha.id):
                patient_a = self.app_module.Patient(
                    tenant_id=alpha.id,
                    name="Alpha Patient",
                    mobile="9876543210",
                )
                self.db.session.add(patient_a)
                self.db.session.commit()
            with self.app_module.tenant_context(beta.id):
                patient_b = self.app_module.Patient(
                    tenant_id=beta.id,
                    name="Beta Patient",
                    mobile="9876543210",
                )
                self.db.session.add(patient_b)
                self.db.session.commit()

            with self.app_module.tenant_context(alpha.id):
                duplicate_user = self.app_module.User(
                    tenant_id=alpha.id, username="SHARED-ADMIN", role="staff"
                )
                self.db.session.add(duplicate_user)
                with self.assertRaises(IntegrityError):
                    self.db.session.commit()
                self.db.session.rollback()

                duplicate_patient = self.app_module.Patient(
                    tenant_id=alpha.id,
                    name="Duplicate Patient",
                    mobile="9876543210",
                )
                self.db.session.add(duplicate_patient)
                with self.assertRaises(IntegrityError):
                    self.db.session.commit()
                self.db.session.rollback()

    def test_vendor_note_creation_rejects_cross_tenant_vendor_and_purchase(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="note-alpha")
            beta = self._seed_tenant(code="note-beta")
            self._seed_admin_for_tenant(alpha, "note_admin")
            self._seed_admin_for_tenant(beta, "note_beta_admin")
            with self.app_module.tenant_scope_disabled():
                vendor_a = self.app_module.Vendor(tenant_id=alpha.id, name="Alpha Vendor")
                vendor_b = self.app_module.Vendor(tenant_id=beta.id, name="Beta Vendor")
                self.db.session.add_all([vendor_a, vendor_b])
                self.db.session.flush()
                purchase_b = self.app_module.VendorPurchase(
                    tenant_id=beta.id,
                    vendor_id=vendor_b.id,
                    purchase_no="BETA-1",
                    invoice_no="BETA-SUP-1",
                    purchase_date=datetime.utcnow(),
                    payment_mode="CASH",
                    payment_status="Paid",
                    total_amount=10,
                    created_by="beta_admin",
                )
                self.db.session.add(purchase_b)
                self.db.session.commit()
                alpha_id, beta_id, alpha_vendor_id, beta_vendor_id, beta_purchase_id = (
                    alpha.id,
                    beta.id,
                    vendor_a.id,
                    vendor_b.id,
                    purchase_b.id,
                )

        self._login("note-alpha", "note_admin", "Admin@123")
        request_data = {
            "note_type": "CREDIT",
            "note_date": date.today().isoformat(),
        }
        cross_tenant_vendor = self.client.post(
            "/api/vendor-notes",
            json={**request_data, "vendor_id": beta_vendor_id},
        )
        self.assertEqual(cross_tenant_vendor.status_code, 400)

        cross_tenant_purchase = self.client.post(
            "/api/vendor-notes",
            json={
                **request_data,
                "vendor_id": alpha_vendor_id,
                "reference_purchase_id": beta_purchase_id,
            },
        )
        self.assertEqual(cross_tenant_purchase.status_code, 400)
        alpha_note = self.client.post(
            "/api/vendor-notes",
            json={
                **request_data,
                "vendor_id": alpha_vendor_id,
                "mode": "AMOUNT_ONLY",
                "grand_total": 5,
            },
        )
        self.assertEqual(alpha_note.status_code, 201, alpha_note.get_json())

        self._login("note-beta", "note_beta_admin", "Admin@123")
        beta_note = self.client.post(
            "/api/vendor-notes",
            json={
                **request_data,
                "vendor_id": beta_vendor_id,
                "mode": "AMOUNT_ONLY",
                "grand_total": 5,
            },
        )
        self.assertEqual(beta_note.status_code, 201, beta_note.get_json())
        self.assertEqual(alpha_note.get_json()["note_no"], beta_note.get_json()["note_no"])
        with self.app.app_context():
            with self.app_module.tenant_context(alpha_id):
                self.assertEqual(self.app_module.VendorNote.query.count(), 1)
            with self.app_module.tenant_context(beta_id):
                self.assertEqual(self.app_module.VendorNote.query.count(), 1)

    def test_pharmacy_business_setup_is_super_admin_scoped_and_invoice_identity_is_snapshotted(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_admin_for_tenant(beta, "beta_admin")
            self._seed_super_admin()
            alpha_id, beta_id = alpha.id, beta.id

        client_a = self.app.test_client()
        response = client_a.post(
            "/login",
            data={"tenant": "alpha", "username": "alpha_admin", "password": "Admin@123"},
        )
        self.assertEqual(response.status_code, 302)
        dashboard_a = client_a.get("/")
        self.assertNotIn(b"/settings/pharmacy-business", dashboard_a.data)
        setup_a = client_a.get("/settings/pharmacy-business", follow_redirects=True)
        self.assertEqual(setup_a.status_code, 200)
        self.assertIn(b"managed by the platform super admin", setup_a.data)

        owner = self.app.test_client()
        self._login_super_admin()
        owner = self.client
        setup_page = owner.get(f"/super-admin/tenants/{alpha_id}/pharmacy-business")
        self.assertEqual(setup_page.status_code, 200)
        self.assertIn(b"Legal Business Name", setup_page.data)
        self.assertIn(b"Invoice Footer Message", setup_page.data)

        png_file = io.BytesIO()
        Image.new("RGBA", (1, 1), (20, 80, 140, 255)).save(png_file, format="PNG")
        png = png_file.getvalue()
        response = owner.post(
            f"/super-admin/tenants/{alpha_id}/pharmacy-business",
            data={
                "pharmacy_name": "Alpha Community Pharmacy",
                "legal_business_name": "Alpha Community Healthcare LLP",
                "owner_name": "Alpha Owner",
                "contact_number": "9111111111",
                "alternate_mobile": "9222222222",
                "email": "alpha@example.test",
                "business_address": "1 Alpha Street",
                "city": "Jaipur",
                "state": "Rajasthan",
                "pin_code": "302001",
                "gstin": "08ABAFT0637R1ZP",
                "drug_license": "ALPHA-DL-01",
                "business_type": "Retail pharmacy",
                "invoice_header": "Alpha header",
                "invoice_footer": "Alpha footer",
                "invoice_terms": "Alpha terms",
                "authorized_signatory": "Alpha Signatory",
                "invoice_prefix": "ALP",
                "pharmacy_logo": (io.BytesIO(png), "alpha.png"),
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Pharmacy Business Setup saved.", response.data)

        with self.app.app_context(), self.app_module.tenant_context(alpha_id):
            alpha_invoice = self.app_module.Invoice(
                tenant_id=alpha_id,
                invoice_no="ALP-00001",
                customer="Alpha Patient",
                subtotal=100,
                total=100,
                created_at=datetime.utcnow(),
            )
            self.app_module.apply_invoice_print_profile(alpha_invoice, alpha_invoice.created_at)
            self.db.session.add(alpha_invoice)
            self.db.session.commit()
            alpha_invoice_id = alpha_invoice.id
            saved_identity = self.app_module.tenant_business_identity(
                self.app_module.Tenant.query.get(alpha_id)
            )
            self.assertEqual(saved_identity["legal_business_name"], "Alpha Community Healthcare LLP")
            self.assertEqual(saved_identity["invoice_footer"], "Alpha footer")
            alpha_logo_path = saved_identity["logo_path"]

        setup_reload = owner.get(f"/super-admin/tenants/{alpha_id}/pharmacy-business")
        self.assertIn(b"Alpha Community Healthcare LLP", setup_reload.data)
        logo_response = client_a.get(alpha_logo_path)
        self.assertEqual(logo_response.status_code, 200)
        self.assertEqual(logo_response.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertIn(b"\x89PNG", logo_response.data)

        response = owner.post(
            f"/super-admin/tenants/{alpha_id}/pharmacy-business",
            data={
                "pharmacy_name": "Alpha Updated Pharmacy",
                "legal_business_name": "Alpha Updated Healthcare LLP",
                "owner_name": "Alpha Owner",
                "contact_number": "9111111111",
                "email": "alpha@example.test",
                "business_address": "1 Alpha Street",
                "city": "Jaipur",
                "state": "Rajasthan",
                "pin_code": "302001",
                "gstin": "08ABAFT0637R1ZP",
                "drug_license": "ALPHA-DL-01",
                "invoice_header": "Updated header",
                "invoice_footer": "Updated footer",
                "invoice_prefix": "ALP",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        old_invoice_response = client_a.get(f"/invoice/{alpha_invoice_id}")
        self.assertIn(b"Alpha Updated Pharmacy", old_invoice_response.data)
        self.assertIn(b"Updated header", old_invoice_response.data)
        self.assertIn(b"Updated footer", old_invoice_response.data)
        self.assertNotIn(b"Alpha Community Pharmacy", old_invoice_response.data)

        with self.app.app_context(), self.app_module.tenant_context(alpha_id):
            new_invoice = self.app_module.Invoice(
                tenant_id=alpha_id,
                invoice_no="ALP-00002",
                customer="Alpha Patient 2",
                subtotal=100,
                total=100,
                created_at=datetime.utcnow(),
            )
            self.app_module.apply_invoice_print_profile(new_invoice, new_invoice.created_at)
            self.db.session.add(new_invoice)
            self.db.session.commit()
            new_invoice_id = new_invoice.id
        new_invoice_response = client_a.get(f"/invoice/{new_invoice_id}")
        self.assertIn(b"Alpha Updated Pharmacy", new_invoice_response.data)
        self.assertIn(b"Updated header", new_invoice_response.data)
        self.assertIn(b"Updated footer", new_invoice_response.data)

        response = owner.post(
            f"/super-admin/tenants/{beta_id}/pharmacy-business",
            data={
                "pharmacy_name": "Beta Independent Pharmacy",
                "legal_business_name": "Beta Health Limited",
                "owner_name": "Beta Owner",
                "contact_number": "9333333333",
                "email": "beta@example.test",
                "business_address": "2 Beta Road",
                "city": "Udaipur",
                "state": "Rajasthan",
                "pin_code": "313001",
                "drug_license": "BETA-DL-01",
                "invoice_prefix": "BET",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        with self.app.app_context(), self.app_module.tenant_context(beta_id):
            beta_invoice = self.app_module.Invoice(
                tenant_id=beta_id,
                invoice_no="BET-00001",
                customer="Beta Patient",
                subtotal=100,
                total=100,
                created_at=datetime.utcnow(),
            )
            self.app_module.apply_invoice_print_profile(beta_invoice, beta_invoice.created_at)
            self.db.session.add(beta_invoice)
            self.db.session.commit()
            beta_invoice_id = beta_invoice.id
        client_b = self.app.test_client()
        self.assertEqual(
            client_b.post(
                "/login",
                data={"tenant": "beta", "username": "beta_admin", "password": "Admin@123"},
            ).status_code,
            302,
        )
        beta_invoice_response = client_b.get(f"/invoice/{beta_invoice_id}")
        self.assertIn(b"Beta Independent Pharmacy", beta_invoice_response.data)
        self.assertNotIn(b"Alpha Updated Pharmacy", beta_invoice_response.data)
        self.assertEqual(client_b.get(alpha_logo_path).status_code, 404)
        self.assertEqual(self.app.test_client().get(alpha_logo_path).status_code, 404)

    def test_pos_billing_and_profit_summary_are_tenant_scoped(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_admin_for_tenant(beta, "beta_admin")
            alpha_med = self._seed_medicine(alpha, "SHARED MED", "A1")
            beta_med = self._seed_medicine(beta, "SHARED MED", "B1")
            with self.app_module.tenant_context(alpha.id):
                alpha_med.discount_percent = 0
                self.db.session.commit()
            with self.app_module.tenant_context(beta.id):
                beta_med.discount_percent = 10
                self.db.session.commit()
            tenant_ids = (alpha.id, beta.id)

        clients = (self.app.test_client(), self.app.test_client())
        for client, code, username in (
            (clients[0], "alpha", "alpha_admin"),
            (clients[1], "beta", "beta_admin"),
        ):
            login_response = client.post(
                "/login",
                data={
                    "tenant": code,
                    "username": username,
                    "password": "Admin@123",
                },
            )
            self.assertEqual(login_response.status_code, 302)
            billing_response = client.post(
                "/billing",
                data={
                    "customer": f"{code.title()} Patient",
                    "mobile": "9876543210",
                    "payment_mode": "CASH",
                    "medicine_name": ["SHARED MED"],
                    "qty": ["2"],
                    "batch_override[]": ["A1" if code == "alpha" else "B1"],
                },
            )
            self.assertEqual(billing_response.status_code, 200)
            self.assertIn(b"INV-", billing_response.data)

        report_totals = []
        for tenant_id in tenant_ids:
            with self.app.app_context(), self.app_module.tenant_context(tenant_id):
                invoice = self.app_module.Invoice.query.one()
                item = self.app_module.InvoiceItem.query.one()
                patient = self.app_module.Patient.query.one()
                medicine = self.app_module.Medicine.query.one()
                day = datetime.utcnow().date()
                summary, error = self.app_module.build_profit_report_summary(
                    (day - timedelta(days=1)).isoformat(),
                    (day + timedelta(days=1)).isoformat(),
                )
                self.assertIsNone(error)
                self.assertEqual(invoice.tenant_id, tenant_id)
                self.assertEqual(item.invoice_id, invoice.id)
                self.assertEqual(patient.tenant_id, tenant_id)
                self.assertEqual(patient.mobile, "9876543210")
                self.assertEqual(medicine.tenant_id, tenant_id)
                self.assertEqual(medicine.qty, 8)
                self.assertEqual(summary["sales_total"], invoice.subtotal)
                report_totals.append(summary["sales_total"])
        self.assertNotEqual(report_totals[0], report_totals[1])

    def test_orm_reads_are_scoped_to_current_tenant(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_admin_for_tenant(beta, "beta_admin")
            alpha_med = self._seed_medicine(alpha, "ALPHA MED", "A1")
            beta_med = self._seed_medicine(beta, "BETA MED", "B1")
            alpha_tenant_id = alpha.id
            alpha_id, beta_id = alpha_med.id, beta_med.id

        self._login("alpha", "alpha_admin")
        with self.app.app_context(), self.app_module.tenant_context(alpha_tenant_id):
            names = [row.name for row in self.app_module.Medicine.query.order_by(self.app_module.Medicine.id).all()]
            self.assertEqual(names, ["ALPHA MED"])
            self.assertIsNotNone(self.app_module.Medicine.query.filter_by(id=alpha_id).first())
            self.assertIsNone(self.app_module.Medicine.query.filter_by(id=beta_id).first())

    def test_cross_tenant_update_delete_and_reference_are_blocked(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            alpha_med = self._seed_medicine(alpha, "ALPHA MED", "A1")
            beta_med = self._seed_medicine(beta, "BETA MED", "B1")
            with self.app_module.tenant_scope_disabled():
                beta_patient = self.app_module.Patient(
                    tenant_id=beta.id,
                    name="Beta Patient",
                )
                beta_location = self.app_module.ClinicLocation(
                    tenant_id=beta.id,
                    code="BETA",
                    display_name="Beta Location",
                )
                beta_clinician = self.app_module.Clinician(
                    tenant_id=beta.id,
                    code="BETA",
                    display_name="Beta Clinician",
                )
                beta_invoice = self.app_module.Invoice(
                    tenant_id=beta.id,
                    invoice_no="BETA-INV-1",
                    customer="Beta Patient",
                    total=10,
                    created_by="beta_admin",
                )
                self.db.session.add_all(
                    [beta_patient, beta_location, beta_clinician, beta_invoice]
                )
                self.db.session.commit()
            alpha_tenant_id = alpha.id
            alpha_med_name = alpha_med.name
            beta_med_id = beta_med.id
            beta_invoice_id = beta_invoice.id
            beta_patient_id = beta_patient.id
            beta_location_id = beta_location.id
            beta_clinician_id = beta_clinician.id

        with self.app.app_context(), self.app_module.tenant_context(alpha_tenant_id):
            with self.app_module.tenant_scope_disabled():
                beta_med = self.db.session.get(self.app_module.Medicine, beta_med_id)
            beta_med.qty = 99
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            bad_invoice = self.app_module.Invoice(
                invoice_no="ALPHA-INV-1",
                customer="Alpha Patient",
                patient_id=beta_patient_id,
            )
            self.db.session.add(bad_invoice)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            bad_stock_history = self.app_module.StockHistory(
                medicine_id=beta_med_id,
                action="ADJUST",
                qty_change=1,
            )
            self.db.session.add(bad_stock_history)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            missing_stock_reference = self.app_module.StockHistory(
                medicine_id=999999,
                action="ADJUST",
                qty_change=1,
            )
            self.db.session.add(missing_stock_reference)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            bad_appointment = self.app_module.Appointment(
                patient_name="Alpha Patient",
                doctor_name="Beta Clinician",
                clinician_id=beta_clinician_id,
                location_id=beta_location_id,
                appointment_date=datetime.utcnow().date(),
                appointment_time=datetime.utcnow().time(),
            )
            self.db.session.add(bad_appointment)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            bad_location_appointment = self.app_module.Appointment(
                patient_name="Alpha Patient",
                doctor_name="Alpha Clinician",
                location_id=beta_location_id,
                appointment_date=datetime.utcnow().date(),
                appointment_time=datetime.utcnow().time(),
            )
            self.db.session.add(bad_location_appointment)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            with self.app_module.tenant_scope_disabled():
                beta_med = self.db.session.get(self.app_module.Medicine, beta_med_id)
            self.db.session.delete(beta_med)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

            bad_item = self.app_module.InvoiceItem(
                invoice_id=beta_invoice_id,
                name=alpha_med_name,
                qty=1,
                price=10,
                amount=10,
            )
            self.db.session.add(bad_item)
            with self.assertRaises(PermissionError):
                self.db.session.commit()
            self.db.session.rollback()

    def test_quarantine_actions_cannot_access_another_tenants_record(self):
        with self.app.app_context():
            alpha = self._seed_tenant("quarantine-alpha")
            beta = self._seed_tenant("quarantine-beta")
            self._seed_admin_for_tenant(alpha, "quarantine-alpha-admin", "Admin@123")
            self._seed_admin_for_tenant(beta, "quarantine-beta-admin", "Admin@123")
            with self.app_module.tenant_context(beta.id):
                medicine = self.app_module.Medicine(
                    name="BETA MEDICINE",
                    batch="BETA-BATCH",
                    expiry="2027-12-31",
                    mrp=10,
                    qty=0,
                    is_active=True,
                )
                self.db.session.add(medicine)
                self.db.session.flush()
                quarantine = self.app_module.QuarantineStock(
                    tenant_id=beta.id,
                    medicine_id=medicine.id,
                    medicine_name=medicine.name,
                    batch=medicine.batch,
                    qty=2,
                    reason="DAMAGED",
                    status="PENDING",
                )
                self.db.session.add(quarantine)
                self.db.session.commit()
                quarantine_id = quarantine.id
            alpha_tenant_code = alpha.tenant_code
            beta_tenant_id = beta.id

        self._login(alpha_tenant_code, "quarantine-alpha-admin", "Admin@123")
        response = self.client.post(
            f"/quarantine-stock/{quarantine_id}/write-off",
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 404)
        with self.app.app_context(), self.app_module.tenant_context(beta_tenant_id):
            saved = self.app_module.QuarantineStock.query.get(quarantine_id)
            self.assertEqual(saved.status, "PENDING")

    def test_connection_reuse_does_not_leak_tenant_context(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_admin_for_tenant(beta, "beta_admin")
            self._seed_medicine(alpha, "ALPHA MED", "A1")
            self._seed_medicine(beta, "BETA MED", "B1")

        self._login("alpha", "alpha_admin")
        self.assertIn(b"ALPHA MED", self.client.get("/api/global-search?q=med").data)
        self.assertNotIn(b"BETA MED", self.client.get("/api/global-search?q=med").data)

        self.client.get("/logout")
        self._login("beta", "beta_admin")
        self.assertIn(b"BETA MED", self.client.get("/api/global-search?q=med").data)
        self.assertNotIn(b"ALPHA MED", self.client.get("/api/global-search?q=med").data)

    def test_system_center_backup_area_is_super_admin_only(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_super_admin()

        self._login("alpha", "alpha_admin")
        tenant_response = self.client.get("/system-center")
        self.assertEqual(tenant_response.status_code, 302)
        self.assertTrue(tenant_response.headers.get("Location", "").endswith("/"))

        dashboard_response = self.client.get("/")
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertNotIn(b"/system-center", dashboard_response.data)

        self.client.get("/logout")
        self._login_super_admin()
        dashboard_response = self.client.get("/super-admin")
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertIn(b"/system-center", dashboard_response.data)
        owner_response = self.client.get("/system-center")
        self.assertEqual(owner_response.status_code, 200)
        self.assertIn(b"System Center", owner_response.data)
        self.assertIn(b"Backup & Restore", owner_response.data)

    def test_super_admin_can_disable_appointment_and_lab_modules_per_tenant(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            tenant_id = alpha.id
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_super_admin()
            self.assertFalse(self.app_module.tenant_feature_enabled("appointments", alpha))
            self.assertFalse(self.app_module.tenant_feature_enabled("lab", alpha))

        self._login_super_admin()
        response = self.client.post(
            f"/super-admin/tenants/{tenant_id}/subscription",
            data={
                "plan_ref": "custom",
                "status": "Active",
                "subscription_start": "2026-10-11",
                "subscription_expiry": "2026-11-10",
                "amount": "0",
                "feature_pos": "1",
                "feature_inventory": "1",
                "feature_reports": "1",
                "feature_exports": "1",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context(), self.app_module.tenant_scope_disabled():
            tenant = self.app_module.Tenant.query.get(tenant_id)
            features = self.app_module.tenant_subscription_settings(tenant)["features"]
            self.assertFalse(features["appointments"])
            self.assertFalse(features["lab"])

        self.client.get("/logout")
        self._login("alpha", "alpha_admin")
        dashboard_response = self.client.get("/")
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertNotIn(b"/appointments", dashboard_response.data)
        self.assertNotIn(b"/lab-billing", dashboard_response.data)
        self.assertNotIn(b"Appointments Today", dashboard_response.data)

        appointment_response = self.client.get("/appointments")
        self.assertEqual(appointment_response.status_code, 302)
        self.assertTrue(appointment_response.headers.get("Location", "").endswith("/"))
        lab_response = self.client.get("/lab-billing")
        self.assertEqual(lab_response.status_code, 302)
        self.assertTrue(lab_response.headers.get("Location", "").endswith("/"))

    def test_invoice_share_tokens_are_bound_to_tenant(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            with self.app_module.tenant_context(alpha.id):
                invoice = self.app_module.Invoice(
                    tenant_id=alpha.id,
                    invoice_no="ALPHA-SHARE-1",
                    customer="Alpha Patient",
                    total=100,
                    created_at=datetime.utcnow(),
                )
                self.db.session.add(invoice)
                self.db.session.commit()
                token = self.app_module.build_invoice_share_token(invoice)
                invoice_id = invoice.id
            alpha_id, beta_id = alpha.id, beta.id

        payload = self.app_module.read_invoice_share_payload(token)
        self.assertEqual(payload["invoice_id"], invoice_id)
        self.assertEqual(payload["tenant_id"], alpha_id)
        self.assertNotEqual(payload["tenant_id"], beta_id)

        public_response = self.app.test_client().get(f"/invoice/share/{token}")
        self.assertEqual(public_response.status_code, 200)
        self.assertIn(b"Alpha Patient", public_response.data)

    def test_tenant_computer_limit_blocks_new_browser_until_reset(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            self._seed_super_admin()
            self.app_module.update_tenant_device_registry(alpha, limit=1, devices={})
            self.db.session.commit()
            alpha_id = alpha.id

        first_browser = self.app.test_client()
        first_login = first_browser.post(
            "/login",
            data={"tenant": "alpha", "username": "alpha_admin", "password": "Admin@123"},
        )
        self.assertEqual(first_login.status_code, 302)
        self.assertIn("tenant_device_id", first_login.headers.get("Set-Cookie", ""))

        second_browser = self.app.test_client()
        blocked = second_browser.post(
            "/login",
            data={"tenant": "alpha", "username": "alpha_admin", "password": "Admin@123"},
        )
        self.assertEqual(blocked.status_code, 200)
        self.assertIn(b"allowed on 1 computer", blocked.data)

        owner = self.app.test_client()
        owner.post(
            "/login",
            data={"username": "owner", "password": "Owner@123"},
        )
        reset = owner.post(f"/super-admin/tenants/{alpha_id}/devices/reset")
        self.assertEqual(reset.status_code, 302)

        allowed_after_reset = second_browser.post(
            "/login",
            data={"tenant": "alpha", "username": "alpha_admin", "password": "Admin@123"},
        )
        self.assertEqual(allowed_after_reset.status_code, 302)

    def test_unauthorized_tenant_switching_is_rejected(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            self._seed_admin_for_tenant(alpha, "alpha_admin")
            beta_tenant_id = beta.id

        self._login("alpha", "alpha_admin")
        with self.client.session_transaction() as session:
            session["tenant_id"] = beta_tenant_id
            session["tenant_code"] = "beta"
        response = self.client.get("/", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers.get("Location", ""))

    def test_business_numbers_are_tenant_scoped_and_settings_driven(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")
            alpha.settings.settings_json = json.dumps({
                "invoice_prefix": "AX",
                "purchase_prefix": "AP",
                "return_prefix": "AR",
            })
            self.db.session.commit()

            with self.app_module.tenant_scope_disabled():
                self.db.session.add(self.app_module.Invoice(
                    tenant_id=alpha.id,
                    invoice_no="DUP-1",
                    customer="Alpha",
                    created_at=datetime(2026, 10, 9),
                ))
                self.db.session.add(self.app_module.Invoice(
                    tenant_id=beta.id,
                    invoice_no="DUP-1",
                    customer="Beta",
                    created_at=datetime(2026, 10, 9),
                ))
                self.db.session.commit()

            with self.app_module.tenant_context(alpha.id):
                invoice = self.app_module.Invoice(customer="Alpha Seq", created_at=datetime(2026, 10, 9))
                self.db.session.add(invoice)
                self.db.session.flush()
                self.assertEqual(self.app_module.assign_invoice_number(invoice, datetime(2026, 10, 9)), "AX-00001")
                self.db.session.rollback()

            with self.app_module.tenant_context(beta.id):
                invoice = self.app_module.Invoice(customer="Beta Seq", created_at=datetime(2026, 10, 9))
                self.db.session.add(invoice)
                self.db.session.flush()
                self.assertEqual(self.app_module.assign_invoice_number(invoice, datetime(2026, 10, 9)), "INV-00001")
                self.db.session.rollback()

            with self.app_module.tenant_scope_disabled():
                self.db.session.add(self.app_module.Invoice(
                    tenant_id=alpha.id,
                    invoice_no="DUP-1",
                    customer="Alpha Duplicate",
                    created_at=datetime(2026, 10, 9),
                ))
                with self.assertRaises(IntegrityError):
                    self.db.session.commit()
                self.db.session.rollback()

    def test_uploaded_files_are_written_under_tenant_directory(self):
        with self.app.app_context():
            alpha = self._seed_tenant(code="alpha")
            beta = self._seed_tenant(code="beta")

            with self.app_module.tenant_context(alpha.id):
                stored_name = self.app_module.save_uploaded_file(
                    FileStorage(stream=io.BytesIO(b"pdf-ish"), filename="bill.pdf"),
                    self.app_module.VENDOR_BILL_UPLOAD_FOLDER,
                    "vendorbill",
                )
                alpha_name, alpha_base, alpha_path = self.app_module.resolve_tenant_upload_file(
                    self.app_module.VENDOR_BILL_UPLOAD_FOLDER,
                    stored_name,
                )
                self.assertEqual(alpha_name, stored_name)
                self.assertIn(f"tenant-{alpha.id}", alpha_base)
                self.assertTrue(os.path.exists(alpha_path))

            with self.app_module.tenant_context(beta.id):
                _beta_name, beta_base, beta_path = self.app_module.resolve_tenant_upload_file(
                    self.app_module.VENDOR_BILL_UPLOAD_FOLDER,
                    stored_name,
                )
                self.assertIn(f"tenant-{beta.id}", beta_base)
                self.assertFalse(os.path.exists(beta_path))

    def test_super_admin_can_create_tenant_and_subscription_history(self):
        with self.app.app_context():
            self._seed_super_admin()

        self._login_super_admin()
        response = self.client.post(
            "/super-admin/tenants/create",
            data={
                "pharmacy_name": "Gamma Pharmacy",
                "tenant_code": "gamma",
                "owner_name": "Gamma Owner",
                "admin_username": "gamma_admin",
                "plan_ref": "monthly",
                "amount": "1999",
                "user_limit": "4",
                "payment_reference": "MANUAL-1",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context(), self.app_module.tenant_scope_disabled():
            tenant = self.app_module.Tenant.query.filter_by(tenant_code="gamma").one()
            self.assertEqual(tenant.subscription_plan_ref, "monthly")
            self.assertEqual(tenant.status, "Active")
            history = self.app_module.SubscriptionHistory.query.filter_by(tenant_id=tenant.id).one()
            self.assertEqual(history.plan_ref, "monthly")
            with self.app_module.tenant_context(tenant.id):
                admin = self.app_module.User.query.filter_by(username="gamma_admin").one()
                self.assertEqual(admin.role, "admin")
                self.assertTrue(admin.check_password("not-the-password") is False)

    def test_expired_subscription_blocks_mutations_but_allows_reads(self):
        with self.app.app_context():
            tenant = self._seed_tenant(code="alpha", status="Expired")
            self._seed_admin_for_tenant(tenant, "alpha_admin")
            tenant_id = tenant.id

        self._login("alpha", "alpha_admin")
        read_response = self.client.get("/billing")
        self.assertEqual(read_response.status_code, 200)

        write_response = self.client.post(
            "/users/add",
            data={
                "username": "blocked_user",
                "password": "Admin@123",
                "confirm_password": "Admin@123",
                "role": "staff",
                "access_profile": "custom",
            },
            follow_redirects=False,
        )
        self.assertEqual(write_response.status_code, 302)
        with self.app.app_context(), self.app_module.tenant_context(tenant_id):
            self.assertIsNone(self.app_module.User.query.filter_by(username="blocked_user").first())
