import importlib
import os
import sys
import tempfile
import unittest
from datetime import date, time


class AIIntegrationAPITests(unittest.TestCase):
    ENV_KEYS = (
        "DATABASE_URL",
        "SECRET_KEY",
        "APP_TIMEZONE",
        "APP_STORAGE_ROOT",
        "APP_BACKUP_ROOT",
        "ENABLE_BACKGROUND_JOBS",
        "AI_API_ENABLED",
        "AI_ACCESS_TOKEN_TTL_SECONDS",
        "AI_AUTH_RATE_LIMIT_PER_MINUTE",
        "AI_AUDIT_FINGERPRINT_SECRET",
    )

    @classmethod
    def setUpClass(cls):
        cls.original_env = {key: os.environ.get(key) for key in cls.ENV_KEYS}
        cls.temp_dir = tempfile.TemporaryDirectory()
        os.environ["DATABASE_URL"] = "sqlite:///{}".format(
            os.path.join(cls.temp_dir.name, "ai_api.db")
        )
        os.environ["SECRET_KEY"] = "ai-api-test-session-secret"
        os.environ["CSRF_PROTECTION"] = "0"
        os.environ["APP_TIMEZONE"] = "Asia/Kolkata"
        os.environ["APP_STORAGE_ROOT"] = os.path.join(cls.temp_dir.name, "uploads")
        os.environ["APP_BACKUP_ROOT"] = os.path.join(cls.temp_dir.name, "backups")
        os.environ["ENABLE_BACKGROUND_JOBS"] = "0"
        os.environ["AI_API_ENABLED"] = "1"
        os.environ["AI_ACCESS_TOKEN_TTL_SECONDS"] = "300"
        os.environ["AI_AUTH_RATE_LIMIT_PER_MINUTE"] = "20"
        os.environ["AI_AUDIT_FINGERPRINT_SECRET"] = "ai-api-test-audit-secret"

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
        cls.app.config["AI_API_ENABLED"] = True

        from services.ai_auth_service import create_api_client, rotate_api_client_secret

        cls.create_api_client = staticmethod(create_api_client)
        cls.rotate_api_client_secret = staticmethod(rotate_api_client_secret)

    @classmethod
    def tearDownClass(cls):
        for key, value in cls.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        cls.temp_dir.cleanup()

    def setUp(self):
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.create_all()
            admin = self.app_module.User(
                username="admin",
                role="admin",
                is_active=True,
                access_profile="admin",
                can_manage_users=True,
            )
            admin.set_password("Admin@123")
            self.db.session.add(admin)
            self.api_client, self.raw_secret = self.create_api_client(
                self.db.session,
                name="Test Call Assistant",
                scopes={"clinic:read", "doctor:read"},
                created_by="admin",
            )
            self.client_id = self.api_client.client_id
            self.api_client_pk = self.api_client.id
            self.db.session.commit()
        self.client = self.app.test_client()

    def tearDown(self):
        self.app.config["AI_API_ENABLED"] = True
        with self.app.app_context():
            self.db.session.remove()

    def _token(self, **overrides):
        payload = {
            "client_id": self.client_id,
            "client_secret": self.raw_secret,
            "scope": "clinic:read",
        }
        payload.update(overrides)
        response = self.client.post("/api/v1/ai/auth/token", json=payload)
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()["data"]["access_token"]

    def _admin_csrf(self):
        self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
        )
        self.client.get("/admin/ai-integration")
        with self.client.session_transaction() as browser_session:
            return browser_session["_ai_admin_csrf"]

    def test_api_is_feature_flagged_off_safely(self):
        self.app.config["AI_API_ENABLED"] = False
        response = self.client.post("/api/v1/ai/auth/token", json={})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"]["code"], "SERVICE_UNAVAILABLE")
        self.assertTrue(response.headers.get("X-Request-ID"))

    def test_missing_bearer_token_returns_standard_unauthorized_response(self):
        response = self.client.get("/api/v1/ai/integration/status")
        payload = response.get_json()
        self.assertEqual(response.status_code, 401)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error"]["code"], "UNAUTHORIZED")
        self.assertEqual(payload["request_id"], response.headers["X-Request-ID"])
        self.assertNotIn("Access-Control-Allow-Origin", response.headers)

    def test_invalid_client_id_and_invalid_secret_use_same_safe_error(self):
        unknown = self.client.post(
            "/api/v1/ai/auth/token",
            json={"client_id": "ai_unknown", "client_secret": "wrong-secret"},
        )
        wrong_secret = self.client.post(
            "/api/v1/ai/auth/token",
            json={"client_id": self.client_id, "client_secret": "wrong-secret"},
        )
        self.assertEqual(unknown.status_code, 401)
        self.assertEqual(wrong_secret.status_code, 401)
        self.assertEqual(unknown.get_json()["error"], wrong_secret.get_json()["error"])

    def test_token_is_opaque_hashed_and_allows_scoped_request(self):
        raw_token = self._token()
        response = self.client.get(
            "/api/v1/ai/integration/status",
            headers={
                "Authorization": "Bearer {}".format(raw_token),
                "X-Request-ID": "req-foundation-001",
                "X-Call-ID": "call-001",
                "X-Session-ID": "session-001",
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()["data"]
        self.assertEqual(data["client"]["client_id"], self.client_id)
        self.assertEqual(data["correlation"]["call_id"], "call-001")
        with self.app.app_context():
            stored = self.app_module.AIAccessToken.query.one()
            self.assertNotEqual(stored.token_hash, raw_token)
            self.assertNotIn(raw_token, stored.token_hash)
            self.assertEqual(len(stored.token_hash), 64)
            audit = self.app_module.AIAPIRequestAudit.query.filter_by(
                request_id="req-foundation-001"
            ).one()
            self.assertEqual(audit.call_id, "call-001")
            self.assertEqual(audit.session_id, "session-001")
            self.assertEqual(audit.api_client_id, self.api_client_pk)
            self.assertEqual(len(audit.client_ip_fingerprint), 64)

    def test_requesting_ungranted_scope_is_forbidden(self):
        response = self.client.post(
            "/api/v1/ai/auth/token",
            json={
                "client_id": self.client_id,
                "client_secret": self.raw_secret,
                "scope": "appointment:create",
            },
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"]["code"], "FORBIDDEN")

    def test_disabled_client_cannot_authenticate(self):
        with self.app.app_context():
            client = self.db.session.get(self.app_module.AIAPIClient, self.api_client_pk)
            client.is_active = False
            self.db.session.commit()
        response = self.client.post(
            "/api/v1/ai/auth/token",
            json={"client_id": self.client_id, "client_secret": self.raw_secret},
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"]["code"], "UNAUTHORIZED")

    def test_ip_allowlist_is_enforced_for_token_and_bearer_requests(self):
        with self.app.app_context():
            client = self.db.session.get(self.app_module.AIAPIClient, self.api_client_pk)
            client.allowed_ips_json = '["203.0.113.10/32"]'
            self.db.session.commit()
        denied = self.client.post(
            "/api/v1/ai/auth/token",
            json={"client_id": self.client_id, "client_secret": self.raw_secret},
            environ_base={"REMOTE_ADDR": "198.51.100.20"},
        )
        self.assertEqual(denied.status_code, 401)
        allowed = self.client.post(
            "/api/v1/ai/auth/token",
            json={"client_id": self.client_id, "client_secret": self.raw_secret},
            environ_base={"REMOTE_ADDR": "203.0.113.10"},
        )
        self.assertEqual(allowed.status_code, 200)

    def test_secret_rotation_revokes_existing_tokens(self):
        raw_token = self._token()
        with self.app.app_context():
            client = self.db.session.get(self.app_module.AIAPIClient, self.api_client_pk)
            replacement_secret = self.rotate_api_client_secret(
                self.db.session,
                client,
                updated_by="admin",
            )
            self.db.session.commit()
            self.assertNotEqual(replacement_secret, self.raw_secret)
        response = self.client.get(
            "/api/v1/ai/integration/status",
            headers={"Authorization": "Bearer {}".format(raw_token)},
        )
        self.assertEqual(response.status_code, 401)

    def test_admin_page_requires_login_and_never_displays_secret_hash(self):
        redirected = self.client.get("/admin/ai-integration")
        self.assertEqual(redirected.status_code, 302)
        self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
        )
        response = self.client.get("/admin/ai-integration")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"AI Integration", response.data)
        self.assertIn(self.client_id.encode("utf-8"), response.data)
        with self.app.app_context():
            secret_hash = self.db.session.get(
                self.app_module.AIAPIClient, self.api_client_pk
            ).secret_hash
        self.assertNotIn(secret_hash.encode("utf-8"), response.data)
        self.assertNotIn(self.raw_secret.encode("utf-8"), response.data)

    def test_admin_can_save_clinic_profile_with_legacy_timezone_column(self):
        self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
        )
        self.client.get("/admin/ai-integration")
        with self.client.session_transaction() as browser_session:
            csrf_token = browser_session["_ai_admin_csrf"]
        response = self.client.post(
            "/admin/ai-integration/clinic-profile",
            data={
                "csrf_token": csrf_token,
                "clinic_name": "Test Clinic",
                "timezone_name": "Asia/Kolkata",
                "late_grace_minutes": "15",
                "auto_reschedule_after_minutes": "10",
                "late_staff_notification_required": "1",
            },
        )
        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            from models import ClinicProfile

            profile = ClinicProfile.query.one()
            self.assertEqual(profile.timezone_name, "Asia/Kolkata")
            self.assertEqual(profile.legacy_timezone, "Asia/Kolkata")

    def test_admin_shows_and_edits_each_location_as_an_ajax_card(self):
        csrf_token = self._admin_csrf()
        with self.app.app_context():
            from models import ClinicLocation

            clinic = ClinicLocation(
                code="MAIN", display_name="The Endo Clinic", location_type="CLINIC",
                is_active=True, is_default=True,
            )
            hospital = ClinicLocation(
                code="HOSP", display_name="Mangalam Hospital", location_type="HOSPITAL",
                is_active=True, is_default=False,
            )
            self.db.session.add_all([clinic, hospital])
            self.db.session.commit()
            clinic_id = clinic.id
            hospital_id = hospital.id

        page = self.client.get("/admin/ai-integration")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"The Endo Clinic", page.data)
        self.assertIn(b"Mangalam Hospital", page.data)
        self.assertIn(b"Appointments allowed", page.data)
        self.assertIn(b"Timing/info only", page.data)
        self.assertIn(
            "/admin/ai-integration/locations/{}/edit".format(hospital_id).encode(),
            page.data,
        )

        edited = self.client.post(
            "/admin/ai-integration/locations/{}/edit".format(hospital_id),
            data={
                "csrf_token": csrf_token,
                "code": "HOSP",
                "display_name": "Mangalam Plus Medicity",
                "location_type": "HOSPITAL",
                "public_phone": "0141000000",
                "city": "Jaipur",
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(edited.status_code, 200)
        self.assertTrue(edited.get_json()["ok"])
        with self.app.app_context():
            from models import ClinicLocation

            hospital = self.db.session.get(ClinicLocation, hospital_id)
            clinic = self.db.session.get(ClinicLocation, clinic_id)
            self.assertEqual(hospital.display_name, "Mangalam Plus Medicity")
            self.assertEqual(hospital.city, "Jaipur")
            self.assertFalse(hospital.is_default)
            self.assertTrue(clinic.is_default)

    def test_hospital_cannot_be_made_default_booking_location(self):
        csrf_token = self._admin_csrf()
        response = self.client.post(
            "/admin/ai-integration/locations",
            data={
                "csrf_token": csrf_token,
                "code": "HOSP",
                "display_name": "Information Hospital",
                "location_type": "HOSPITAL",
                "is_default": "on",
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["ok"])
        with self.app.app_context():
            from models import ClinicLocation

            self.assertEqual(ClinicLocation.query.count(), 0)

    def test_admin_can_edit_schedule_rule_with_ajax_without_redirect(self):
        csrf_token = self._admin_csrf()
        with self.app.app_context():
            from models import ClinicLocation, Clinician, ClinicScheduleRule

            location = ClinicLocation(code="MAIN", display_name="Main Clinic")
            doctor = Clinician(code="DR1", display_name="Dr Test")
            self.db.session.add_all([location, doctor])
            self.db.session.flush()
            rule = ClinicScheduleRule(
                location_id=location.id,
                clinician_id=doctor.id,
                weekday=0,
                booking_enabled=True,
                arrival_window_start="17:30",
                arrival_window_end="19:00",
                normal_daily_limit=15,
                priority_daily_limit=2,
                slot_duration_minutes=20,
                max_patients_per_slot=1,
                is_active=True,
            )
            self.db.session.add(rule)
            self.db.session.commit()
            rule_id = rule.id
            location_id = location.id
            doctor_id = doctor.id

        page = self.client.get("/admin/ai-integration")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"Doctor availability", page.data)
        self.assertIn(b"05:30 PM", page.data)
        self.assertIn(b"07:00 PM", page.data)
        self.assertIn(b"Appointment window", page.data)
        self.assertNotIn(b"<label>Slot minutes</label>", page.data)
        self.assertNotIn(b"<label>Patients per slot</label>", page.data)
        self.assertIn(b"Save changes", page.data)
        self.assertIn(
            "/admin/ai-integration/schedule-rules/{}/edit".format(rule_id).encode("utf-8"),
            page.data,
        )

        response = self.client.post(
            "/admin/ai-integration/schedule-rules/{}/edit".format(rule_id),
            data={
                "csrf_token": csrf_token,
                "location_id": str(location_id),
                "clinician_id": str(doctor_id),
                "weekday": "0",
                "arrival_window_start": "17:45",
                "arrival_window_end": "19:45",
                "normal_daily_limit": "18",
                "priority_daily_limit": "3",
                "slot_duration_minutes": "15",
                "max_patients_per_slot": "2",
                "booking_enabled": "on",
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.is_json)
        self.assertTrue(response.get_json()["ok"])
        self.assertNotEqual(response.status_code, 302)

        with self.app.app_context():
            from models import ClinicScheduleRule

            updated = self.db.session.get(ClinicScheduleRule, rule_id)
            self.assertEqual(updated.arrival_window_start, "17:45")
            self.assertEqual(updated.arrival_window_end, "19:45")
            self.assertEqual(updated.slot_duration_minutes, 15)
            self.assertEqual(updated.max_patients_per_slot, 2)
            self.assertTrue(updated.is_active)

    def test_admin_can_delete_schedule_rule_without_deleting_existing_appointments(self):
        csrf_token = self._admin_csrf()
        with self.app.app_context():
            from models import (
                Appointment,
                AuditLog,
                ClinicLocation,
                Clinician,
                ClinicScheduleRule,
            )

            location = ClinicLocation(code="MAIN", display_name="Main Clinic")
            doctor = Clinician(code="DR1", display_name="Dr Test")
            self.db.session.add_all([location, doctor])
            self.db.session.flush()
            rule = ClinicScheduleRule(
                location_id=location.id,
                clinician_id=doctor.id,
                weekday=0,
                booking_enabled=True,
                arrival_window_start="17:30",
                arrival_window_end="19:00",
                normal_daily_limit=15,
                priority_daily_limit=2,
                slot_duration_minutes=20,
                max_patients_per_slot=1,
                is_active=True,
            )
            appointment = Appointment(
                patient_name="Existing Patient",
                doctor_name=doctor.display_name,
                clinician_id=doctor.id,
                location_id=location.id,
                appointment_date=date(2026, 9, 7),
                appointment_time=time(17, 30),
            )
            self.db.session.add_all([rule, appointment])
            self.db.session.commit()
            rule_id = rule.id
            appointment_id = appointment.id

        page = self.client.get("/admin/ai-integration")
        self.assertNotIn(
            "/admin/ai-integration/schedule-rules/{}/delete".format(rule_id).encode(),
            page.data,
        )

        active_delete = self.client.post(
            "/admin/ai-integration/schedule-rules/{}/delete".format(rule_id),
            data={"csrf_token": csrf_token},
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(active_delete.status_code, 200)
        self.assertFalse(active_delete.get_json()["ok"])
        with self.app.app_context():
            self.assertIsNotNone(self.db.session.get(ClinicScheduleRule, rule_id))

        disabled = self.client.post(
            "/admin/ai-integration/settings/schedule-rule/{}/toggle".format(rule_id),
            data={"csrf_token": csrf_token},
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(disabled.status_code, 200)
        self.assertTrue(disabled.get_json()["ok"])

        page = self.client.get("/admin/ai-integration")
        self.assertIn(b"Delete", page.data)
        self.assertIn(
            "/admin/ai-integration/schedule-rules/{}/delete".format(rule_id).encode(),
            page.data,
        )

        response = self.client.post(
            "/admin/ai-integration/schedule-rules/{}/delete".format(rule_id),
            data={"csrf_token": csrf_token},
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

        with self.app.app_context():
            self.assertIsNone(self.db.session.get(ClinicScheduleRule, rule_id))
            self.assertIsNotNone(self.db.session.get(Appointment, appointment_id))
            audit = AuditLog.query.filter_by(
                action="AI_SCHEDULE_RULE_DELETED", entity_id=rule_id
            ).one()
            self.assertEqual(audit.entity_type, "SCHEDULE_RULE")

    def test_schedule_edit_validation_returns_ajax_error_and_keeps_values(self):
        csrf_token = self._admin_csrf()
        with self.app.app_context():
            from models import ClinicLocation, Clinician, ClinicScheduleRule

            location = ClinicLocation(code="MAIN", display_name="Main Clinic")
            doctor = Clinician(code="DR1", display_name="Dr Test")
            self.db.session.add_all([location, doctor])
            self.db.session.flush()
            rule = ClinicScheduleRule(
                location_id=location.id,
                clinician_id=doctor.id,
                weekday=1,
                booking_enabled=True,
                arrival_window_start="17:30",
                arrival_window_end="19:30",
                slot_duration_minutes=20,
                max_patients_per_slot=1,
                is_active=True,
            )
            self.db.session.add(rule)
            self.db.session.commit()
            rule_id = rule.id
            location_id = location.id
            doctor_id = doctor.id

        response = self.client.post(
            "/admin/ai-integration/schedule-rules/{}/edit".format(rule_id),
            data={
                "csrf_token": csrf_token,
                "location_id": str(location_id),
                "clinician_id": str(doctor_id),
                "weekday": "1",
                "arrival_window_start": "20:00",
                "arrival_window_end": "19:00",
                "slot_duration_minutes": "20",
                "max_patients_per_slot": "1",
                "booking_enabled": "on",
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["ok"])
        with self.app.app_context():
            from models import ClinicScheduleRule

            unchanged = self.db.session.get(ClinicScheduleRule, rule_id)
            self.assertEqual(unchanged.arrival_window_start, "17:30")
            self.assertEqual(unchanged.arrival_window_end, "19:30")


if __name__ == "__main__":
    unittest.main()
