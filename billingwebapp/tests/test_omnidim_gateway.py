import importlib
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


class OmnidimGatewayTests(unittest.TestCase):
    ENV_KEYS = (
        "DATABASE_URL",
        "SECRET_KEY",
        "APP_TIMEZONE",
        "APP_STORAGE_ROOT",
        "APP_PRIVATE_STORAGE_ROOT",
        "APP_BACKUP_ROOT",
        "ENABLE_BACKGROUND_JOBS",
        "APPLICATION_BASE_URL",
        "AI_CALLBACKS_ENABLED",
        "AI_COMPLAINTS_ENABLED",
        "OMNIDIM_GATEWAY_ENABLED",
        "OMNIDIM_GATEWAY_SECRET",
        "OMNIDIM_GATEWAY_RATE_LIMIT_PER_MINUTE",
    )

    @classmethod
    def setUpClass(cls):
        cls.original_env = {key: os.environ.get(key) for key in cls.ENV_KEYS}
        cls.temp_dir = tempfile.TemporaryDirectory()
        os.environ.update(
            {
                "DATABASE_URL": "sqlite:///{}".format(
                    os.path.join(cls.temp_dir.name, "omnidim_gateway.db")
                ),
                "SECRET_KEY": "omnidim-gateway-test-session-secret",
                "APP_TIMEZONE": "Asia/Kolkata",
                "APP_STORAGE_ROOT": os.path.join(cls.temp_dir.name, "uploads"),
                "APP_PRIVATE_STORAGE_ROOT": os.path.join(cls.temp_dir.name, "private"),
                "APP_BACKUP_ROOT": os.path.join(cls.temp_dir.name, "backups"),
                "ENABLE_BACKGROUND_JOBS": "0",
                "APPLICATION_BASE_URL": "https://clinic.example.test",
                "AI_CALLBACKS_ENABLED": "1",
                "AI_COMPLAINTS_ENABLED": "1",
                "OMNIDIM_GATEWAY_ENABLED": "1",
                "OMNIDIM_GATEWAY_SECRET": "omnidim-gateway-test-secret-with-more-than-thirty-two-bytes",
                "OMNIDIM_GATEWAY_RATE_LIMIT_PER_MINUTE": "120",
            }
        )
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_dir not in sys.path:
            sys.path.insert(0, project_dir)
        if "app" in sys.modules:
            cls.app_module = importlib.reload(sys.modules["app"])
        else:
            cls.app_module = importlib.import_module("app")
        cls.app = cls.app_module.app
        cls.db = cls.app_module.db
        cls.app.config.update(
            TESTING=True,
            AI_CALLBACKS_ENABLED=True,
            AI_COMPLAINTS_ENABLED=True,
            OMNIDIM_GATEWAY_ENABLED=True,
            OMNIDIM_GATEWAY_SECRET=os.environ["OMNIDIM_GATEWAY_SECRET"],
            APPLICATION_BASE_URL="https://clinic.example.test",
        )
        from models import (
            AIAPIRequestAudit,
            CallbackRequest,
            ClinicLocation,
            ClinicProfile,
            ClinicScheduleRule,
            Clinician,
            Complaint,
            OmnidimGatewayAction,
        )

        cls.models = {
            name: value
            for name, value in locals().items()
            if name
            in {
                "AIAPIRequestAudit",
                "CallbackRequest",
                "ClinicLocation",
                "ClinicProfile",
                "ClinicScheduleRule",
                "Clinician",
                "Complaint",
                "OmnidimGatewayAction",
            }
        }

    @classmethod
    def tearDownClass(cls):
        for key, value in cls.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        cls.temp_dir.cleanup()

    def setUp(self):
        M = self.models
        self.target_date = datetime.now(ZoneInfo("Asia/Kolkata")).date() + timedelta(days=1)
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.create_all()
            self.db.session.add(
                M["ClinicProfile"](
                    clinic_name="Test Endo Clinic",
                    phone="01123456789",
                    reception_phone="01123456780",
                    timezone_name="Asia/Kolkata",
                    available_services_json='["Consultation", "Lab"]',
                )
            )
            location = M["ClinicLocation"](
                code="MAIN",
                display_name="Main Clinic",
                public_address="1 Test Road",
                public_phone="01123456789",
                location_type="CLINIC",
                is_active=True,
                is_default=True,
            )
            self.db.session.add(location)
            self.db.session.flush()
            doctor = M["Clinician"](
                code="DRTEST",
                display_name="Dr. Test",
                public_title="Dr.",
                specialty="Endocrinology",
                default_location_id=location.id,
                is_active=True,
            )
            self.db.session.add(doctor)
            self.db.session.flush()
            self.db.session.add(
                M["ClinicScheduleRule"](
                    location_id=location.id,
                    clinician_id=doctor.id,
                    weekday=self.target_date.weekday(),
                    booking_enabled=True,
                    arrival_window_start="10:00",
                    arrival_window_end="11:00",
                    normal_daily_limit=6,
                    priority_daily_limit=0,
                    slot_duration_minutes=20,
                    max_patients_per_slot=1,
                    individual_time_slots=True,
                    is_active=True,
                )
            )
            self.db.session.commit()
            self.doctor_id = doctor.id
            self.location_id = location.id
        self.client = self.app.test_client()
        self.headers = {
            "X-Clinic-Gateway-Key": self.app.config["OMNIDIM_GATEWAY_SECRET"],
        }

    def tearDown(self):
        self.app.config["OMNIDIM_GATEWAY_ENABLED"] = True
        with self.app.app_context():
            self.db.session.remove()

    def test_gateway_is_disabled_and_authenticated_independently_of_raw_ai_api(self):
        self.app.config["OMNIDIM_GATEWAY_ENABLED"] = False
        disabled = self.client.get("/api/v1/omnidim/clinic-info", headers=self.headers)
        self.assertEqual(disabled.status_code, 503, disabled.get_json())
        self.assertEqual(disabled.get_json()["error"]["code"], "SERVICE_UNAVAILABLE")

        self.app.config["OMNIDIM_GATEWAY_ENABLED"] = True
        denied = self.client.get("/api/v1/omnidim/clinic-info")
        self.assertEqual(denied.status_code, 401, denied.get_json())
        self.assertEqual(denied.get_json()["error"]["code"], "UNAUTHORIZED")

    def test_public_clinic_timing_slots_and_secure_portal_handoffs(self):
        clinic = self.client.get("/api/v1/omnidim/clinic-info", headers=self.headers)
        self.assertEqual(clinic.status_code, 200, clinic.get_json())
        self.assertEqual(clinic.get_json()["data"]["clinic"]["name"], "Test Endo Clinic")

        doctors = self.client.get("/api/v1/omnidim/doctors", headers=self.headers)
        self.assertEqual(doctors.status_code, 200, doctors.get_json())
        self.assertEqual(doctors.get_json()["data"]["count"], 1)

        timing = self.client.get(
            "/api/v1/omnidim/timings?doctor_name=Dr.%20Test&date={}".format(
                self.target_date.isoformat()
            ),
            headers=self.headers,
        )
        self.assertEqual(timing.status_code, 200, timing.get_json())
        self.assertTrue(timing.get_json()["data"]["timing"]["bookable"])

        slots = self.client.get(
            "/api/v1/omnidim/appointment-slots?doctor_name=DRTEST&date={}".format(
                self.target_date.isoformat()
            ),
            headers=self.headers,
        )
        self.assertEqual(slots.status_code, 200, slots.get_json())
        self.assertGreater(slots.get_json()["data"]["available_slot_count"], 0)

        booking = self.client.get(
            "/api/v1/omnidim/appointment-booking-link", headers=self.headers
        )
        reports = self.client.get("/api/v1/omnidim/lab-report-access", headers=self.headers)
        self.assertEqual(booking.status_code, 200, booking.get_json())
        self.assertEqual(reports.status_code, 200, reports.get_json())
        self.assertTrue(booking.get_json()["data"]["verification_required"])
        self.assertTrue(reports.get_json()["data"]["verification_required"])
        self.assertEqual(booking.get_json()["data"]["url"], "https://clinic.example.test/book-appointment")
        self.assertEqual(reports.get_json()["data"]["url"], "https://clinic.example.test/my-lab-reports")

    def test_callback_and_complaint_are_confirmed_and_idempotent(self):
        callback_payload = {
            "request_key": "call-001-callback-001",
            "confirmed": True,
            "mobile": "9876543210",
            "caller_name": "Ravi Kumar",
            "reason": "Please call me about clinic timings",
            "category": "GENERAL",
            "call_id": "call-001",
        }
        created = self.client.post(
            "/api/v1/omnidim/callbacks", json=callback_payload, headers=self.headers
        )
        replay = self.client.post(
            "/api/v1/omnidim/callbacks", json=callback_payload, headers=self.headers
        )
        self.assertEqual(created.status_code, 201, created.get_json())
        self.assertEqual(replay.status_code, 201, replay.get_json())
        self.assertFalse(created.get_json()["data"]["replayed"])
        self.assertTrue(replay.get_json()["data"]["replayed"])
        self.assertEqual(
            created.get_json()["data"]["callback_ref"], replay.get_json()["data"]["callback_ref"]
        )
        with self.app.app_context():
            self.assertEqual(self.models["CallbackRequest"].query.count(), 1)
            self.assertEqual(self.models["OmnidimGatewayAction"].query.count(), 1)

        conflict_payload = dict(callback_payload)
        conflict_payload["reason"] = "Different request"
        conflict = self.client.post(
            "/api/v1/omnidim/callbacks", json=conflict_payload, headers=self.headers
        )
        self.assertEqual(conflict.status_code, 409, conflict.get_json())
        self.assertEqual(conflict.get_json()["error"]["code"], "IDEMPOTENCY_CONFLICT")

        complaint_payload = {
            "request_key": "call-001-complaint-001",
            "confirmed": True,
            "mobile": "9876543210",
            "caller_name": "Ravi Kumar",
            "category": "SERVICE",
            "summary": "Reception callback was delayed",
            "details": "Caller asked staff to review the delay.",
        }
        complaint = self.client.post(
            "/api/v1/omnidim/complaints", json=complaint_payload, headers=self.headers
        )
        self.assertEqual(complaint.status_code, 201, complaint.get_json())
        with self.app.app_context():
            self.assertEqual(self.models["Complaint"].query.count(), 1)

    def test_appointment_request_creates_staff_follow_up_not_an_unverified_booking(self):
        payload = {
            "request_key": "call-002-appointment-001",
            "confirmed": True,
            "mobile": "9876543210",
            "caller_name": "Ravi Kumar",
            "doctor_name": "Dr. Test",
            "preferred_date": self.target_date.isoformat(),
            "preferred_time": "10:20",
            "reason": "Follow-up consultation",
        }
        response = self.client.post(
            "/api/v1/omnidim/appointment-requests", json=payload, headers=self.headers
        )
        self.assertEqual(response.status_code, 201, response.get_json())
        data = response.get_json()["data"]
        self.assertTrue(data["staff_confirmation_required"])
        with self.app.app_context():
            row = self.models["CallbackRequest"].query.one()
            self.assertEqual(row.category, "APPOINTMENT")
            self.assertIn("Dr. Test", row.reason)


if __name__ == "__main__":
    unittest.main()
