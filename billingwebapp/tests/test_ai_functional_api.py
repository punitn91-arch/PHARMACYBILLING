import importlib
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, time
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


class AIFunctionalAPITests(unittest.TestCase):
    ENV_KEYS = (
        "DATABASE_URL", "SECRET_KEY", "APP_TIMEZONE", "APP_STORAGE_ROOT",
        "APP_PRIVATE_STORAGE_ROOT", "APP_BACKUP_ROOT", "ENABLE_BACKGROUND_JOBS",
        "AI_API_ENABLED", "AI_OTP_ENABLED", "AI_SECURE_REPORT_DELIVERY_ENABLED",
        "AI_CALLBACKS_ENABLED", "AI_COMPLAINTS_ENABLED", "AI_NOTIFICATIONS_ENABLED",
        "AI_NOTIFICATION_MODE", "PUBLIC_PORTAL_OTP_MODE", "APPLICATION_BASE_URL",
    )

    @classmethod
    def setUpClass(cls):
        cls.original_env = {key: os.environ.get(key) for key in cls.ENV_KEYS}
        cls.temp_dir = tempfile.TemporaryDirectory()
        os.environ.update({
            "DATABASE_URL": "sqlite:///{}".format(os.path.join(cls.temp_dir.name, "functional.db")),
            "SECRET_KEY": "functional-api-secret",
            "CSRF_PROTECTION": "0",
            "APP_TIMEZONE": "Asia/Kolkata",
            "APP_STORAGE_ROOT": os.path.join(cls.temp_dir.name, "uploads"),
            "APP_PRIVATE_STORAGE_ROOT": os.path.join(cls.temp_dir.name, "private"),
            "APP_BACKUP_ROOT": os.path.join(cls.temp_dir.name, "backups"),
            "ENABLE_BACKGROUND_JOBS": "0",
            "AI_API_ENABLED": "1",
            "AI_OTP_ENABLED": "1",
            "AI_SECURE_REPORT_DELIVERY_ENABLED": "1",
            "AI_CALLBACKS_ENABLED": "1",
            "AI_COMPLAINTS_ENABLED": "1",
            "AI_NOTIFICATIONS_ENABLED": "1",
            "AI_NOTIFICATION_MODE": "test",
            "PUBLIC_PORTAL_OTP_MODE": "development",
            "APPLICATION_BASE_URL": "http://localhost",
        })
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
            AI_API_ENABLED=True,
            AI_OTP_ENABLED=True,
            AI_SECURE_REPORT_DELIVERY_ENABLED=True,
            AI_CALLBACKS_ENABLED=True,
            AI_COMPLAINTS_ENABLED=True,
            AI_NOTIFICATIONS_ENABLED=True,
        )
        from models import (
            AIAPIClient, Appointment, CallbackRequest, ClinicKnowledgeEntry, ClinicLocation,
            ClinicProfile, ClinicScheduleRule, Clinician, Complaint, LabOrder, LabReport,
            LabTest, NotificationDelivery, NotificationTemplate, Patient, ReceptionSchedule,
            SecureDocumentToken,
        )
        from services.ai_auth_service import ALLOWED_AI_SCOPES, create_api_client
        cls.models = {
            name: value for name, value in locals().items()
            if name in {
                "AIAPIClient", "Appointment", "CallbackRequest", "ClinicKnowledgeEntry",
                "ClinicLocation", "ClinicProfile", "ClinicScheduleRule", "Clinician",
                "Complaint", "LabOrder", "LabReport", "LabTest", "NotificationDelivery",
                "NotificationTemplate", "Patient", "ReceptionSchedule", "SecureDocumentToken",
            }
        }
        cls.allowed_scopes = ALLOWED_AI_SCOPES
        cls.create_api_client = staticmethod(create_api_client)

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
            self.db.session.add(M["ClinicProfile"](
                clinic_name="Test Endo Clinic", phone="01123456789",
                reception_phone="01123456780", timezone_name="Asia/Kolkata",
                payment_methods_json='["Cash","UPI"]', available_services_json='["Consultation","Lab"]',
            ))
            location = M["ClinicLocation"](
                code="MAIN", display_name="Main Clinic", public_address="1 Test Road",
                public_phone="01123456789", maps_url="https://maps.example/main",
                location_type="CLINIC", is_active=True, is_default=True,
            )
            self.db.session.add(location)
            self.db.session.flush()
            doctor = M["Clinician"](
                code="DRTEST", display_name="Dr. Test", public_title="Dr.",
                specialty="Endocrinology", qualification="MD", consultation_fee=600,
                default_location_id=location.id, is_active=True,
            )
            self.db.session.add(doctor)
            self.db.session.flush()
            for clinician_id in (None, doctor.id):
                self.db.session.add(M["ClinicScheduleRule"](
                    location_id=location.id, clinician_id=clinician_id,
                    weekday=self.target_date.weekday(), booking_enabled=True,
                    arrival_window_start="10:00", arrival_window_end="11:00",
                    normal_daily_limit=10, priority_daily_limit=0,
                    slot_duration_minutes=20, max_patients_per_slot=1, is_active=True,
                ))
            patient = M["Patient"](name="Ravi Kumar", mobile="9876543210", age=35, gender="MALE")
            self.db.session.add(patient)
            self.db.session.flush()
            order = M["LabOrder"](
                order_no="LAB-AI-001", patient_id=patient.id, patient_name=patient.name,
                mobile=patient.mobile, status="COMPLETED",
            )
            self.db.session.add(order)
            self.db.session.flush()
            report = M["LabReport"](
                lab_order_id=order.id, patient_id=patient.id, patient_name=patient.name,
                mobile=patient.mobile, title="CBC Final", report_date=self.target_date,
                storage_key="ai-test-report.pdf", original_filename="cbc-final.pdf",
                mime_type="application/pdf", status="PUBLISHED",
            )
            self.db.session.add(report)
            self.db.session.add(M["ClinicKnowledgeEntry"](
                category="APPOINTMENTS", question="How to book?",
                answer="Appointments require an available clinic slot.", language="en",
            ))
            self.db.session.add(M["ReceptionSchedule"](
                location_id=location.id, weekday=self.target_date.weekday(), is_open=True,
                open_time=time(9, 0), close_time=time(18, 0), is_active=True,
            ))
            self.db.session.add(M["NotificationTemplate"](
                event_code="APPOINTMENT_CONFIRMATION", channel="SMS", language="en",
                body_template="Appointment {appointment_no} is confirmed for {patient_name}.",
                is_active=True,
            ))
            self.db.session.add(M["LabTest"](
                test_code="CBC001", name="Complete Blood Count", category="Hematology",
                specimen_type="Blood", preparation="No special preparation",
                default_price=350, aliases_json='["CBC"]', fasting_required=False,
                turnaround_text="Same day", is_active=True,
            ))
            self.db.session.add(M["LabTest"](
                test_code="GLU001", name="Fasting Blood Sugar", category="Biochemistry",
                specimen_type="Blood", preparation="8 hours fasting required",
                default_price=120, aliases_json='["FBS","sugar test"]',
                fasting_required=True, turnaround_text="Same day", is_active=True,
            ))
            api_client, raw_secret = self.create_api_client(
                self.db.session, name="Functional Assistant", scopes=self.allowed_scopes,
            )
            self.db.session.commit()
            self.location_id = location.id
            self.doctor_id = doctor.id
            self.patient_id = patient.id
            self.report_id = report.id
            self.client_id = api_client.client_id
            self.raw_secret = raw_secret

            report_dir = os.path.join(self.app.config["APP_PRIVATE_STORAGE_ROOT"], "lab_reports")
            os.makedirs(report_dir, exist_ok=True)
            with open(os.path.join(report_dir, "ai-test-report.pdf"), "wb") as handle:
                handle.write(b"%PDF-1.4\n% test report\n")

        self.client = self.app.test_client()
        token_response = self.client.post("/api/v1/ai/auth/token", json={
            "client_id": self.client_id, "client_secret": self.raw_secret,
        })
        self.assertEqual(token_response.status_code, 200, token_response.get_json())
        self.access_token = token_response.get_json()["data"]["access_token"]
        self.auth = {"Authorization": "Bearer {}".format(self.access_token)}

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()

    def _patient_session(self, mobile="9876543210"):
        identify = self.client.post("/api/v1/ai/patients/identify", json={"mobile": mobile}, headers=self.auth)
        self.assertEqual(identify.status_code, 202, identify.get_json())
        reference = identify.get_json()["data"]["identification_ref"]
        sent = self.client.post("/api/v1/ai/verification/otp/send", json={
            "identification_ref": reference,
        }, headers=self.auth)
        self.assertEqual(sent.status_code, 202, sent.get_json())
        otp = sent.get_json()["data"].get("development_otp")
        self.assertRegex(otp or "", r"^\d{6}$")
        verified = self.client.post("/api/v1/ai/verification/otp/verify", json={
            "identification_ref": reference, "otp": otp,
        }, headers=self.auth)
        self.assertEqual(verified.status_code, 200, verified.get_json())
        return verified.get_json()["data"]["patient_session_token"]

    def test_clinic_doctor_schedule_knowledge_and_slots(self):
        clinic = self.client.get("/api/v1/ai/clinic", headers=self.auth)
        self.assertEqual(clinic.status_code, 200)
        self.assertEqual(clinic.get_json()["data"]["clinic"]["name"], "Test Endo Clinic")
        doctors = self.client.get("/api/v1/ai/doctors", headers=self.auth)
        self.assertEqual(doctors.status_code, 200)
        self.assertEqual(doctors.get_json()["data"]["count"], 1)
        schedule = self.client.get(
            "/api/v1/ai/doctors/{}/schedule?location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        )
        self.assertEqual(schedule.status_code, 200, schedule.get_json())
        self.assertTrue(schedule.get_json()["data"]["bookable"])
        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        )
        self.assertEqual(slots.status_code, 200, slots.get_json())
        self.assertEqual(len(slots.get_json()["data"]["slots"]), 3)
        knowledge = self.client.get("/api/v1/ai/knowledge?limit=10", headers=self.auth)
        self.assertEqual(knowledge.status_code, 200)
        self.assertEqual(knowledge.get_json()["data"]["count"], 1)
        doctor_row = doctors.get_json()["data"]["items"][0]
        for field in ("languages", "sub_specialties", "conditions_treated", "services_offered"):
            self.assertIn(field, doctor_row)
            self.assertEqual(doctor_row[field], [])

    def test_hospital_exposes_doctor_timing_but_rejects_appointment_slots(self):
        with self.app.app_context():
            M = self.models
            hospital = M["ClinicLocation"](
                code="HOSP", display_name="Information Hospital",
                location_type="HOSPITAL", is_active=True, is_default=False,
            )
            self.db.session.add(hospital)
            self.db.session.flush()
            self.db.session.add(M["ClinicScheduleRule"](
                location_id=hospital.id, clinician_id=self.doctor_id,
                weekday=self.target_date.weekday(), booking_enabled=True,
                arrival_window_start="17:30", arrival_window_end="19:30",
                normal_daily_limit=20, priority_daily_limit=0,
                slot_duration_minutes=20, max_patients_per_slot=1,
                individual_time_slots=True, is_active=True,
            ))
            self.db.session.commit()
            hospital_id = hospital.id

        locations = self.client.get("/api/v1/ai/locations", headers=self.auth)
        hospital_payload = next(
            row for row in locations.get_json()["data"]["items"] if row["id"] == hospital_id
        )
        self.assertEqual(hospital_payload["booking_mode"], "INFORMATION_ONLY")
        self.assertFalse(hospital_payload["appointment_booking_enabled"])

        schedule = self.client.get(
            "/api/v1/ai/doctors/{}/schedule?location_id={}&date={}".format(
                self.doctor_id, hospital_id, self.target_date.isoformat()
            ), headers=self.auth,
        )
        self.assertEqual(schedule.status_code, 200, schedule.get_json())
        schedule_data = schedule.get_json()["data"]
        self.assertEqual(schedule_data["status"], "OPEN")
        self.assertEqual(schedule_data["arrival_window"], "5:30 PM to 7:30 PM")
        self.assertFalse(schedule_data["bookable"])

        availability = self.client.get(
            "/api/v1/ai/doctors/{}/availability?location_id={}&date={}".format(
                self.doctor_id, hospital_id, self.target_date.isoformat()
            ), headers=self.auth,
        )
        self.assertTrue(availability.get_json()["data"]["available"])

        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, hospital_id, self.target_date.isoformat()
            ), headers=self.auth,
        )
        self.assertEqual(slots.status_code, 409, slots.get_json())
        self.assertEqual(slots.get_json()["error"]["code"], "LOCATION_NOT_BOOKABLE")

    def test_arrival_window_mode_pools_capacity_instead_of_fixed_slots(self):
        with self.app.app_context():
            M = self.models
            self.db.session.add(M["ClinicScheduleRule"](
                location_id=self.location_id, clinician_id=self.doctor_id,
                weekday=self.target_date.weekday(), booking_enabled=True,
                arrival_window_start="14:00", arrival_window_end="15:00",
                normal_daily_limit=3, priority_daily_limit=0,
                slot_duration_minutes=20, max_patients_per_slot=1,
                individual_time_slots=False, is_active=True,
                effective_from=self.target_date,
            ))
            self.db.session.commit()
        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        )
        self.assertEqual(slots.status_code, 200, slots.get_json())
        data = slots.get_json()["data"]
        self.assertFalse(data["individual_time_slots"])
        self.assertEqual(len(data["slots"]), 1)
        self.assertEqual(data["slots"][0]["remaining_capacity"], 3)

    def test_lab_search_matches_admin_alias_and_common_abbreviation(self):
        by_admin_alias = self.client.get("/api/v1/ai/lab/tests/search?q=CBC", headers=self.auth)
        self.assertEqual(by_admin_alias.status_code, 200, by_admin_alias.get_json())
        names = {row["name"] for row in by_admin_alias.get_json()["data"]["items"]}
        self.assertIn("Complete Blood Count", names)

        by_builtin_alias = self.client.get(
            "/api/v1/ai/lab/tests/search?q=sugar%20test", headers=self.auth
        )
        self.assertEqual(by_builtin_alias.status_code, 200, by_builtin_alias.get_json())
        sugar_names = {row["name"] for row in by_builtin_alias.get_json()["data"]["items"]}
        self.assertIn("Fasting Blood Sugar", sugar_names)

    def test_lab_test_detail_exposes_fasting_price_and_turnaround(self):
        search = self.client.get("/api/v1/ai/lab/tests/search?q=sugar", headers=self.auth)
        test_id = search.get_json()["data"]["items"][0]["id"]
        detail = self.client.get("/api/v1/ai/lab/tests/{}".format(test_id), headers=self.auth)
        self.assertEqual(detail.status_code, 200, detail.get_json())
        body = detail.get_json()["data"]
        self.assertEqual(body["name"], "Fasting Blood Sugar")
        self.assertTrue(body["fasting_required"])
        self.assertEqual(body["price"], 120.0)
        self.assertEqual(body["turnaround"], "Same day")

    def test_lab_test_not_found_returns_404(self):
        missing = self.client.get("/api/v1/ai/lab/tests/999999", headers=self.auth)
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.get_json()["error"]["code"], "LAB_TEST_NOT_FOUND")

    def test_lab_categories_lists_distinct_active_categories(self):
        categories = self.client.get("/api/v1/ai/lab/categories", headers=self.auth)
        self.assertEqual(categories.status_code, 200, categories.get_json())
        self.assertEqual(
            set(categories.get_json()["data"]["items"]), {"Hematology", "Biochemistry"}
        )

    def test_lab_endpoints_require_lab_read_scope(self):
        with self.app.app_context():
            limited_client, limited_secret = self.create_api_client(
                self.db.session, name="No Lab Scope", scopes={"clinic:read"},
            )
            self.db.session.commit()
            limited_client_id = limited_client.client_id
        token_response = self.client.post("/api/v1/ai/auth/token", json={
            "client_id": limited_client_id, "client_secret": limited_secret,
        })
        self.assertEqual(token_response.status_code, 200, token_response.get_json())
        limited_auth = {
            "Authorization": "Bearer {}".format(token_response.get_json()["data"]["access_token"])
        }
        forbidden = self.client.get("/api/v1/ai/lab/tests/search?q=cbc", headers=limited_auth)
        self.assertEqual(forbidden.status_code, 403)

    def test_identification_shape_does_not_disclose_patient_existence(self):
        known = self.client.post("/api/v1/ai/patients/identify", json={"mobile": "9876543210"}, headers=self.auth)
        unknown = self.client.post("/api/v1/ai/patients/identify", json={"mobile": "9999999999"}, headers=self.auth)
        self.assertEqual(known.status_code, 202)
        self.assertEqual(unknown.status_code, 202)
        self.assertEqual(set(known.get_json()["data"]), set(unknown.get_json()["data"]))

    def test_verified_booking_is_idempotent_and_prevents_double_booking(self):
        patient_token = self._patient_session()
        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        ).get_json()["data"]["slots"]
        start_at = slots[0]["start_at"]
        headers = dict(self.auth, **{"X-Patient-Session": patient_token, "Idempotency-Key": "booking-key-001"})
        payload = {"doctor_id": self.doctor_id, "location_id": self.location_id, "start_at": start_at}
        created = self.client.post("/api/v1/ai/appointments", json=payload, headers=headers)
        self.assertEqual(created.status_code, 201, created.get_json())
        replay = self.client.post("/api/v1/ai/appointments", json=payload, headers=headers)
        self.assertEqual(replay.status_code, 201)
        other_headers = dict(headers, **{"Idempotency-Key": "booking-key-002"})
        collision = self.client.post("/api/v1/ai/appointments", json=payload, headers=other_headers)
        self.assertEqual(collision.status_code, 409)
        self.assertEqual(collision.get_json()["error"]["code"], "APPOINTMENT_UNAVAILABLE")
        with self.app.app_context():
            self.assertEqual(self.models["Appointment"].query.count(), 1)

    def test_contact_only_booking_requires_no_otp_and_is_idempotent(self):
        """New booking path (clinic policy: booking a NEW appointment needs
        only the caller's name + mobile, never OTP-verified proof of phone
        possession -- the existing verified-session path above is
        unaffected and still required for viewing/cancelling/rescheduling).
        No X-Patient-Session header, no /patients/identify or
        /verification/otp/* call anywhere in this test."""
        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        ).get_json()["data"]["slots"]
        start_at = slots[0]["start_at"]
        headers = dict(self.auth, **{"Idempotency-Key": "contact-booking-key-001"})
        payload = {
            "doctor_id": self.doctor_id,
            "location_id": self.location_id,
            "start_at": start_at,
            "patient_name": "Contact Only Patient",
            "mobile": "9812345670",
        }
        created = self.client.post("/api/v1/ai/appointments", json=payload, headers=headers)
        self.assertEqual(created.status_code, 201, created.get_json())
        with self.app.app_context():
            patient = self.models["Patient"].query.get(created.get_json()["data"]["patient_id"])
            self.assertEqual(patient.mobile, "+919812345670")
            self.assertEqual(patient.name, "Contact Only Patient")

        replay = self.client.post("/api/v1/ai/appointments", json=payload, headers=headers)
        self.assertEqual(replay.status_code, 201)

        other_headers = dict(headers, **{"Idempotency-Key": "contact-booking-key-002"})
        collision = self.client.post("/api/v1/ai/appointments", json=payload, headers=other_headers)
        self.assertEqual(collision.status_code, 409)
        self.assertEqual(collision.get_json()["error"]["code"], "APPOINTMENT_UNAVAILABLE")

        with self.app.app_context():
            self.assertEqual(self.models["Appointment"].query.count(), 1)
            self.assertEqual(self.models["Patient"].query.filter_by(mobile="+919812345670").count(), 1)

    def test_contact_only_booking_missing_name_or_mobile_falls_back_to_verified_session(self):
        """Without both patient_name and mobile, the route falls back to
        the original verified-session requirement rather than silently
        booking with incomplete contact info -- so a caller that omits them
        and has no X-Patient-Session gets the same error as before this
        change existed."""
        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        ).get_json()["data"]["slots"]
        payload = {
            "doctor_id": self.doctor_id, "location_id": self.location_id, "start_at": slots[0]["start_at"],
            "patient_name": "Only Name Given",
        }
        response = self.client.post(
            "/api/v1/ai/appointments", json=payload,
            headers=dict(self.auth, **{"Idempotency-Key": "contact-booking-key-003"}),
        )
        self.assertEqual(response.status_code, 401)

    def test_verified_reschedule_and_cancel_preserve_history(self):
        patient_token = self._patient_session()
        slots = self.client.get(
            "/api/v1/ai/appointments/slots?doctor_id={}&location_id={}&date={}".format(
                self.doctor_id, self.location_id, self.target_date.isoformat()
            ), headers=self.auth,
        ).get_json()["data"]["slots"]
        session_headers = {"X-Patient-Session": patient_token}
        created = self.client.post(
            "/api/v1/ai/appointments",
            json={"doctor_id": self.doctor_id, "location_id": self.location_id, "start_at": slots[0]["start_at"]},
            headers=dict(self.auth, **session_headers, **{"Idempotency-Key": "mutate-book-001"}),
        )
        appointment_no = created.get_json()["data"]["appointment_no"]
        moved = self.client.post(
            "/api/v1/ai/appointments/{}/reschedule".format(appointment_no),
            json={"doctor_id": self.doctor_id, "location_id": self.location_id, "start_at": slots[1]["start_at"]},
            headers=dict(self.auth, **session_headers, **{"Idempotency-Key": "mutate-move-001"}),
        )
        self.assertEqual(moved.status_code, 200, moved.get_json())
        cancelled = self.client.post(
            "/api/v1/ai/appointments/{}/cancel".format(appointment_no),
            json={"reason": "Patient requested cancellation"},
            headers=dict(self.auth, **session_headers, **{"Idempotency-Key": "mutate-cancel-001"}),
        )
        self.assertEqual(cancelled.status_code, 200, cancelled.get_json())
        self.assertEqual(cancelled.get_json()["data"]["status"], "CANCELLED")
        with self.app.app_context():
            appointment = self.models["Appointment"].query.filter_by(appointment_no=appointment_no).one()
            self.assertIsNotNone(appointment.previous_appointment_date)
            self.assertIsNotNone(appointment.previous_appointment_time)
            self.assertIsNotNone(appointment.rescheduled_at)
            self.assertIsNotNone(appointment.cancelled_at)

    def test_otp_resend_cooldown_and_expiry_are_enforced(self):
        identify = self.client.post(
            "/api/v1/ai/patients/identify", json={"mobile": "9876543210"}, headers=self.auth
        )
        reference = identify.get_json()["data"]["identification_ref"]
        first = self.client.post(
            "/api/v1/ai/verification/otp/send",
            json={"identification_ref": reference}, headers=self.auth,
        )
        second = self.client.post(
            "/api/v1/ai/verification/otp/send",
            json={"identification_ref": reference}, headers=self.auth,
        )
        self.assertEqual(second.status_code, 429, second.get_json())
        with self.app.app_context():
            from models import PortalOtpChallenge
            challenge = PortalOtpChallenge.query.order_by(PortalOtpChallenge.id.desc()).first()
            challenge.expires_at = datetime.utcnow() - timedelta(seconds=1)
            self.db.session.commit()
        expired = self.client.post(
            "/api/v1/ai/verification/otp/verify",
            json={"identification_ref": reference, "otp": first.get_json()["data"]["development_otp"]},
            headers=self.auth,
        )
        self.assertEqual(expired.status_code, 400, expired.get_json())
        self.assertEqual(expired.get_json()["error"]["code"], "OTP_EXPIRED")

    def test_report_idor_is_denied_for_other_verified_patient(self):
        identify = self.client.post(
            "/api/v1/ai/patients/identify",
            json={"mobile": "9234567890", "name": "Other Patient"},
            headers=self.auth,
        )
        reference = identify.get_json()["data"]["identification_ref"]
        sent = self.client.post(
            "/api/v1/ai/verification/otp/send",
            json={"identification_ref": reference}, headers=self.auth,
        )
        verified = self.client.post(
            "/api/v1/ai/verification/otp/verify",
            json={"identification_ref": reference, "otp": sent.get_json()["data"]["development_otp"]},
            headers=self.auth,
        )
        headers = dict(self.auth, **{
            "X-Patient-Session": verified.get_json()["data"]["patient_session_token"]
        })
        denied = self.client.post(
            "/api/v1/ai/reports/{}/secure-link".format(self.report_id), headers=headers
        )
        self.assertEqual(denied.status_code, 404, denied.get_json())

    def test_report_link_is_patient_owned_one_time_and_token_is_not_audited(self):
        patient_token = self._patient_session()
        headers = dict(self.auth, **{"X-Patient-Session": patient_token})
        status = self.client.get("/api/v1/ai/reports/status", headers=headers)
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.get_json()["data"]["items"][0]["status"], "READY")
        link = self.client.post(
            "/api/v1/ai/reports/{}/secure-link".format(self.report_id), headers=headers,
        )
        self.assertEqual(link.status_code, 201, link.get_json())
        secure_path = urlparse(link.get_json()["data"]["secure_url"]).path
        first = self.client.get(secure_path)
        self.assertEqual(first.status_code, 200)
        first.close()
        second = self.client.get(secure_path)
        self.assertEqual(second.status_code, 403)
        raw_token = secure_path.rsplit("/", 1)[-1]
        with self.app.app_context():
            audit_paths = [row.endpoint for row in self.app_module.AIAPIRequestAudit.query.all()]
            self.assertNotIn(raw_token, " ".join(audit_paths))

    def test_new_patient_can_register_only_after_owning_phone_by_otp(self):
        identify = self.client.post(
            "/api/v1/ai/patients/identify",
            json={"mobile": "9123456789", "name": "New Patient"},
            headers=self.auth,
        )
        reference = identify.get_json()["data"]["identification_ref"]
        sent = self.client.post(
            "/api/v1/ai/verification/otp/send",
            json={"identification_ref": reference},
            headers=self.auth,
        )
        self.assertEqual(sent.status_code, 202, sent.get_json())
        verified = self.client.post(
            "/api/v1/ai/verification/otp/verify",
            json={
                "identification_ref": reference,
                "otp": sent.get_json()["data"]["development_otp"],
            },
            headers=self.auth,
        )
        self.assertEqual(verified.status_code, 200, verified.get_json())
        with self.app.app_context():
            patient = self.models["Patient"].query.filter_by(mobile="+919123456789").one()
            self.assertEqual(patient.name, "New Patient")

    def test_report_send_uses_fixed_template_and_is_idempotent(self):
        with self.app.app_context():
            self.db.session.add(self.models["NotificationTemplate"](
                event_code="REPORT_LINK", channel="SMS", language="en",
                body_template="{report_title}: {secure_link}", is_active=True,
            ))
            self.db.session.commit()
        patient_token = self._patient_session()
        headers = dict(self.auth, **{
            "X-Patient-Session": patient_token,
            "Idempotency-Key": "report-send-key-001",
        })
        endpoint = "/api/v1/ai/reports/{}/send".format(self.report_id)
        first = self.client.post(endpoint, json={"channel": "SMS"}, headers=headers)
        self.assertEqual(first.status_code, 202, first.get_json())
        replay = self.client.post(endpoint, json={"channel": "SMS"}, headers=headers)
        self.assertEqual(replay.status_code, 200, replay.get_json())
        self.assertTrue(replay.get_json()["data"]["duplicate"])
        with self.app.app_context():
            self.assertEqual(self.models["NotificationDelivery"].query.count(), 1)
            self.assertEqual(self.models["SecureDocumentToken"].query.count(), 1)
            report = self.db.session.get(self.models["LabReport"], self.report_id)
            self.assertEqual(report.delivery_status, "SENT")

    def test_callbacks_complaints_and_notifications_are_idempotent(self):
        callback_headers = dict(self.auth, **{"Idempotency-Key": "callback-key-001"})
        callback = self.client.post("/api/v1/ai/callbacks", json={
            "mobile": "9876543210", "caller_name": "Ravi", "reason": "Need billing help",
        }, headers=callback_headers)
        self.assertEqual(callback.status_code, 201, callback.get_json())
        replay = self.client.post("/api/v1/ai/callbacks", json={
            "mobile": "9876543210", "caller_name": "Ravi", "reason": "Need billing help",
        }, headers=callback_headers)
        self.assertEqual(replay.status_code, 201)
        complaint = self.client.post("/api/v1/ai/complaints", json={
            "mobile": "9876543210", "category": "SERVICE", "summary": "Long wait",
        }, headers=dict(self.auth, **{"Idempotency-Key": "complaint-key-001"}))
        self.assertEqual(complaint.status_code, 201, complaint.get_json())
        patient_token = self._patient_session()
        notification = self.client.post("/api/v1/ai/notifications", json={
            "recipient": "9876543210", "event_code": "APPOINTMENT_CONFIRMATION",
            "channel": "SMS", "language": "en",
            "variables": {"appointment_no": "A-1", "patient_name": "Ravi"},
        }, headers=dict(self.auth, **{
            "X-Patient-Session": patient_token, "Idempotency-Key": "notification-key-001",
        }))
        self.assertEqual(notification.status_code, 202, notification.get_json())
        with self.app.app_context():
            self.assertEqual(self.models["CallbackRequest"].query.count(), 1)
            self.assertEqual(self.models["Complaint"].query.count(), 1)
            self.assertEqual(self.models["NotificationDelivery"].query.count(), 1)


if __name__ == "__main__":
    unittest.main()
