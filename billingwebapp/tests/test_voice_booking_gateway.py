import importlib
import os
import sys
import tempfile
import unittest
from datetime import timedelta
from unittest.mock import patch

from voice_receptionist import FlaskClinicVoiceGateway


class VoiceBookingGatewayTests(unittest.TestCase):
    """Exercise the local voice adapter against a disposable Flask database."""

    ENV_KEYS = (
        "DATABASE_URL",
        "SECRET_KEY",
        "APP_TIMEZONE",
        "ENABLE_BACKGROUND_JOBS",
        "APP_STORAGE_ROOT",
        "APP_BACKUP_ROOT",
        "PUBLIC_PORTAL_OTP_MODE",
        "DEFAULT_ADMIN_PASSWORD",
    )

    @classmethod
    def setUpClass(cls):
        cls.original_env = {key: os.environ.get(key) for key in cls.ENV_KEYS}
        cls.temp_dir = tempfile.TemporaryDirectory()
        os.environ["DATABASE_URL"] = "sqlite:///{}".format(
            os.path.join(cls.temp_dir.name, "voice_booking_test.db")
        )
        os.environ["SECRET_KEY"] = "voice-booking-test-secret"
        os.environ["CSRF_PROTECTION"] = "0"
        os.environ["APP_TIMEZONE"] = "Asia/Kolkata"
        os.environ["ENABLE_BACKGROUND_JOBS"] = "0"
        os.environ["APP_STORAGE_ROOT"] = os.path.join(cls.temp_dir.name, "uploads")
        os.environ["APP_BACKUP_ROOT"] = os.path.join(cls.temp_dir.name, "backups")
        os.environ["PUBLIC_PORTAL_OTP_MODE"] = "twilio_whatsapp"
        os.environ["DEFAULT_ADMIN_PASSWORD"] = "voice-booking-test-password"

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
        self.gateway = FlaskClinicVoiceGateway(self.app_module)

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()

    def test_normal_booking_is_created_only_after_context_bound_otp_verification(self):
        appointment_date = self.app_module.clinic_now().date() + timedelta(days=1)
        with patch.object(self.app_module, "send_portal_otp") as send_otp:
            send_otp.side_effect = lambda **kwargs: (True, kwargs["code"], "")
            pending, error = self.gateway.start_normal_booking(
                patient_name="Asha Sharma",
                mobile="9876543210",
                gender="FEMALE",
                appointment_date=appointment_date,
            )
            otp = send_otp.call_args.kwargs["code"]

        self.assertEqual(error, "")
        self.assertIsNotNone(pending)
        with self.app.app_context():
            booking = self.app_module.PublicAppointmentBooking.query.one()
            self.assertEqual(booking.status, "OTP_PENDING")
            self.assertEqual(self.app_module.Appointment.query.count(), 0)

        failed, error = self.gateway.verify_normal_booking_otp(
            booking_ref=pending.booking_ref,
            mobile="9876543210",
            otp_code="000000",
        )
        self.assertIsNone(failed)
        self.assertIn("Invalid", error)
        with self.app.app_context():
            self.assertEqual(self.app_module.Appointment.query.count(), 0)

        confirmed, error = self.gateway.verify_normal_booking_otp(
            booking_ref=pending.booking_ref,
            mobile="9876543210",
            otp_code=otp,
        )
        self.assertEqual(error, "")
        self.assertIsNotNone(confirmed)
        self.assertEqual(confirmed.appointment_date, appointment_date)
        with self.app.app_context():
            booking = self.app_module.PublicAppointmentBooking.query.one()
            appointment = self.db.session.get(self.app_module.Appointment, booking.appointment_id)
            self.assertEqual(booking.status, "BOOKED")
            self.assertIsNotNone(appointment)
            self.assertIsNone(appointment.token_no)

    def test_cancel_expires_the_otp_reservation_without_creating_an_appointment(self):
        appointment_date = self.app_module.clinic_now().date() + timedelta(days=1)
        with patch.object(self.app_module, "send_portal_otp") as send_otp:
            send_otp.side_effect = lambda **kwargs: (True, kwargs["code"], "")
            pending, error = self.gateway.start_normal_booking(
                patient_name="Asha Sharma",
                mobile="9876543210",
                gender="FEMALE",
                appointment_date=appointment_date,
            )

        self.assertEqual(error, "")
        self.assertIsNotNone(pending)
        self.gateway.cancel_pending_booking(
            booking_ref=pending.booking_ref,
            mobile="9876543210",
        )
        with self.app.app_context():
            booking = self.app_module.PublicAppointmentBooking.query.one()
            self.assertEqual(booking.status, "EXPIRED")
            self.assertEqual(self.app_module.Appointment.query.count(), 0)

    def test_voice_booking_fails_closed_without_a_real_sms_provider(self):
        previous_mode = os.environ.get("PUBLIC_PORTAL_OTP_MODE")
        os.environ["PUBLIC_PORTAL_OTP_MODE"] = "development"
        try:
            pending, error = self.gateway.start_normal_booking(
                patient_name="Asha Sharma",
                mobile="9876543210",
                gender="FEMALE",
                appointment_date=self.app_module.clinic_now().date() + timedelta(days=1),
            )
        finally:
            if previous_mode is None:
                os.environ.pop("PUBLIC_PORTAL_OTP_MODE", None)
            else:
                os.environ["PUBLIC_PORTAL_OTP_MODE"] = previous_mode

        self.assertIsNone(pending)
        self.assertIn("SMS OTP delivery is not configured", error)
        with self.app.app_context():
            self.assertEqual(self.app_module.PublicAppointmentBooking.query.count(), 0)


if __name__ == "__main__":
    unittest.main()
