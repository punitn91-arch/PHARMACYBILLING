import importlib
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, time, timedelta
from io import BytesIO
from unittest.mock import patch


class PublicPatientPortalTests(unittest.TestCase):
    """Regression coverage for OTP-protected public patient portals."""

    ENV_KEYS = (
        "DATABASE_URL",
        "SECRET_KEY",
        "APP_TIMEZONE",
        "ENABLE_BACKGROUND_JOBS",
        "APP_STORAGE_ROOT",
        "APP_BACKUP_ROOT",
        "PUBLIC_PORTAL_OTP_MODE",
        "PUBLIC_PORTAL_SHOW_DEV_OTP",
    )

    @classmethod
    def setUpClass(cls):
        cls.original_env = {key: os.environ.get(key) for key in cls.ENV_KEYS}
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "public_portals_test.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "public-portals-test-secret"
        os.environ["APP_TIMEZONE"] = "Asia/Kolkata"
        os.environ["ENABLE_BACKGROUND_JOBS"] = "0"
        os.environ["APP_STORAGE_ROOT"] = os.path.join(cls.temp_dir.name, "uploads")
        os.environ["APP_BACKUP_ROOT"] = os.path.join(cls.temp_dir.name, "backups")
        os.environ["PUBLIC_PORTAL_OTP_MODE"] = "test"
        os.environ["PUBLIC_PORTAL_SHOW_DEV_OTP"] = "1"

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

        # Keep test PDFs outside the workspace's normal instance directory.
        cls.private_report_dir = os.path.join(cls.temp_dir.name, "private_lab_reports")
        os.makedirs(cls.private_report_dir, exist_ok=True)
        cls.app_module.LAB_REPORT_UPLOAD_FOLDER = cls.private_report_dir
        cls.app.config["INFRA_UPLOAD_DIRECTORIES"]["private_lab_reports"] = cls.private_report_dir

    @classmethod
    def tearDownClass(cls):
        for key, value in cls.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        cls.temp_dir.cleanup()

    def setUp(self):
        self.client = self.app.test_client()
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.create_all()
            self._seed_admin()

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()

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

    def _login_admin(self):
        response = self.client.post(
            "/login",
            data={"username": "admin", "password": "Admin@123"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

    def _create_lab_order(self, *, mobile="9876543210"):
        with self.app.app_context():
            patient = self.app_module.Patient(
                name="Ravi Kumar",
                mobile=mobile,
                gender="MALE",
            )
            self.db.session.add(patient)
            self.db.session.flush()
            order = self.app_module.LabOrder(
                order_no="LAB-TEST-0001",
                patient_id=patient.id,
                patient_name=patient.name,
                mobile=mobile,
                gender="MALE",
                doctor="Dr. Test",
                subtotal=450,
                total=450,
                payment_mode="CASH",
                status="ORDERED",
                created_by="admin",
            )
            self.db.session.add(order)
            self.db.session.commit()
            return order.id

    def _send_and_verify_report_otp(self, client, mobile):
        with patch.object(self.app_module, "send_portal_otp") as send_otp:
            send_otp.side_effect = lambda **kwargs: (True, kwargs["code"], "")
            send_response = client.post(
                "/my-lab-reports/send-otp",
                data={"mobile": mobile},
                follow_redirects=False,
            )
            otp = send_otp.call_args.kwargs["code"]
        self.assertEqual(send_response.status_code, 200)
        self.assertIn(b"verification code", send_response.data.lower())

        verify_response = client.post(
            "/my-lab-reports/verify-otp",
            data={"otp": otp},
            follow_redirects=False,
        )
        self.assertEqual(verify_response.status_code, 302)
        self.assertIn("/my-lab-reports/list", verify_response.headers["Location"])

    def test_normal_qr_booking_requires_otp_creates_appointment_and_stops_at_15(self):
        target_date = self.app_module.clinic_now().date() + timedelta(days=1)
        first_slot = datetime.combine(target_date, time(10, 0))

        with self.app.app_context():
            settings = self.app_module.AppointmentBookingSettings(
                booking_enabled=True,
                normal_daily_limit=15,
                priority_daily_limit=2,
                normal_fee=600,
                priority_fee=1000,
                opening_time="17:30",
                slot_minutes=20,
                arrival_window_start="17:30",
                arrival_window_end="19:45",
                max_days_ahead=14,
                booking_cutoff_minutes=0,
            )
            self.db.session.add(settings)
            for index in range(14):
                appointment_time = (first_slot + timedelta(minutes=20 * index)).time()
                self.db.session.add(
                    self.app_module.Appointment(
                        appointment_no=f"APT-{index + 1:06d}",
                        token_no=index + 1,
                        patient_name=f"Existing Patient {index + 1}",
                        mobile=f"900000{index:04d}",
                        gender="OTHER",
                        doctor_name="Dr. Test",
                        appointment_date=target_date,
                        appointment_time=appointment_time,
                        payment_mode="CASH",
                        payment_status="UNPAID",
                        consultation_fee=600,
                        status="BOOKED",
                        created_by="admin",
                        is_deleted=False,
                    )
                )
            self.db.session.commit()

        page_response = self.client.get(f"/book-appointment?date={target_date.isoformat()}")
        self.assertEqual(page_response.status_code, 200)
        self.assertIn(b"5:30 PM", page_response.data)
        self.assertIn(b"7:45 PM", page_response.data)
        self.assertIn(b"first-come, first-served", page_response.data)
        self.assertIn(b"sticky-clinic-intro", page_response.data)
        self.assertIn(b"position: sticky", page_response.data)
        self.assertNotIn(b'name="appointment_time"', page_response.data)

        booking_payload = {
            "patient_name": "Asha Sharma",
            "mobile": "9876543210",
            "gender": "FEMALE",
            "appointment_date": target_date.isoformat(),
            # A browser cannot choose a time.  This hostile legacy value must
            # be ignored in favour of the server's FCFS compatibility marker.
            "appointment_time": "09:00",
            "booking_type": "NORMAL",
            "symptoms": "Routine consultation",
        }
        with patch.object(self.app_module, "send_portal_otp") as send_otp:
            send_otp.side_effect = lambda **kwargs: (True, kwargs["code"], "")
            start_response = self.client.post(
                "/book-appointment",
                data=booking_payload,
                follow_redirects=False,
            )
            otp = send_otp.call_args.kwargs["code"]
        self.assertEqual(start_response.status_code, 302)
        self.assertIn("/book-appointment/verify", start_response.headers["Location"])

        verify_response = self.client.post(
            "/book-appointment/verify",
            data={"otp_code": otp},
            follow_redirects=False,
        )
        self.assertEqual(verify_response.status_code, 200)
        self.assertIn(b"Appointment confirmed", verify_response.data)

        with self.app.app_context():
            booking = self.app_module.PublicAppointmentBooking.query.filter_by(
                mobile="9876543210"
            ).one()
            appointment = self.db.session.get(self.app_module.Appointment, booking.appointment_id)
            self.assertEqual(booking.status, "BOOKED")
            self.assertIsNotNone(booking.otp_verified_at)
            self.assertIsNotNone(appointment)
            self.assertEqual(appointment.created_by, "PUBLIC_QR")
            self.assertIsNone(appointment.token_no)
            self.assertEqual(booking.appointment_time, time(17, 30))
            self.assertEqual(appointment.appointment_time, time(17, 30))
            self.assertEqual(appointment.appointment_date, target_date)
            self.assertEqual(self.app_module.Appointment.query.filter_by(appointment_date=target_date).count(), 15)
            appointment_id = appointment.id

            # A public confirmation is a capacity reservation, not a place on
            # the live token board before reception records the arrival.
            queue_snapshot = self.app_module.build_live_queue_snapshot(target_date)
            reserved_row = next(
                item for item in queue_snapshot["lanes"]["scheduled"]
                if item["id"] == appointment_id
            )
            self.assertTrue(reserved_row["unarrived_public_reservation"])
            self.assertFalse(reserved_row["token_assigned"])
            self.assertNotEqual(
                (queue_snapshot["current_serving"] or {}).get("id"),
                appointment_id,
            )

        # The queue token belongs to reception, so it is issued only when the
        # patient is marked as arrived—not when OTP confirmation succeeds.
        self._login_admin()
        check_in_response = self.client.post(
            f"/appointments/{appointment_id}/status",
            data={"status": "WAITING"},
            follow_redirects=False,
        )
        self.assertEqual(check_in_response.status_code, 302)
        with self.app.app_context():
            appointment = self.db.session.get(self.app_module.Appointment, appointment_id)
            self.assertEqual(appointment.token_no, 15)
            queue_snapshot = self.app_module.build_live_queue_snapshot(target_date)
            queued_row = next(
                item for item in queue_snapshot["lanes"]["waiting"]
                if item["id"] == appointment_id
            )
            self.assertTrue(queued_row["token_assigned"])
            self.assertEqual(queued_row["token_no"], 15)

        rejected_payload = dict(booking_payload)
        rejected_payload.update({
            "patient_name": "Sixteenth Patient",
            "mobile": "9876543211",
            "appointment_time": "23:59",
        })
        rejected_response = self.client.post(
            "/book-appointment",
            data=rejected_payload,
            follow_redirects=False,
        )
        self.assertEqual(rejected_response.status_code, 400)
        self.assertIn(b"Regular appointments are full", rejected_response.data)
        with self.app.app_context():
            self.assertEqual(
                self.app_module.PublicAppointmentBooking.query.filter_by(mobile="9876543211").count(),
                0,
            )

    def test_priority_request_needs_staff_verified_payment_before_confirmation(self):
        target_date = self.app_module.clinic_now().date() + timedelta(days=1)
        first_slot = datetime.combine(target_date, time(10, 0))
        with self.app.app_context():
            settings = self.app_module.AppointmentBookingSettings(
                booking_enabled=True,
                normal_daily_limit=15,
                priority_daily_limit=2,
                normal_fee=600,
                priority_fee=1000,
                opening_time="17:30",
                slot_minutes=20,
                arrival_window_start="17:30",
                arrival_window_end="19:45",
                max_days_ahead=14,
                booking_cutoff_minutes=0,
            )
            self.db.session.add(settings)
            for index in range(15):
                self.db.session.add(
                    self.app_module.Appointment(
                        appointment_no=f"APT-{index + 1:06d}",
                        token_no=index + 1,
                        patient_name=f"Regular {index + 1}",
                        mobile=f"901000{index:04d}",
                        gender="OTHER",
                        doctor_name="Dr. Test",
                        appointment_date=target_date,
                        appointment_time=(first_slot + timedelta(minutes=20 * index)).time(),
                        payment_mode="CASH",
                        payment_status="UNPAID",
                        consultation_fee=600,
                        status="BOOKED",
                        created_by="admin",
                        is_deleted=False,
                    )
                )
            self.db.session.commit()

        priority_payload = {
            "patient_name": "Priority Patient",
            "mobile": "9876543212",
            "gender": "OTHER",
            "appointment_date": target_date.isoformat(),
            "appointment_time": "08:00",
            "booking_type": "PRIORITY",
            "symptoms": "Needs same-day review",
        }
        with patch.object(self.app_module, "send_portal_otp") as send_otp:
            send_otp.side_effect = lambda **kwargs: (True, kwargs["code"], "")
            start_response = self.client.post("/book-appointment", data=priority_payload, follow_redirects=False)
            otp = send_otp.call_args.kwargs["code"]
        self.assertEqual(start_response.status_code, 302)
        verify_response = self.client.post("/book-appointment/verify", data={"otp_code": otp})
        self.assertEqual(verify_response.status_code, 200)
        self.assertIn(b"Awaiting final confirmation", verify_response.data)

        with self.app.app_context():
            booking = self.app_module.PublicAppointmentBooking.query.filter_by(mobile="9876543212").one()
            self.assertEqual(booking.status, "PAYMENT_PENDING")
            booking_id = booking.id
            self.assertIsNone(booking.appointment_id)

        self._login_admin()
        confirm_response = self.client.post(
            f"/appointment-booking/priority-requests/{booking_id}/confirm",
            data={"payment_mode": "UPI", "payment_reference": "UPI-VERIFIED-001"},
            follow_redirects=False,
        )
        self.assertEqual(confirm_response.status_code, 302)
        with self.app.app_context():
            booking = self.db.session.get(self.app_module.PublicAppointmentBooking, booking_id)
            appointment = self.db.session.get(self.app_module.Appointment, booking.appointment_id)
            self.assertEqual(booking.status, "BOOKED")
            self.assertEqual(booking.payment_provider, "MANUAL_VERIFIED")
            self.assertEqual(appointment.payment_status, "PAID")
            self.assertEqual(appointment.payment_mode, "UPI")
            self.assertIsNone(appointment.token_no)
            self.assertEqual(appointment.appointment_time, time(17, 30))

    def test_admin_can_render_a_patient_safe_booking_qr(self):
        self._login_admin()
        with patch.dict(os.environ, {"PUBLIC_BOOKING_BASE_URL": "https://clinic.example"}, clear=False):
            page_response = self.client.get("/appointment-booking/qr")
            self.assertEqual(page_response.status_code, 200)
            self.assertIn(b"https://clinic.example/appointment", page_response.data)
            self.assertNotIn(b"patient_name", page_response.data)

            png_response = self.client.get("/appointment-booking/qr.png")
            self.assertEqual(png_response.status_code, 200)
            self.assertEqual(png_response.mimetype, "image/png")
            self.assertEqual(png_response.data[:8], b"\x89PNG\r\n\x1a\n")

    def test_public_appointment_landing_links_to_booking_form(self):
        response = self.client.get("/appointment")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Book Appointment", response.data)
        self.assertIn(b'href="/book-appointment"', response.data)

    def test_admin_can_manage_public_booking_controls(self):
        self._login_admin()
        settings_response = self.client.get("/appointment-booking/settings")
        self.assertEqual(settings_response.status_code, 200)
        self.assertIn(b"Regular daily capacity", settings_response.data)
        self.assertIn(b"Clinic visit window starts", settings_response.data)
        self.assertNotIn(b"First slot", settings_response.data)

        save_response = self.client.post(
            "/appointment-booking/settings",
            data={
                "booking_enabled": "on",
                "normal_daily_limit": "15",
                "priority_daily_limit": "2",
                "normal_fee": "600",
                "priority_fee": "1000",
                "arrival_window_start": "17:30",
                "arrival_window_end": "19:45",
                "max_days_ahead": "14",
                "booking_cutoff_minutes": "30",
            },
            follow_redirects=False,
        )
        self.assertEqual(save_response.status_code, 302)
        with self.app.app_context():
            settings = self.app_module.AppointmentBookingSettings.query.one()
            self.assertEqual(settings.normal_daily_limit, 15)
            self.assertEqual(settings.priority_fee, 1000)
            self.assertEqual(settings.arrival_window_start, "17:30")
            self.assertEqual(settings.arrival_window_end, "19:45")

        queue_response = self.client.get("/appointment-booking/priority-requests")
        self.assertEqual(queue_response.status_code, 200)
        self.assertIn(b"Priority appointment requests", queue_response.data)

    def test_msg91_sms_otp_uses_the_approved_flow_template_without_a_live_request(self):
        """The real SMS mode must submit the DLT/MSG91 template payload, not WhatsApp."""

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return False

            def read(self):
                return b'{"type":"success","message":"queued"}'

        delivery_module = importlib.import_module(self.app_module.send_portal_otp.__module__)
        with (
            patch.dict(
                os.environ,
                {
                    "PUBLIC_PORTAL_OTP_MODE": "msg91_sms",
                    "MSG91_AUTH_KEY": "test-auth-key",
                    "MSG91_TEMPLATE_ID": "approved-flow-template-id",
                    "MSG91_OTP_VARIABLE": "verification_code",
                    "MSG91_SENDER_ID": "ENDOCN",
                },
                clear=False,
            ),
            patch.object(delivery_module.urllib.request, "urlopen", return_value=FakeResponse()) as urlopen,
        ):
            sent, development_code, error_message = self.app_module.send_portal_otp(
                mobile="+91 98765 43210",
                code="123456",
                purpose="Public Appointment",
                is_production=True,
            )

        self.assertTrue(sent)
        self.assertEqual(development_code, "")
        self.assertEqual(error_message, "")
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://control.msg91.com/api/v5/flow")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authkey"), "test-auth-key")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(
            json.loads(request.data.decode("utf-8")),
            {
                "template_id": "approved-flow-template-id",
                "short_url": "0",
                "sender": "ENDOCN",
                "recipients": [
                    {"mobiles": "919876543210", "verification_code": "123456"},
                ],
            },
        )

    def test_msg91_sms_otp_never_attempts_delivery_without_required_config(self):
        delivery_module = importlib.import_module(self.app_module.send_portal_otp.__module__)
        with (
            patch.dict(
                os.environ,
                {
                    "PUBLIC_PORTAL_OTP_MODE": "msg91_sms",
                    "MSG91_AUTH_KEY": "",
                    "MSG91_AUTHKEY": "",
                    "MSG91_TEMPLATE_ID": "",
                    "MSG91_FLOW_ID": "",
                },
                clear=False,
            ),
            patch.object(delivery_module.urllib.request, "urlopen") as urlopen,
        ):
            sent, development_code, error_message = self.app_module.send_portal_otp(
                mobile="9876543210",
                code="123456",
                purpose="Public Appointment",
                is_production=True,
            )

        self.assertFalse(sent)
        self.assertEqual(development_code, "")
        self.assertEqual(error_message, "SMS OTP delivery is not configured yet. Please contact the clinic.")
        urlopen.assert_not_called()

    def test_staff_uploaded_private_pdf_is_available_only_after_matching_mobile_otp(self):
        order_id = self._create_lab_order()
        self._login_admin()
        upload_response = self.client.post(
            f"/lab-order/{order_id}/reports/upload",
            data={
                "report_title": "CBC Final Report",
                "report_date": "2026-08-16",
                "patient_note": "Please review with your doctor.",
                "report_file": (BytesIO(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"), "cbc-final.pdf"),
            },
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        self.assertEqual(upload_response.status_code, 302)

        with self.app.app_context():
            report = self.app_module.LabReport.query.one()
            report_id = report.id
            storage_key = report.storage_key
            private_path = os.path.join(self.private_report_dir, storage_key)
            self.assertEqual(report.status, "PUBLISHED")
            self.assertTrue(os.path.isfile(private_path))
            self.assertFalse(private_path.startswith(os.path.join(self.app.static_folder, "uploads")))

        public_client = self.app.test_client()
        no_session_response = public_client.get(
            f"/my-lab-reports/{report_id}/download",
            follow_redirects=False,
        )
        self.assertEqual(no_session_response.status_code, 302)

        self._send_and_verify_report_otp(public_client, "9876543210")
        list_response = public_client.get("/my-lab-reports/list")
        self.assertEqual(list_response.status_code, 200)
        self.assertIn(b"CBC Final Report", list_response.data)
        self.assertNotIn(storage_key.encode("utf-8"), list_response.data)

        download_response = public_client.get(f"/my-lab-reports/{report_id}/download")
        self.assertEqual(download_response.status_code, 200)
        self.assertEqual(download_response.data[:5], b"%PDF-")
        self.assertIn("attachment", download_response.headers.get("Content-Disposition", ""))
        self.assertIn("private", download_response.headers.get("Cache-Control", ""))

        with self.app.app_context():
            report = self.db.session.get(self.app_module.LabReport, report_id)
            self.assertEqual(report.download_count, 1)

        # A second verified mobile must not learn or download a guessed report id.
        other_client = self.app.test_client()
        self._send_and_verify_report_otp(other_client, "9123456789")
        other_mobile_download = other_client.get(
            f"/my-lab-reports/{report_id}/download",
            follow_redirects=False,
        )
        self.assertEqual(other_mobile_download.status_code, 404)

        # The storage key is not a public static asset; authorization is required.
        static_response = public_client.get(f"/static/uploads/{storage_key}")
        self.assertEqual(static_response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
