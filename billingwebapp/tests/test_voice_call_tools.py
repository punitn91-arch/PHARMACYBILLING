import unittest
from datetime import date, datetime

from services.voice_call_tools import (
    AuthorizationStatus,
    AuthorizedProtectedCall,
    AuthorizedPublicCall,
    CallAuthorizationContext,
    CallToolRequest,
    ClinicCallToolPolicy,
    ProtectedCallAction,
    ProtectedInformationResult,
    PublicCallAction,
    PublicInformationResult,
)


class VoiceCallToolsPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = ClinicCallToolPolicy()

    def test_public_clinic_information_is_allowed_without_otp(self):
        decision = self.policy.authorize(CallToolRequest("clinic_information"))

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.status, AuthorizationStatus.ALLOWED)
        self.assertIsInstance(decision.call, AuthorizedPublicCall)
        self.assertEqual(decision.call.action, PublicCallAction.CLINIC_INFORMATION)
        self.assertIsNone(decision.call.appointment_date)

    def test_public_schedule_accepts_only_one_iso_date_argument(self):
        decision = self.policy.authorize(
            CallToolRequest("doctor_schedule", {"appointment_date": "2026-08-24"})
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.call.action, PublicCallAction.DOCTOR_SCHEDULE)
        self.assertEqual(decision.call.appointment_date, date(2026, 8, 24))

    def test_public_availability_rejects_missing_invalid_or_extra_arguments(self):
        missing = self.policy.authorize(CallToolRequest("appointment_availability"))
        invalid = self.policy.authorize(
            CallToolRequest("appointment_availability", {"appointment_date": "tomorrow"})
        )
        extra = self.policy.authorize(
            CallToolRequest(
                "appointment_availability",
                {"appointment_date": "2026-08-24", "mobile": "9876543210"},
            )
        )

        for decision in (missing, invalid, extra):
            self.assertEqual(decision.status, AuthorizationStatus.DENIED)
            self.assertFalse(decision.allowed)
            self.assertIsNone(decision.call)
            self.assertNotIn("9876543210", decision.summary)

    def test_date_objects_are_typed_but_datetimes_are_rejected(self):
        accepted = self.policy.authorize(
            CallToolRequest("doctor_schedule", {"appointment_date": date(2026, 8, 24)})
        )
        rejected = self.policy.authorize(
            CallToolRequest(
                "doctor_schedule", {"appointment_date": datetime(2026, 8, 24, 10, 30)}
            )
        )

        self.assertTrue(accepted.allowed)
        self.assertEqual(accepted.call.appointment_date, date(2026, 8, 24))
        self.assertEqual(rejected.status, AuthorizationStatus.DENIED)

    def test_unknown_tool_and_non_mapping_arguments_are_rejected(self):
        unknown = self.policy.authorize(CallToolRequest("delete_patient", {}))
        malformed = self.policy.authorize(CallToolRequest("clinic_information", ["anything"]))

        self.assertEqual(unknown.status, AuthorizationStatus.DENIED)
        self.assertEqual(malformed.status, AuthorizationStatus.DENIED)
        self.assertIsNone(unknown.call)
        self.assertIsNone(malformed.call)

    def test_protected_tool_requires_verified_otp_context(self):
        no_context = self.policy.authorize(CallToolRequest("lab_report_status"))
        empty_subject = self.policy.authorize(
            CallToolRequest("lab_report_status"),
            CallAuthorizationContext(otp_verified=True),
        )

        for decision in (no_context, empty_subject):
            self.assertEqual(decision.status, AuthorizationStatus.OTP_REQUIRED)
            self.assertTrue(decision.requires_otp)
            self.assertFalse(decision.allowed)
            self.assertIsNone(decision.call)

    def test_protected_tool_uses_opaque_verified_context_not_tool_arguments(self):
        context = CallAuthorizationContext(
            otp_verified=True,
            verified_caller_ref="server-session-opaque-reference",
        )
        decision = self.policy.authorize(CallToolRequest("patient_appointment_status"), context)

        self.assertTrue(decision.allowed)
        self.assertIsInstance(decision.call, AuthorizedProtectedCall)
        self.assertEqual(decision.call.action, ProtectedCallAction.PATIENT_APPOINTMENT_STATUS)
        self.assertNotIn("server-session-opaque-reference", repr(decision.call))
        self.assertNotIn("server-session-opaque-reference", decision.summary)

    def test_protected_tools_reject_otp_mobile_and_patient_arguments(self):
        context = CallAuthorizationContext(otp_verified=True, verified_caller_ref="opaque")
        for arguments in (
            {"mobile": "9876543210"},
            {"otp_code": "123456"},
            {"patient_id": 7},
            {"report_id": 42},
        ):
            with self.subTest(arguments=arguments):
                decision = self.policy.authorize(
                    CallToolRequest("lab_report_status", arguments), context
                )
                self.assertEqual(decision.status, AuthorizationStatus.DENIED)
                self.assertNotIn("9876543210", decision.summary)
                self.assertNotIn("123456", decision.summary)


class VoiceCallToolsResultTests(unittest.TestCase):
    def test_public_result_redacts_accidental_patient_fields(self):
        result = PublicInformationResult(
            action=PublicCallAction.CLINIC_INFORMATION,
            summary="Clinic reply for Asha Sharma on 9876543210.",
            fields={
                "patient_name": "Asha Sharma",
                "mobile": "9876543210",
                "arrival_window": "5:30 PM to 7:45 PM",
            },
        )

        self.assertNotIn("Asha Sharma", result.summary)
        self.assertNotIn("9876543210", result.summary)
        self.assertEqual(result.fields["patient_name"], "[redacted]")
        self.assertEqual(result.fields["mobile"], "[redacted]")
        self.assertEqual(result.fields["arrival_window"], "5:30 PM to 7:45 PM")

    def test_protected_result_redacts_otp_reference_and_patient_data(self):
        result = ProtectedInformationResult(
            action=ProtectedCallAction.LAB_REPORT_STATUS,
            summary=(
                "Asha Sharma's report is ready for 9876543210; OTP 123456 and "
                "reference booking-abc must not be spoken."
            ),
            fields={
                "patient_name": "Asha Sharma",
                "mobile": "9876543210",
                "otp_code": "123456",
                "booking_ref": "booking-abc",
                "report_status": "PUBLISHED",
            },
        )

        for value in ("Asha Sharma", "9876543210", "123456", "booking-abc"):
            self.assertNotIn(value, result.summary)
        self.assertEqual(result.fields["patient_name"], "[redacted]")
        self.assertEqual(result.fields["mobile"], "[redacted]")
        self.assertEqual(result.fields["otp_code"], "[redacted]")
        self.assertEqual(result.fields["booking_ref"], "[redacted]")
        self.assertEqual(result.fields["report_status"], "PUBLISHED")

    def test_unverified_protected_result_fails_closed(self):
        result = ProtectedInformationResult(
            action=ProtectedCallAction.LAB_REPORT_STATUS,
            summary="Patient mobile 9876543210 has a report.",
            fields={"mobile": "9876543210", "report_status": "PUBLISHED"},
            otp_verified=False,
        )

        self.assertEqual(
            result.summary,
            "Verification is required before patient-specific information can be shared.",
        )
        self.assertEqual(dict(result.fields), {})


if __name__ == "__main__":
    unittest.main()
