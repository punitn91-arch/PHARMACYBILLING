"""Tests for provider-neutral telephony safety primitives."""

from __future__ import annotations

import unittest

from services.telephony import (
    TelephonyEvent,
    build_telephony_event,
    normalize_indian_caller_e164,
    sign_hmac_request,
    verify_hmac_signed_request,
)


class IndianCallerNormalizationTests(unittest.TestCase):
    def test_normalizes_common_indian_e164_forms(self):
        expected = "+919876543210"
        for raw_caller in (
            "9876543210",
            "09876543210",
            "919876543210",
            "+91 98765-43210",
            "0091 (98765) 43210",
        ):
            with self.subTest(raw_caller=raw_caller):
                result = normalize_indian_caller_e164(raw_caller)
                self.assertTrue(result.ok)
                self.assertEqual(result.value, expected)

    def test_rejects_invalid_or_unbounded_caller_without_echoing_it(self):
        raw_caller = "+91 00000 00000"
        for value in (raw_caller, "12345", "+921234567890", "98765abc210", "9" * 33, None):
            with self.subTest(value=value):
                result = normalize_indian_caller_e164(value)
                self.assertFalse(result.ok)
                self.assertIsNotNone(result.error)
                self.assertEqual(result.error.code, "invalid_caller")
                self.assertNotIn(raw_caller, result.error.public_message)


class TelephonyEventTests(unittest.TestCase):
    def test_builds_bounded_event_and_redacts_caller_from_repr(self):
        result = build_telephony_event(
            provider="Demo-Carrier",
            provider_call_id="call:abc-123",
            event_id="event:001",
            event_type="CALL.ANSWERED",
            caller="+91 98765 43210",
            occurred_at="1700000000",
        )

        self.assertTrue(result.ok)
        self.assertIsInstance(result.value, TelephonyEvent)
        self.assertEqual(result.value.provider, "demo-carrier")
        self.assertEqual(result.value.event_type, "call.answered")
        self.assertEqual(result.value.caller_e164, "+919876543210")
        self.assertNotIn("9876543210", repr(result.value))

    def test_rejects_invalid_event_field_before_it_can_be_persisted(self):
        result = build_telephony_event(
            provider="demo carrier",  # spaces are deliberately invalid identifiers
            provider_call_id="call-123",
            event_id="event-123",
            event_type="call.answered",
            caller="9876543210",
            occurred_at=1700000000,
        )

        self.assertFalse(result.ok)
        self.assertIsNotNone(result.error)
        self.assertEqual(result.error.code, "invalid_event_field")
        self.assertEqual(result.error.status_code, 400)


class SignedRequestTests(unittest.TestCase):
    SECRET = "unit-test-webhook-secret"
    METHOD = "POST"
    TARGET = "/telephony/inbound?flow=clinic"
    TIMESTAMP = 1_700_000_000
    BODY = b'{"event":"call.answered"}'

    def _signature(self):
        result = sign_hmac_request(
            secret=self.SECRET,
            method=self.METHOD,
            request_target=self.TARGET,
            timestamp=self.TIMESTAMP,
            body=self.BODY,
        )
        self.assertTrue(result.ok)
        return result.value

    def test_accepts_valid_canonical_hmac_signature_within_replay_window(self):
        result = verify_hmac_signed_request(
            secret=self.SECRET,
            method=self.METHOD,
            request_target=self.TARGET,
            timestamp=self.TIMESTAMP,
            signature=self._signature(),
            body=self.BODY,
            now=self.TIMESTAMP + 60,
            replay_window_seconds=300,
        )

        self.assertTrue(result.ok)
        self.assertIsNone(result.error)

    def test_rejects_invalid_signature_and_tampered_body(self):
        invalid_signature = verify_hmac_signed_request(
            secret=self.SECRET,
            method=self.METHOD,
            request_target=self.TARGET,
            timestamp=self.TIMESTAMP,
            signature="sha256=" + ("0" * 64),
            body=self.BODY,
            now=self.TIMESTAMP,
            replay_window_seconds=300,
        )
        tampered_body = verify_hmac_signed_request(
            secret=self.SECRET,
            method=self.METHOD,
            request_target=self.TARGET,
            timestamp=self.TIMESTAMP,
            signature=self._signature(),
            body=b'{"event":"call.ended"}',
            now=self.TIMESTAMP,
            replay_window_seconds=300,
        )

        for result in (invalid_signature, tampered_body):
            self.assertFalse(result.ok)
            self.assertIsNotNone(result.error)
            self.assertEqual(result.error.code, "invalid_signature")
            self.assertEqual(result.error.status_code, 401)

    def test_rejects_expired_timestamp_before_accepting_signature(self):
        result = verify_hmac_signed_request(
            secret=self.SECRET,
            method=self.METHOD,
            request_target=self.TARGET,
            timestamp=self.TIMESTAMP,
            signature=self._signature(),
            body=self.BODY,
            now=self.TIMESTAMP + 301,
            replay_window_seconds=300,
        )

        self.assertFalse(result.ok)
        self.assertIsNotNone(result.error)
        self.assertEqual(result.error.code, "expired_timestamp")
        self.assertEqual(result.error.status_code, 401)


if __name__ == "__main__":
    unittest.main()
