import importlib
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime


class VoiceCallSessionTests(unittest.TestCase):
    ENV_KEYS = (
        "DATABASE_URL",
        "SECRET_KEY",
        "APP_TIMEZONE",
        "ENABLE_BACKGROUND_JOBS",
        "APP_STORAGE_ROOT",
        "APP_BACKUP_ROOT",
        "VOICE_TELEPHONY_ENABLED",
        "VOICE_TELEPHONY_PROVIDER",
        "VOICE_TELEPHONY_WEBHOOK_SECRET",
        "VOICE_TELEPHONY_FINGERPRINT_SECRET",
    )

    @classmethod
    def setUpClass(cls):
        cls.original_env = {key: os.environ.get(key) for key in cls.ENV_KEYS}
        cls.temp_dir = tempfile.TemporaryDirectory()
        os.environ["DATABASE_URL"] = "sqlite:///{}".format(
            os.path.join(cls.temp_dir.name, "voice_calls.db")
        )
        os.environ["SECRET_KEY"] = "voice-call-session-test-secret"
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

        from services.telephony import build_telephony_event, sign_hmac_request
        from services.voice_call_sessions import (
            VoiceCallSessionError,
            persist_verified_telephony_event,
            safe_call_context,
        )

        cls.build_telephony_event = staticmethod(build_telephony_event)
        cls.sign_hmac_request = staticmethod(sign_hmac_request)
        cls.persist_verified_telephony_event = staticmethod(persist_verified_telephony_event)
        cls.safe_call_context = staticmethod(safe_call_context)
        cls.VoiceCallSessionError = VoiceCallSessionError

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

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()

    def _event(
        self,
        *,
        event_id="event-001",
        event_type="call.answered",
        occurred_at=1_700_000_000,
        caller="+91 98765 43210",
        provider_call_id="call-001",
    ):
        result = self.build_telephony_event(
            provider="staging-carrier",
            provider_call_id=provider_call_id,
            event_id=event_id,
            event_type=event_type,
            caller=caller,
            occurred_at=occurred_at,
        )
        self.assertTrue(result.ok)
        return result.value

    def test_verified_event_is_idempotent_and_does_not_store_raw_caller(self):
        body = b'{"safe":"metadata"}'
        with self.app.app_context():
            receipt = self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(),
                raw_body=body,
                fingerprint_secret="test-telephony-webhook-secret",
                now=datetime(2026, 8, 23, 10, 0),
            )
            self.assertFalse(receipt.duplicate)
            self.assertEqual(receipt.call_status, "IN_PROGRESS")
            call = self.app_module.VoiceCall.query.one()
            self.assertEqual(call.caller_last4, "3210")
            self.assertNotEqual(call.caller_fingerprint, "+919876543210")
            self.assertEqual(len(call.caller_fingerprint), 64)
            self.assertFalse(hasattr(call, "caller_e164"))
            self.assertEqual(self.app_module.VoiceCallEvent.query.count(), 1)
            stored_event = self.app_module.VoiceCallEvent.query.one()
            self.assertEqual(len(stored_event.event_fingerprint), 64)
            self.assertNotIn("9876543210", stored_event.event_fingerprint)

            retry = self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(),
                raw_body=body,
                fingerprint_secret="test-telephony-webhook-secret",
                now=datetime(2026, 8, 23, 10, 1),
            )
            self.assertTrue(retry.duplicate)
            self.assertEqual(self.app_module.VoiceCall.query.count(), 1)
            self.assertEqual(self.app_module.VoiceCallEvent.query.count(), 1)

    def test_terminal_event_completes_the_existing_call_without_creating_another(self):
        with self.app.app_context():
            self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(event_id="event-start"),
                raw_body=b"start",
                fingerprint_secret="test-telephony-webhook-secret",
                now=datetime(2026, 8, 23, 10, 0),
            )
            receipt = self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(
                    event_id="event-end",
                    event_type="call.completed",
                    occurred_at=1_700_000_240,
                ),
                raw_body=b"end",
                fingerprint_secret="test-telephony-webhook-secret",
                now=datetime(2026, 8, 23, 10, 4),
            )
            call = self.app_module.VoiceCall.query.one()
            self.assertFalse(receipt.duplicate)
            self.assertEqual(receipt.call_status, "COMPLETED")
            self.assertEqual(call.status, "COMPLETED")
            self.assertIsNotNone(call.ended_at)
            self.assertEqual(call.duration_seconds, 240)
            self.assertEqual(self.app_module.VoiceCallEvent.query.count(), 2)

    def test_late_event_never_reopens_a_completed_call(self):
        with self.app.app_context():
            self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(event_id="event-start", occurred_at=1_700_000_000),
                raw_body=b"start",
                fingerprint_secret="test-telephony-fingerprint-secret",
                now=datetime(2026, 8, 23, 10, 0),
            )
            self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(
                    event_id="event-completed",
                    event_type="call.completed",
                    occurred_at=1_700_000_240,
                ),
                raw_body=b"completed",
                fingerprint_secret="test-telephony-fingerprint-secret",
                now=datetime(2026, 8, 23, 10, 4),
            )
            receipt = self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(
                    event_id="event-late-answered",
                    event_type="call.answered",
                    occurred_at=1_700_000_100,
                ),
                raw_body=b"late-answered",
                fingerprint_secret="test-telephony-fingerprint-secret",
                now=datetime(2026, 8, 23, 10, 5),
            )
            call = self.app_module.VoiceCall.query.one()
            self.assertFalse(receipt.duplicate)
            self.assertEqual(call.status, "COMPLETED")
            self.assertEqual(call.duration_seconds, 240)
            self.assertEqual(call.last_event_at, datetime.utcfromtimestamp(1_700_000_240))

    def test_reused_event_id_with_different_facts_is_rejected(self):
        with self.app.app_context():
            self.persist_verified_telephony_event(
                db_session=self.db.session,
                event=self._event(event_id="event-same"),
                raw_body=b"first",
                fingerprint_secret="test-telephony-fingerprint-secret",
            )
            with self.assertRaises(self.VoiceCallSessionError):
                self.persist_verified_telephony_event(
                    db_session=self.db.session,
                    event=self._event(
                        event_id="event-same",
                        event_type="call.completed",
                        occurred_at=1_700_000_240,
                    ),
                    raw_body=b"different",
                    fingerprint_secret="test-telephony-fingerprint-secret",
                )
            self.assertEqual(self.app_module.VoiceCallEvent.query.count(), 1)

    def test_context_rejects_patient_data_and_accepts_only_allow_list(self):
        accepted = self.safe_call_context(
            {
                "selected_date": "2026-08-24",
                "preferred_period": "evening",
                "human_handoff_requested": False,
            }
        )
        self.assertEqual(json.loads(accepted)["selected_date"], "2026-08-24")
        with self.assertRaises(self.VoiceCallSessionError):
            self.safe_call_context({"patient_name": "Asha Sharma"})
        with self.assertRaises(self.VoiceCallSessionError):
            self.safe_call_context({"otp": "123456"})

    def test_generic_signed_webhook_rejects_extra_sensitive_payload_and_deduplicates(self):
        secret = "staging-telephony-webhook-secret"
        os.environ["VOICE_TELEPHONY_ENABLED"] = "1"
        os.environ["VOICE_TELEPHONY_PROVIDER"] = "staging-carrier"
        os.environ["VOICE_TELEPHONY_WEBHOOK_SECRET"] = secret
        os.environ["VOICE_TELEPHONY_FINGERPRINT_SECRET"] = "staging-telephony-fingerprint-secret"
        path = "/telephony/staging-carrier/inbound"
        now_epoch = int(time.time())
        body = json.dumps(
            {
                "call_id": "call-webhook-1",
                "event_id": "event-webhook-1",
                "event_type": "call.answered",
                "caller": "+919876543210",
                "occurred_at": now_epoch,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        signature = self.sign_hmac_request(
            secret=secret,
            method="POST",
            request_target=path,
            timestamp=now_epoch,
            body=body,
        ).value
        client = self.app.test_client()
        headers = {
            "Content-Type": "application/json",
            "X-Clinic-Timestamp": str(now_epoch),
            "X-Clinic-Signature": signature,
        }
        response = client.post(path, data=body, headers=headers)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json["accepted"], True)
        self.assertEqual(response.json["duplicate"], False)

        retry = client.post(path, data=body, headers=headers)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json["duplicate"], True)

        conflicting_payload = json.loads(body.decode("utf-8"))
        conflicting_payload["event_type"] = "call.completed"
        conflicting_body = json.dumps(conflicting_payload, separators=(",", ":")).encode("utf-8")
        conflicting_signature = self.sign_hmac_request(
            secret=secret,
            method="POST",
            request_target=path,
            timestamp=now_epoch,
            body=conflicting_body,
        ).value
        conflict = client.post(
            path,
            data=conflicting_body,
            headers={**headers, "X-Clinic-Signature": conflicting_signature},
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json["error"], "event_conflict")

        old_payload = json.loads(body.decode("utf-8"))
        old_payload["event_id"] = "event-webhook-old"
        old_payload["occurred_at"] = now_epoch - 3_601
        old_body = json.dumps(old_payload, separators=(",", ":")).encode("utf-8")
        old_signature = self.sign_hmac_request(
            secret=secret,
            method="POST",
            request_target=path,
            timestamp=now_epoch,
            body=old_body,
        ).value
        old_event = client.post(
            path,
            data=old_body,
            headers={**headers, "X-Clinic-Signature": old_signature},
        )
        self.assertEqual(old_event.status_code, 400)
        self.assertEqual(old_event.json["error"], "invalid_event")

        payload_with_transcript = json.loads(body.decode("utf-8"))
        payload_with_transcript["transcript"] = "patient private speech"
        unsafe_body = json.dumps(payload_with_transcript, separators=(",", ":")).encode("utf-8")
        unsafe_signature = self.sign_hmac_request(
            secret=secret,
            method="POST",
            request_target=path,
            timestamp=now_epoch,
            body=unsafe_body,
        ).value
        rejected = client.post(
            path,
            data=unsafe_body,
            headers={**headers, "X-Clinic-Signature": unsafe_signature},
        )
        self.assertEqual(rejected.status_code, 400)
        self.assertEqual(rejected.json["error"], "invalid_event")
        with self.app.app_context():
            self.assertEqual(self.app_module.VoiceCall.query.count(), 1)
            self.assertEqual(self.app_module.VoiceCallEvent.query.count(), 1)

    def test_generic_staging_endpoint_is_disabled_in_production(self):
        secret = "staging-telephony-webhook-secret"
        os.environ["VOICE_TELEPHONY_ENABLED"] = "1"
        os.environ["VOICE_TELEPHONY_PROVIDER"] = "staging-carrier"
        os.environ["VOICE_TELEPHONY_WEBHOOK_SECRET"] = secret
        os.environ["VOICE_TELEPHONY_FINGERPRINT_SECRET"] = "staging-telephony-fingerprint-secret"
        path = "/telephony/staging-carrier/inbound"
        now_epoch = int(time.time())
        body = json.dumps(
            {
                "call_id": "call-production-gate",
                "event_id": "event-production-gate",
                "event_type": "call.answered",
                "caller": "+919876543210",
                "occurred_at": now_epoch,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        signature = self.sign_hmac_request(
            secret=secret,
            method="POST",
            request_target=path,
            timestamp=now_epoch,
            body=body,
        ).value
        previous_app_env = self.app.config["APP_ENV"]
        self.app.config["APP_ENV"] = "production"
        try:
            response = self.app.test_client().post(
                path,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Clinic-Timestamp": str(now_epoch),
                    "X-Clinic-Signature": signature,
                },
            )
        finally:
            self.app.config["APP_ENV"] = previous_app_env
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
