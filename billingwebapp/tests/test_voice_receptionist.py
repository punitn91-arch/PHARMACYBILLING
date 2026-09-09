import base64
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from services.voice_receptionist import (
    AppointmentAvailability,
    ConfirmedVoiceBooking,
    PendingVoiceBooking,
    VoiceReceptionist,
)
from voice_receptionist import (
    SARVAM_STT_ENDPOINT,
    SARVAM_TTS_ENDPOINT,
    SARVAM_TTS_TIMEOUT_SECONDS,
    SarvamSpeechCache,
    SarvamSpeechToText,
    SarvamTextToSpeech,
    booking_stage_requires_private_input,
    get_sarvam_api_key,
    is_usable_voice_transcript,
    record_audio,
    recovery_language_hint,
    should_use_sarvam_public_stt,
    speak,
    transcribe_audio,
    transcribe_audio_compatibility_pass,
    transcribe_audio_with_sarvam,
)


class FakeGateway:
    def __init__(self, *, booking_open=True, normal_remaining=3, priority_available=False):
        self.requested_dates = []
        self.booking_open = booking_open
        self.normal_remaining = normal_remaining
        self.priority_available = priority_available

    def appointment_availability(self, appointment_date):
        self.requested_dates.append(appointment_date)
        return AppointmentAvailability(
            appointment_date=appointment_date,
            booking_open=self.booking_open,
            normal_remaining=self.normal_remaining,
            priority_remaining=2,
            priority_available=self.priority_available,
            arrival_window_label="5:30 PM to 7:45 PM",
            normal_fee=600,
            priority_fee=1000,
        )


class FakeBookingGateway:
    def __init__(self, *, ready=True):
        self.started = []
        self.verified = []
        self.resent = []
        self.cancelled = []
        self.ready = ready

    def voice_booking_ready(self):
        if self.ready:
            return True, ""
        return False, "SMS OTP delivery is not configured."

    def start_normal_booking(self, **details):
        self.started.append(details)
        return PendingVoiceBooking(
            booking_ref="private-test-reference",
            appointment_date=details["appointment_date"],
            arrival_window_label="5:30 PM to 7:45 PM",
        ), ""

    def verify_normal_booking_otp(self, *, booking_ref, mobile, otp_code):
        self.verified.append((booking_ref, mobile, otp_code))
        return ConfirmedVoiceBooking(
            appointment_date=date(2026, 8, 23),
            arrival_window_label="5:30 PM to 7:45 PM",
        ), ""

    def resend_normal_booking_otp(self, *, booking_ref, mobile):
        self.resent.append((booking_ref, mobile))
        return ""

    def cancel_pending_booking(self, *, booking_ref, mobile):
        self.cancelled.append((booking_ref, mobile))


class VoiceReceptionistTests(unittest.TestCase):
    def setUp(self):
        self.gateway = FakeGateway()
        self.assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
        )

    def test_doctor_question_does_not_turn_into_a_date_answer(self):
        reply = self.assistant.answer("Doctor sahab aaj clinic mein hain kya", "hi")

        self.assertEqual(reply.intent, "doctor_schedule")
        self.assertIn("22-08-2026", reply.text)
        self.assertIn("5:30 PM to 7:45 PM", reply.text)

    def test_tomorrow_appointment_uses_live_availability_gateway(self):
        reply = self.assistant.answer("Kal appointment milega?", "hi")

        self.assertEqual(reply.intent, "appointment")
        self.assertEqual(self.gateway.requested_dates[-1], date(2026, 8, 23))
        self.assertIn("3", reply.text)

    def test_report_question_requires_otp_and_never_exposes_patient_data(self):
        reply = self.assistant.answer("Meri thyroid report aa gayi?", "hi")

        self.assertEqual(reply.intent, "lab_report")
        self.assertTrue(reply.requires_otp)
        self.assertIn("OTP", reply.text)

    def test_medicine_question_is_sent_to_clinician(self):
        reply = self.assistant.answer("Meri dawa change kar do", "hi")

        self.assertEqual(reply.intent, "medical_safety")
        self.assertIn("doctor", reply.text.lower())

    def test_voice_booking_collects_details_then_requires_private_otp(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        reply = assistant.answer("Kal appointment book kar do", "hi")
        self.assertEqual(reply.intent, "appointment_booking_ask_name")
        self.assertTrue(reply.redact_transcript)
        self.assertEqual(booking_gateway.started, [])

        reply = assistant.answer("Asha Sharma", "hi")
        self.assertEqual(reply.intent, "appointment_booking_ask_mobile")
        self.assertTrue(reply.redact_transcript)

        reply = assistant.answer("98 765 43210", "hi")
        self.assertEqual(reply.intent, "appointment_booking_ask_gender")
        self.assertTrue(reply.redact_transcript)

        reply = assistant.answer("female", "hi")
        self.assertEqual(reply.intent, "appointment_booking_review")
        self.assertNotIn("Asha Sharma", reply.text)
        self.assertNotIn("9876543210", reply.text)

        reply = assistant.answer("haan", "hi")
        self.assertEqual(reply.intent, "appointment_booking_otp_sent")
        self.assertTrue(reply.needs_private_otp)
        self.assertEqual(len(booking_gateway.started), 1)
        self.assertEqual(booking_gateway.started[0]["appointment_date"], date(2026, 8, 23))
        self.assertEqual(booking_gateway.started[0]["gender"], "FEMALE")

        spoken_otp_reply = assistant.answer("123456", "hi")
        self.assertEqual(spoken_otp_reply.intent, "appointment_otp_must_be_private")
        self.assertEqual(booking_gateway.verified, [])

        reply = assistant.submit_private_otp("123456", "hi")
        self.assertEqual(reply.intent, "appointment_confirmed")
        self.assertEqual(len(booking_gateway.verified), 1)
        self.assertFalse(assistant.is_private_booking_active())

    def test_booking_cancel_releases_only_the_pending_reservation(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        assistant.answer("Kal appointment book kar do", "hi")
        assistant.answer("Asha Sharma", "hi")
        assistant.answer("9876543210", "hi")
        assistant.answer("female", "hi")
        assistant.answer("haan", "hi")
        reply = assistant.submit_private_otp("cancel", "hi")

        self.assertEqual(reply.intent, "appointment_booking_cancelled")
        self.assertEqual(len(booking_gateway.cancelled), 1)
        self.assertFalse(assistant.is_private_booking_active())

    def test_full_regular_capacity_hands_off_priority_without_creating_booking(self):
        full_gateway = FakeGateway(normal_remaining=0, priority_available=True)
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            full_gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        reply = assistant.answer("Kal appointment book kar do", "hi")

        self.assertEqual(reply.intent, "appointment_booking_priority_handoff")
        self.assertEqual(booking_gateway.started, [])
        self.assertFalse(assistant.is_private_booking_active())

    def test_mobile_parser_accepts_country_prefix_and_devanagari_digits(self):
        self.assertEqual(
            VoiceReceptionist._extract_mobile_number("+91 98765 43210"),
            "9876543210",
        )
        self.assertEqual(
            VoiceReceptionist._extract_mobile_number("९८ ७६५ ४३२१०"),
            "9876543210",
        )

    def test_booking_date_parser_accepts_spoken_and_calendar_formats(self):
        self.assertEqual(self.assistant._extract_booking_date("कल"), date(2026, 8, 23))
        self.assertEqual(self.assistant._extract_booking_date("tomorrow"), date(2026, 8, 23))
        self.assertEqual(self.assistant._extract_booking_date("23 August"), date(2026, 8, 23))
        self.assertEqual(self.assistant._extract_booking_date("23-08-2026"), date(2026, 8, 23))
        self.assertEqual(self.assistant._extract_booking_date("24-08-2026i"), date(2026, 8, 24))
        self.assertEqual(self.assistant._extract_booking_date("२३ अगस्त"), date(2026, 8, 23))

    def test_doctor_schedule_accepts_common_whisper_hindi_spellings(self):
        reply = self.assistant.answer("डाक्टर साहेब कल क्लीनिक में हैं?", "en")

        self.assertEqual(reply.intent, "doctor_schedule")

    def test_doctor_schedule_accepts_short_and_misspelled_asr_variants(self):
        for transcript in ("डॉ साहब कल क्लिनिक में होंगे", "डाक्तर साहब कल क्लीनिक में बैठते हैं"):
            with self.subTest(transcript=transcript):
                reply = self.assistant.answer(transcript, "hi")
                self.assertEqual(reply.intent, "doctor_schedule")

    def test_gender_parser_accepts_common_hindi_hinglish_and_spaced_asr_variants(self):
        cases = {
            "female": "FEMALE",
            "फी मेल": "FEMALE",
            "fe male": "FEMALE",
            "महिला": "FEMALE",
            "औरत": "FEMALE",
            "लड़की": "FEMALE",
            "male": "MALE",
            "पुरुष": "MALE",
            "आदमी": "MALE",
            "other": "OTHER",
            "अन्य": "OTHER",
            "non-binary": "OTHER",
        }

        for spoken_value, expected_gender in cases.items():
            with self.subTest(spoken_value=spoken_value):
                self.assertEqual(VoiceReceptionist._extract_gender(spoken_value), expected_gender)

        # A gender must be spoken explicitly. In particular, do not turn a
        # dropped "फी" from a female ASR result into a male selection.
        self.assertEqual(VoiceReceptionist._extract_gender("मेल"), "")
        self.assertEqual(VoiceReceptionist._extract_gender("my email is private"), "")

    def test_booking_asr_context_is_stage_specific_and_never_contains_draft_data(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        assistant.answer("Appointment book kar do", "hi")
        self.assertIn("date", assistant.booking_transcription_context().lower())
        assistant.answer("Kal", "hi")
        self.assertIn("name", assistant.booking_transcription_context().lower())
        assistant.answer("Asha Sharma", "hi")
        self.assertIn("mobile", assistant.booking_transcription_context().lower())
        assistant.answer("9876543210", "hi")
        context = assistant.booking_transcription_context()
        self.assertIn("female", context.lower())
        self.assertNotIn("Asha", context)
        self.assertNotIn("9876543210", context)

    def test_booking_confirmation_requires_an_explicit_yes_and_accepts_asr_variants(self):
        for spoken_value in ("yes", "yes please", "हाँ जी", "येस", "haan bhej do", "send the otp"):
            with self.subTest(spoken_value=spoken_value):
                self.assertTrue(VoiceReceptionist._is_affirmative(spoken_value))

        # Do not trigger an OTP SMS from an ambiguous or negative short answer.
        for spoken_value in ("nahin", "what", "please", "do not send", "yes or no"):
            with self.subTest(spoken_value=spoken_value):
                self.assertFalse(VoiceReceptionist._is_affirmative(spoken_value))

    def test_booking_handles_spaced_gender_and_hindi_yes_after_language_misdetection(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        assistant.answer("Kal appointment book kar do", "hi")
        assistant.answer("Asha Sharma", "de")
        assistant.answer("9876543210", "ro")

        reply = assistant.answer("फी मेल", "ar")
        self.assertEqual(reply.intent, "appointment_booking_review")

        reply = assistant.answer("येस जी", "de")
        self.assertEqual(reply.intent, "appointment_booking_otp_sent")
        self.assertEqual(booking_gateway.started[0]["gender"], "FEMALE")

    def test_private_keyboard_fallback_uses_same_booking_validation_without_echoing_data(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        assistant.answer("Appointment book kar do", "en")
        self.assertEqual(
            assistant.private_booking_input_prompt(),
            "Private date (1=today, 2=tomorrow, or a date): ",
        )
        reply = assistant.answer_private_booking_input("2", "en")
        self.assertEqual(reply.intent, "appointment_booking_ask_name")

        self.assertEqual(assistant.private_booking_input_prompt(), "Private patient name: ")
        reply = assistant.answer_private_booking_input("Asha Sharma", "en")
        self.assertEqual(reply.intent, "appointment_booking_ask_mobile")
        reply = assistant.answer_private_booking_input("9876543210", "en")
        self.assertEqual(reply.intent, "appointment_booking_ask_gender")

        self.assertEqual(
            assistant.private_booking_input_prompt(),
            "Private gender (1=female, 2=male, 3=other): ",
        )
        reply = assistant.answer_private_booking_input("1", "en")
        self.assertEqual(reply.intent, "appointment_booking_review")
        self.assertNotIn("Asha Sharma", reply.text)
        self.assertNotIn("9876543210", reply.text)

        self.assertEqual(
            assistant.private_booking_input_prompt(),
            "Private choice (y=send SMS, c=cancel): ",
        )
        reply = assistant.answer_private_booking_input("y", "en")
        self.assertEqual(reply.intent, "appointment_booking_otp_sent")
        self.assertTrue(reply.needs_private_otp)
        self.assertEqual(len(booking_gateway.started), 1)

    def test_english_booking_request_uses_english_reply_even_with_hindi_whisper_hint(self):
        booking_gateway = FakeBookingGateway(ready=False)
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        reply = assistant.answer("I want to book an appointment.", "hi")

        self.assertEqual(reply.language, "en")
        self.assertEqual(reply.intent, "appointment_booking_not_ready")
        self.assertTrue(reply.text.startswith("Voice booking cannot start"))

    def test_booking_keeps_english_after_short_reply_language_misdetections(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        reply = assistant.answer("I want to book an appointment for tomorrow.", "en")
        self.assertEqual(reply.language, "en")
        self.assertEqual(assistant.booking_transcription_language_hint(), "en")
        reply = assistant.answer("Asha Sharma", "ro")
        self.assertEqual(reply.language, "en")
        reply = assistant.answer("9876543210", "de")
        self.assertEqual(reply.language, "en")
        reply = assistant.answer("फीमेल", "ar")
        self.assertEqual(reply.intent, "appointment_booking_review")
        self.assertEqual(reply.language, "en")

        reply = assistant.answer("येस", "hi")
        self.assertEqual(reply.intent, "appointment_booking_otp_sent")
        self.assertEqual(reply.language, "en")
        self.assertTrue(reply.needs_private_otp)

    def test_sarvam_public_stt_is_blocked_for_every_booking_stage(self):
        booking_gateway = FakeBookingGateway()
        assistant = VoiceReceptionist(
            self.gateway,
            today_provider=lambda: date(2026, 8, 22),
            booking_gateway=booking_gateway,
            allow_booking=True,
        )

        self.assertTrue(should_use_sarvam_public_stt(assistant))
        assistant.answer("I want to book an appointment.", "en")
        self.assertFalse(should_use_sarvam_public_stt(assistant))
        # Date is harmless to say aloud, but it must remain on-device once a
        # booking has started. Only identifiers require hidden keyboard input.
        self.assertFalse(booking_stage_requires_private_input(assistant))

        assistant.answer_private_booking_input("2", "en")
        self.assertTrue(booking_stage_requires_private_input(assistant))
        assistant.answer_private_booking_input("Asha Sharma", "en")
        self.assertTrue(booking_stage_requires_private_input(assistant))
        assistant.answer_private_booking_input("9876543210", "en")
        self.assertFalse(booking_stage_requires_private_input(assistant))
        assistant.answer_private_booking_input("1", "en")
        self.assertFalse(booking_stage_requires_private_input(assistant))
        reply = assistant.answer_private_booking_input("y", "en")

        self.assertEqual(reply.intent, "appointment_booking_otp_sent")
        self.assertFalse(booking_stage_requires_private_input(assistant))
        self.assertFalse(should_use_sarvam_public_stt(assistant))


class FakeSarvamResponse:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.body


class SarvamTextToSpeechTests(unittest.TestCase):
    def test_reads_key_from_the_project_dotenv_file_without_needing_shell_export(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_directory = Path(temporary_directory)
            (project_directory / ".env").write_text(
                "OTHER_SETTING=value\nSARVAM_API_KEY=local-test-key\n",
                encoding="utf-8",
            )

            key = get_sarvam_api_key(project_directory=project_directory, environ={})

        self.assertEqual(key, "local-test-key")

    def test_uses_bulbul_v3_hindi_request_and_decodes_wav_audio(self):
        seen = {}
        expected_audio = b"RIFFtest-wav-audio"

        def opener(request, timeout):
            seen["url"] = request.full_url
            seen["timeout"] = timeout
            seen["headers"] = dict(request.header_items())
            seen["payload"] = json.loads(request.data.decode("utf-8"))
            response_body = json.dumps(
                {"audios": [base64.b64encode(expected_audio).decode("ascii")]}
            ).encode("utf-8")
            return FakeSarvamResponse(response_body)

        audio = SarvamTextToSpeech("test-key", request_opener=opener).synthesize(
            "नमस्ते, clinic में आपका स्वागत है।",
            "hi",
            speaker="priya",
            pace=1.0,
        )

        self.assertEqual(audio, expected_audio)
        self.assertEqual(seen["url"], SARVAM_TTS_ENDPOINT)
        self.assertEqual(seen["timeout"], SARVAM_TTS_TIMEOUT_SECONDS)
        self.assertEqual(seen["headers"]["Api-subscription-key"], "test-key")
        self.assertEqual(seen["payload"]["language_code"], "hi-IN")
        self.assertEqual(seen["payload"]["model"], "bulbul:v3")
        self.assertEqual(seen["payload"]["speaker"], "priya")
        self.assertEqual(seen["payload"]["speech_sample_rate"], 24000)


class SarvamSpeechToTextTests(unittest.TestCase):
    def test_uses_saaras_codemix_multipart_request_without_putting_key_in_body(self):
        seen = {}

        def opener(request, timeout):
            seen["url"] = request.full_url
            seen["timeout"] = timeout
            seen["headers"] = dict(request.header_items())
            seen["body"] = request.data
            return FakeSarvamResponse(
                json.dumps(
                    {
                        "transcript": "डॉक्टर साहब कल clinic में हैं?",
                        "language_code": "hi-IN",
                    }
                ).encode("utf-8")
            )

        transcript, language = SarvamSpeechToText("test-key", request_opener=opener).transcribe_wav_bytes(
            b"RIFFpublic-wav-data",
            None,
        )

        self.assertEqual(transcript, "डॉक्टर साहब कल clinic में हैं?")
        self.assertEqual(language, "hi")
        self.assertEqual(seen["url"], SARVAM_STT_ENDPOINT)
        self.assertEqual(seen["timeout"], 15)
        self.assertEqual(seen["headers"]["Api-subscription-key"], "test-key")
        self.assertIn(b'name="model"', seen["body"])
        self.assertIn(b"saaras:v3", seen["body"])
        self.assertIn(b"codemix", seen["body"])
        self.assertIn(b"unknown", seen["body"])
        self.assertIn(b"clinic_public_voice.wav", seen["body"])
        self.assertIn(b"RIFFpublic-wav-data", seen["body"])
        self.assertNotIn(b"test-key", seen["body"])

    def test_public_audio_is_encoded_in_memory_before_upload(self):
        seen = {}

        class FakeClient:
            def transcribe_wav_bytes(self, wav_bytes, language_hint):
                seen["wav"] = wav_bytes
                seen["hint"] = language_hint
                return "doctor sahib", "en"

        transcript, language = transcribe_audio_with_sarvam(
            np,
            FakeClient(),
            np.ones(1600, dtype=np.int16),
            sample_rate=16000,
            language_hint="en",
        )

        self.assertEqual((transcript, language), ("doctor sahib", "en"))
        self.assertEqual(seen["hint"], "en")
        self.assertTrue(seen["wav"].startswith(b"RIFF"))
        self.assertEqual(seen["wav"][8:12], b"WAVE")


class _FakeAudioStream:
    def __init__(self, blocks):
        self.blocks = iter(blocks)
        self.read_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self, frames):
        self.read_count += 1
        try:
            block = next(self.blocks)
        except StopIteration:
            block = np.zeros((frames, 1), dtype=np.float32)
        return block, False


class _FakeSoundDevice:
    class PortAudioError(Exception):
        pass

    def __init__(self, blocks):
        self.blocks = tuple(blocks)
        self.recording_kwargs = None
        self.wait_called = False

    def check_input_settings(self, **_kwargs):
        return None

    def rec(self, frames, **kwargs):
        self.recording_kwargs = {"frames": frames, **kwargs}
        if self.blocks:
            clip = np.concatenate(self.blocks, axis=0)
        else:
            clip = np.zeros((frames, 1), dtype=np.int16)
        if clip.shape[0] < frames:
            clip = np.pad(clip, ((0, frames - clip.shape[0]), (0, 0)))
        return clip[:frames].astype(np.int16)

    def wait(self):
        self.wait_called = True


class _FakeInputStream:
    def __init__(self, blocks):
        self.blocks = tuple(blocks)
        self.position = 0

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def read(self, frames):
        if self.position < len(self.blocks):
            block = self.blocks[self.position]
            self.position += 1
        else:
            block = np.zeros((frames, 1), dtype=np.int16)
        if block.shape[0] < frames:
            block = np.pad(block, ((0, frames - block.shape[0]), (0, 0)))
        return block[:frames].astype(np.int16), False


class _FakeStreamingSoundDevice(_FakeSoundDevice):
    def __init__(self, blocks):
        super().__init__(blocks=())
        self.stream_blocks = tuple(blocks)
        self.stream_kwargs = None

    def InputStream(self, **kwargs):
        self.stream_kwargs = kwargs
        return _FakeInputStream(self.stream_blocks)


class _FakeWhisperSegment:
    def __init__(self, text):
        self.text = text


class _FakeWhisperModel:
    def __init__(self):
        self.audio = None
        self.options = None

    def transcribe(self, audio, **options):
        self.audio = audio
        self.options = options
        return iter([_FakeWhisperSegment(" hello clinic ")]), SimpleNamespace(language="en")


class VoiceRuntimePerformanceTests(unittest.TestCase):
    @staticmethod
    def _block(value, frames=1600):
        return np.full((frames, 1), value, dtype=np.float32)

    def test_recording_uses_a_complete_signed_16_bit_wav_window(self):
        sounddevice = _FakeSoundDevice(
            [
                self._block(0.10, frames=1600),
                self._block(0.20, frames=1600),
            ]
        )

        audio = record_audio(
            np,
            sounddevice,
            device=0,
            sample_rate=16000,
            duration=0.20,
        )

        self.assertEqual(audio.dtype, np.int16)
        self.assertEqual(audio.size, 3200)
        self.assertTrue(sounddevice.wait_called)
        self.assertEqual(sounddevice.recording_kwargs["frames"], 3200)
        self.assertEqual(sounddevice.recording_kwargs["samplerate"], 16000)
        self.assertEqual(sounddevice.recording_kwargs["dtype"], "int16")

    def test_fast_recording_stops_after_speech_and_trailing_silence(self):
        stream_blocks = [
            np.zeros((50, 1), dtype=np.int16),
            np.full((50, 1), 2_000, dtype=np.int16),
            np.full((50, 1), 2_000, dtype=np.int16),
            np.zeros((50, 1), dtype=np.int16),
            np.zeros((50, 1), dtype=np.int16),
            np.zeros((50, 1), dtype=np.int16),
        ]
        sounddevice = _FakeStreamingSoundDevice(stream_blocks)

        audio = record_audio(
            np,
            sounddevice,
            device=0,
            sample_rate=1000,
            duration=1.0,
            stop_on_silence=True,
            silence_seconds=0.25,
            speech_threshold=500,
            minimum_recording_seconds=0.25,
        )

        self.assertEqual(audio.dtype, np.int16)
        self.assertEqual(audio.size, 448)
        self.assertLess(audio.size, 1000)
        self.assertEqual(sounddevice.stream_kwargs["dtype"], "int16")
        self.assertEqual(sounddevice.stream_kwargs["blocksize"], 64)

    def test_fast_recording_falls_back_to_reliable_fixed_window_when_streaming_is_missing(self):
        sounddevice = _FakeSoundDevice([self._block(0.20, frames=1600)])

        audio = record_audio(
            np,
            sounddevice,
            device=0,
            sample_rate=16000,
            duration=1.0,
            stop_on_silence=True,
        )

        self.assertEqual(audio.size, 16000)
        self.assertTrue(sounddevice.wait_called)

    def test_transcription_uses_a_wav_file_and_reliable_decode_options(self):
        model = _FakeWhisperModel()

        transcript, language = transcribe_audio(
            np,
            model,
            np.ones(1600, dtype=np.float32),
            sample_rate=16000,
            language_hint="en",
        )

        self.assertEqual(transcript, "hello clinic")
        self.assertEqual(language, "en")
        self.assertIsInstance(model.audio, str)
        self.assertTrue(model.audio.endswith(".wav"))
        self.assertEqual(model.options["beam_size"], 5)
        self.assertTrue(model.options["vad_filter"])

    def test_compatibility_recovery_matches_the_known_good_small_wav_shape(self):
        model = _FakeWhisperModel()

        transcript, language = transcribe_audio_compatibility_pass(
            np,
            model,
            np.ones(1600, dtype=np.int16),
            sample_rate=16000,
            language_hint="hi",
        )

        self.assertEqual(transcript, "hello clinic")
        self.assertEqual(language, "en")
        self.assertIsInstance(model.audio, str)
        self.assertTrue(model.audio.endswith(".wav"))
        self.assertEqual(model.options, {"language": "hi"})

    def test_recovery_hint_preserves_explicit_language_or_detects_devanagari(self):
        self.assertEqual(recovery_language_hint("en", "डॉक्टर साहब"), "en")
        self.assertEqual(recovery_language_hint(None, "डॉक्टर साहब"), "hi")
        self.assertIsNone(recovery_language_hint(None, "Doctor sahab kal clinic"))

    def test_transcript_gate_preserves_short_booking_answers_and_private_values(self):
        # The gate must never discard the practical one-word answers that make
        # a voice booking usable, nor a normal name/mobile value.
        for transcript in (
            "कल",
            "yes",
            "female",
            "महिला",
            "23 August",
            "9876543210",
            "Asha Sharma",
        ):
            with self.subTest(transcript=transcript):
                self.assertTrue(is_usable_voice_transcript(transcript))

    def test_transcript_gate_blocks_clear_whisper_repetition_and_garbage(self):
        # These are strong hallucination signatures, not ordinary accented
        # Hindi/English phrases.  They must not reach VoiceReceptionist.
        for transcript in (
            "भूत भूत भूत भूत",
            "पनजा पनजा पनजा पनजा",
            "आज आज",
            "aaaaaa",
            "%%%",
            "yes no yes no yes no",
        ):
            with self.subTest(transcript=transcript):
                self.assertFalse(is_usable_voice_transcript(transcript))

    def test_sarvam_cache_reuses_only_explicitly_cacheable_prompt_audio(self):
        cache = SarvamSpeechCache()
        with patch("voice_receptionist.get_sarvam_api_key", return_value="test-key"), patch(
            "voice_receptionist.SarvamTextToSpeech"
        ) as text_to_speech, patch("voice_receptionist.play_wav_audio"):
            text_to_speech.return_value.synthesize.return_value = b"wav-audio"

            for _ in range(2):
                speak(
                    "Please say male, female, or other.",
                    "en",
                    False,
                    "sarvam",
                    "priya",
                    1.08,
                    speech_cache=cache,
                    cacheable=True,
                )
            self.assertEqual(text_to_speech.return_value.synthesize.call_count, 1)

            speak(
                "A dynamic reply is never cached by default.",
                "en",
                False,
                "sarvam",
                "priya",
                1.08,
                speech_cache=cache,
                cacheable=False,
            )
            self.assertEqual(text_to_speech.return_value.synthesize.call_count, 2)


if __name__ == "__main__":
    unittest.main()
