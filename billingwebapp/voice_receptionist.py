#!/usr/bin/env python3
"""Run a local Hindi/English voice receptionist against the Flask clinic app.

The console client reads public booking capacity through the application's own
functions. Optional normal-appointment booking reuses the existing SMS OTP,
capacity-locking, and confirmation functions; it never bypasses those flows.
"""

from __future__ import annotations

import argparse
import base64
from collections import OrderedDict
import getpass
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unicodedata
import urllib.error
import urllib.request
import uuid
import wave
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from services.voice_receptionist import (
    AppointmentAvailability,
    ConfirmedVoiceBooking,
    PendingVoiceBooking,
    VoiceReceptionist,
)


CLINIC_TRANSCRIPTION_PROMPT = (
    "यह क्लिनिक रिसेप्शन की हिंदी और हिंग्लिश बातचीत है। "
    "डॉक्टर साहब, Doctor sahib, clinic, hospital, appointment, report, फीस, "
    "आज clinic में हैं क्या, कितने बजे बैठेंगे, available हैं क्या, appointment book करना है, "
    "patient name, mobile number, male, female, other, हाँ, cancel, आज, कल, तारीख़, "
    "today, tomorrow, date, 23 August, 23-08-2026।"
)
CLINIC_HOTWORDS = (
    "Doctor, doctor sahib, Dr. Sahab, clinic, hospital, appointment, booking, "
    "mobile number, male, female, report, today, tomorrow, aaj, kal, आज, कल, तारीख़, "
    "date, August, अगस्त"
)
SARVAM_TTS_ENDPOINT = "https://api.sarvam.ai/text-to-speech"
SARVAM_TTS_MAX_CHARACTERS = 2500
SARVAM_TTS_TIMEOUT_SECONDS = 10
SARVAM_STT_ENDPOINT = "https://api.sarvam.ai/speech-to-text"
SARVAM_STT_TIMEOUT_SECONDS = 15
WHISPER_SAMPLE_RATE = 16000
DEFAULT_MAX_RECORDING_SECONDS = 4.0
DEFAULT_WHISPER_BEAM_SIZE = 5
DEFAULT_RECOVERY_WHISPER_MODEL = "small"
DEFAULT_EARLY_SILENCE_SECONDS = 0.85
DEFAULT_FAST_SPEECH_THRESHOLD = 350
DEFAULT_FAST_MIN_RECORDING_SECONDS = 0.80
FAST_AUDIO_BLOCK_SECONDS = 0.05

# These hints are static by design: no patient detail, OTP, phone number, or
# prior transcript is ever sent back to Whisper as a prompt.
BOOKING_TRANSCRIPTION_CONTEXTS = {
    "ask_date": (
        "Clinic appointment booking. The caller is giving a date. Likely words: "
        "today, tomorrow, aaj, kal, आज, कल, 23 August, 23-08-2026, २३ अगस्त.",
        "today, tomorrow, aaj, kal, आज, कल, date, तारीख, August, अगस्त",
    ),
    "ask_name": (
        "Clinic appointment booking. The caller is saying the patient's full name. "
        "Transcribe the name exactly.",
        "patient name, full name, नाम",
    ),
    "ask_mobile": (
        "Clinic appointment booking. The caller is saying a ten digit Indian mobile number. "
        "Transcribe number words as digits when possible.",
        "mobile number, phone number, zero one two three four five six seven eight nine, mobile number",
    ),
    "ask_gender": (
        "Clinic appointment booking. The caller will say only one gender: male, female, or other. "
        "Hindi variants include male, female, अन्य, पुरुष, महिला, फीमेल.",
        "male, female, femail, other, महिला, पुरुष, फीमेल, अन्य",
    ),
    "review": (
        "Clinic appointment booking confirmation. The caller may say yes, haan, confirm, no, "
        "change date, change name, change mobile, change gender, or cancel.",
        "yes, haan, confirm, no, cancel, change date, change name, change mobile, change gender",
    ),
}

# Only these fixed, non-personal prompts may remain in the in-memory Sarvam
# cache. Booking names, mobile numbers, OTPs, dates, and all other dynamic
# replies are deliberately excluded.
SAFE_TTS_CACHE_INTENTS = frozenset(
    {
        "appointment_booking_ask_date",
        "appointment_booking_date_retry",
        "appointment_booking_name_retry",
        "appointment_booking_ask_mobile",
        "appointment_booking_mobile_retry",
        "appointment_booking_ask_gender",
        "appointment_booking_gender_retry",
        "appointment_booking_review_retry",
    }
)
PRIVATE_BOOKING_FALLBACK_COMMANDS = frozenset({"t", "type", "private"})
PRIVATE_BOOKING_RETRY_INTENTS = frozenset(
    {
        "appointment_booking_date_retry",
        "appointment_booking_name_retry",
        "appointment_booking_mobile_retry",
        "appointment_booking_gender_retry",
        "appointment_booking_review_retry",
    }
)
READ_ONLY_RECOVERY_INTENTS = frozenset(
    {
        "doctor_schedule",
        "appointment",
        "appointment_unavailable",
        "fees",
        "lab_report",
        "medical_safety",
        "clinic_time",
        "greeting",
    }
)

# A deliberately small set of one-word answers that are both common and safe
# to accept as-is.  In particular, do not discard a genuine short answer just
# because it has very few tokens: the booking flow needs values such as
# ``कल``, ``yes``, and ``female``.
SHORT_VALID_VOICE_ANSWERS = frozenset(
    {
        "aaj",
        "kal",
        "today",
        "tomorrow",
        "yes",
        "no",
        "haan",
        "ha",
        "nahin",
        "cancel",
        "male",
        "female",
        "other",
        "femail",
        "feemale",
        "येस",
        "हाँ",
        "हां",
        "हा",
        "नहीं",
        "नहि",
        "आज",
        "कल",
        "महिला",
        "पुरुष",
        "अन्य",
        "फीमेल",
        "फिमेल",
        "मेल",
        "रद्द",
    }
)
_VOICE_TRANSCRIPT_TOKEN_RE = re.compile(r"[0-9A-Za-z\u0900-\u097F]+")


def _voice_transcript_tokens(transcript: str) -> Tuple[str, ...]:
    """Return privacy-safe, punctuation-free tokens for a quality check.

    This is intentionally *not* a language detector or a dictionary.  Both
    would be too brittle for a caller's name, an Indian mobile number, or a
    short Hindi/Hinglish answer.  It only gives the gate enough structure to
    spot recognizer loops such as ``भूत भूत भूत भूत``.
    """
    normalized = unicodedata.normalize("NFKC", transcript or "").casefold()
    return tuple(_VOICE_TRANSCRIPT_TOKEN_RE.findall(normalized))


def _has_long_letter_run(transcript: str) -> bool:
    """Detect a clear character-loop hallucination without rejecting digits."""
    previous = ""
    run_length = 0
    for character in unicodedata.normalize("NFKC", transcript or "").casefold():
        if not character.isalpha():
            previous = ""
            run_length = 0
            continue
        if character == previous:
            run_length += 1
        else:
            previous = character
            run_length = 1
        # Six identical letters in a row is not a normal Hindi/English word,
        # while a repeated digit must remain allowed for a mobile number.
        if run_length >= 6:
            return True
    return False


def _has_repeated_transcript_loop(tokens: Tuple[str, ...]) -> bool:
    """Recognize only strong adjacent ASR repetition patterns.

    The threshold is deliberately conservative: normal phrases, names, and
    phone numbers are accepted.  A duplicated one-word control answer (for
    example ``आज आज``) is rejected because accepting it can silently choose a
    date or send an OTP after a noisy recording.  The caller can immediately
    say the answer again, or use the hidden ``t`` fallback during booking.
    """
    if len(tokens) < 2:
        return False

    if len(tokens) == 2 and tokens[0] == tokens[1] and tokens[0] in SHORT_VALID_VOICE_ANSWERS:
        return True

    # A word repeated three times in a row, or a two/three-word phrase looped
    # three times, is the pattern produced by the low-quality recordings in
    # the console screenshot.  It is not a meaningful appointment answer.
    max_width = min(3, len(tokens) // 3)
    for width in range(1, max_width + 1):
        for start in range(0, len(tokens) - (width * 3) + 1):
            phrase = tokens[start : start + width]
            if phrase == tokens[start + width : start + (width * 2)] and phrase == tokens[
                start + (width * 2) : start + (width * 3)
            ]:
                return True

    # Catch a longer answer where a hallucinated word is interspersed with
    # noise instead of appearing strictly adjacent.
    if len(tokens) >= 6:
        most_common_count = max(tokens.count(token) for token in set(tokens))
        if most_common_count >= 4 and most_common_count * 2 >= len(tokens):
            return True
    return False


def is_usable_voice_transcript(transcript: str) -> bool:
    """Return ``False`` only for clear ASR garbage before it reaches routing.

    This gate is intentionally narrow.  It does not try to judge whether an
    unfamiliar name or a Hindi/English sentence is "real" speech.  It merely
    blocks malformed text and strong repetition loops, which prevents a noisy
    microphone from accidentally becoming a booking date, gender, or OTP-SMS
    confirmation.
    """
    normalized = unicodedata.normalize("NFKC", transcript or "").strip()
    if not normalized or "\ufffd" in normalized:
        return False

    tokens = _voice_transcript_tokens(normalized)
    if not tokens:
        return False

    # A one-token answer is common in the booking flow.  Preserve it unless
    # it is an unmistakable character run; this includes kal/कल, yes/हाँ, and
    # female/महिला without maintaining a language-specific allow-list.
    if len(tokens) == 1:
        return not _has_long_letter_run(normalized)

    return not _has_long_letter_run(normalized) and not _has_repeated_transcript_loop(tokens)


class AudioSetupError(RuntimeError):
    """Raised when macOS cannot provide a usable microphone."""


class FastRecordingUnavailable(AudioSetupError):
    """The optional low-latency stream API is unavailable on this device."""


class SarvamTTSFailure(RuntimeError):
    """Raised when the optional Sarvam voice service cannot produce audio."""


class SarvamSTTFailure(RuntimeError):
    """Raised when Sarvam cannot transcribe one approved public voice clip."""


def _read_dotenv_values(path: Path) -> Dict[str, str]:
    """Read simple local .env values without printing any secret."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}

    values: Dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if separator and key and key.replace("_", "").isalnum():
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            values[key] = value
    return values


def _read_dotenv_value(path: Path, name: str) -> str:
    return _read_dotenv_values(path).get(name, "")


def load_project_environment(project_directory: Optional[Path] = None) -> None:
    """Make local .env values available to this CLI without overwriting the shell."""
    root = project_directory or Path(__file__).resolve().parent
    for name, value in _read_dotenv_values(root / ".env").items():
        os.environ.setdefault(name, value)


def get_sarvam_api_key(
    *,
    project_directory: Optional[Path] = None,
    environ: Optional[Dict[str, str]] = None,
) -> str:
    """Get the key from the shell first, then the local project .env file."""
    environment = os.environ if environ is None else environ
    key = (environment.get("SARVAM_API_KEY") or "").strip()
    if key:
        return key
    root = project_directory or Path(__file__).resolve().parent
    return _read_dotenv_value(root / ".env", "SARVAM_API_KEY")


class SarvamTextToSpeech:
    """Small dependency-free client for Bulbul v3 text-to-speech."""

    def __init__(
        self,
        api_key: str,
        *,
        request_opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        self.api_key = api_key.strip()
        self.request_opener = request_opener

    def synthesize(self, text: str, language: str, speaker: str, pace: float) -> bytes:
        if not self.api_key:
            raise SarvamTTSFailure("SARVAM_API_KEY is not configured")
        if not text.strip():
            raise SarvamTTSFailure("There is no text to speak")
        if len(text) > SARVAM_TTS_MAX_CHARACTERS:
            raise SarvamTTSFailure("The reply is too long for Sarvam text-to-speech")
        if not 0.5 <= pace <= 2.0:
            raise SarvamTTSFailure("Sarvam pace must be between 0.5 and 2.0")

        language_code = "en-IN" if language == "en" else "hi-IN"
        payload = {
            "text": text,
            "language_code": language_code,
            "model": "bulbul:v3",
            "speaker": speaker,
            "pace": pace,
            "speech_sample_rate": 24000,
        }
        request = urllib.request.Request(
            SARVAM_TTS_ENDPOINT,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "api-subscription-key": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            # A stalled voice request must not leave a caller waiting for half
            # a minute. On a transient network failure, the caller gets the
            # built-in Mac voice immediately after this bounded attempt.
            with self.request_opener(request, timeout=SARVAM_TTS_TIMEOUT_SECONDS) as response:
                body = response.read()
        except urllib.error.HTTPError as error:
            raise SarvamTTSFailure("Sarvam rejected the voice request (HTTP {})".format(error.code)) from error
        except urllib.error.URLError as error:
            raise SarvamTTSFailure("Could not reach Sarvam: {}".format(error.reason)) from error
        except OSError as error:
            raise SarvamTTSFailure("Could not contact Sarvam: {}".format(error)) from error

        try:
            data = json.loads(body.decode("utf-8"))
            encoded_audios = data["audios"]
            if not isinstance(encoded_audios, list) or not encoded_audios or not encoded_audios[0]:
                raise ValueError("No audio was returned")
            return base64.b64decode(encoded_audios[0], validate=True)
        except (KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
            raise SarvamTTSFailure("Sarvam returned an invalid audio response") from error


class SarvamSpeechToText:
    """Sarvam Saaras client for explicitly approved public clinic audio.

    The launcher calls this only while the booking conversation is idle. Once
    a booking starts, audio stays on this Mac: date, gender, and confirmation
    use local recognition, while patient name, mobile number, and OTP use
    hidden local input. No booking audio can reach this client.
    """

    def __init__(
        self,
        api_key: str,
        *,
        request_opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        self.api_key = api_key.strip()
        self.request_opener = request_opener

    @staticmethod
    def _language_code(language_hint: Optional[str]) -> str:
        return {"hi": "hi-IN", "en": "en-IN"}.get(language_hint or "", "unknown")

    @staticmethod
    def _response_language(language_code: Any, language_hint: Optional[str]) -> str:
        code = str(language_code or "").casefold()
        if code.startswith("hi"):
            return "hi"
        if code.startswith("en"):
            return "en"
        return language_hint if language_hint in {"hi", "en"} else "hi"

    @staticmethod
    def _multipart_form(
        *,
        wav_bytes: bytes,
        filename: str,
        language_code: str,
    ) -> Tuple[bytes, str]:
        """Build a small multipart request without adding an HTTP package."""
        boundary = "----ClinicVoice{}".format(uuid.uuid4().hex)
        body = bytearray()

        def add_field(name: str, value: str) -> None:
            body.extend("--{}\r\n".format(boundary).encode("ascii"))
            body.extend(
                'Content-Disposition: form-data; name="{}"\r\n\r\n'.format(name).encode("utf-8")
            )
            body.extend(value.encode("utf-8"))
            body.extend(b"\r\n")

        add_field("model", "saaras:v3")
        add_field("mode", "codemix")
        add_field("language_code", language_code)
        body.extend("--{}\r\n".format(boundary).encode("ascii"))
        body.extend(
            (
                'Content-Disposition: form-data; name="file"; filename="{}"\r\n'
                "Content-Type: audio/wav\r\n\r\n"
            ).format(filename.replace('"', "")).encode("utf-8")
        )
        body.extend(wav_bytes)
        body.extend(b"\r\n--" + boundary.encode("ascii") + b"--\r\n")
        return bytes(body), boundary

    def transcribe_wav_bytes(self, wav_bytes: bytes, language_hint: Optional[str]) -> Tuple[str, str]:
        if not self.api_key:
            raise SarvamSTTFailure("SARVAM_API_KEY is not configured")
        if not wav_bytes:
            return "", self._response_language(None, language_hint)

        body, boundary = self._multipart_form(
            wav_bytes=wav_bytes,
            filename="clinic_public_voice.wav",
            language_code=self._language_code(language_hint),
        )
        request = urllib.request.Request(
            SARVAM_STT_ENDPOINT,
            data=body,
            headers={
                "api-subscription-key": self.api_key,
                "Content-Type": "multipart/form-data; boundary={}".format(boundary),
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with self.request_opener(request, timeout=SARVAM_STT_TIMEOUT_SECONDS) as response:
                response_body = response.read()
        except urllib.error.HTTPError as error:
            raise SarvamSTTFailure("Sarvam speech recognition rejected the request (HTTP {})".format(error.code)) from error
        except urllib.error.URLError as error:
            raise SarvamSTTFailure("Could not reach Sarvam speech recognition: {}".format(error.reason)) from error
        except OSError as error:
            raise SarvamSTTFailure("Could not contact Sarvam speech recognition") from error

        try:
            data = json.loads(response_body.decode("utf-8"))
            transcript = str(data.get("transcript") or "").strip()
            language = self._response_language(data.get("language_code"), language_hint)
        except (TypeError, ValueError, UnicodeDecodeError) as error:
            raise SarvamSTTFailure("Sarvam returned an invalid speech-recognition response") from error
        return transcript, language


class SarvamSpeechCache:
    """Small process-memory cache for explicitly safe, repeated prompt audio.

    The caller must opt in with ``cacheable=True``. This makes it impossible
    for booking details, OTP-related content, or other dynamic replies to be
    retained merely because they happen to repeat. The cache is never written
    to disk and vanishes when the voice process exits.
    """

    def __init__(self, max_entries: int = 12) -> None:
        self.max_entries = max(1, int(max_entries))
        self._audio: "OrderedDict[Tuple[str, str, str, float], bytes]" = OrderedDict()

    @staticmethod
    def _key(text: str, language: str, speaker: str, pace: float) -> Tuple[str, str, str, float]:
        return (text, language, speaker.strip().casefold(), round(float(pace), 3))

    def get(self, text: str, language: str, speaker: str, pace: float) -> Optional[bytes]:
        key = self._key(text, language, speaker, pace)
        audio = self._audio.get(key)
        if audio is not None:
            self._audio.move_to_end(key)
        return audio

    def put(self, text: str, language: str, speaker: str, pace: float, audio: bytes) -> None:
        if not audio:
            return
        key = self._key(text, language, speaker, pace)
        self._audio[key] = audio
        self._audio.move_to_end(key)
        while len(self._audio) > self.max_entries:
            self._audio.popitem(last=False)


def play_wav_audio(audio: bytes) -> None:
    """Play a temporary WAV response with the macOS built-in audio player."""
    temporary_file = tempfile.NamedTemporaryFile(prefix="clinic_sarvam_", suffix=".wav", delete=False)
    path = Path(temporary_file.name)
    try:
        temporary_file.write(audio)
        temporary_file.close()
        completed = subprocess.run(["afplay", str(path)], check=False)
        if completed.returncode != 0:
            raise SarvamTTSFailure("macOS could not play the Sarvam audio")
    except FileNotFoundError as error:
        raise SarvamTTSFailure("Sarvam audio playback is configured for macOS only") from error
    finally:
        if not temporary_file.closed:
            temporary_file.close()
        path.unlink(missing_ok=True)


class FlaskClinicVoiceGateway:
    """Narrow bridge to public availability and the existing OTP booking flow."""

    REAL_SMS_OTP_MODES = {
        "msg91",
        "msg91_sms",
        "sms",
        "twofactor",
        "twofactor_sms",
        "2factor",
        "2factor_sms",
        "twilio_whatsapp",
    }

    def __init__(self, app_module: Any) -> None:
        self.app_module = app_module

    def appointment_availability(self, appointment_date) -> AppointmentAvailability:
        with self.app_module.app.app_context():
            settings = self.app_module.get_public_appointment_booking_settings()
            data = self.app_module.get_public_appointment_availability(appointment_date, settings)
            return AppointmentAvailability(
                appointment_date=appointment_date,
                booking_open=bool(data.get("booking_open")),
                normal_remaining=int(data.get("normal_remaining") or 0),
                priority_remaining=int(data.get("priority_remaining") or 0),
                priority_available=bool(data.get("priority_available")),
                arrival_window_label=str(data.get("arrival_window_label") or ""),
                normal_fee=float(getattr(settings, "normal_fee", 0) or 0),
                priority_fee=float(getattr(settings, "priority_fee", 0) or 0),
                message=str(data.get("message") or ""),
            )

    def _voice_request_context(self):
        """Provide request metadata required by the existing public OTP helpers."""
        return self.app_module.app.test_request_context(
            "/voice-receptionist/booking",
            method="POST",
            headers={"User-Agent": "ClinicVoiceReceptionist/1.0"},
            environ_base={"REMOTE_ADDR": "127.0.0.1"},
        )

    @classmethod
    def _has_real_sms_otp_transport(cls) -> bool:
        mode = (os.environ.get("PUBLIC_PORTAL_OTP_MODE") or "").strip().lower()
        return mode in cls.REAL_SMS_OTP_MODES

    @staticmethod
    def _sms_configuration_error() -> str:
        return (
            "SMS OTP delivery is not configured. Please use the secure booking page "
            "or ask the clinic to configure SMS verification."
        )

    def voice_booking_ready(self) -> Tuple[bool, str]:
        if self._has_real_sms_otp_transport():
            return True, ""
        return False, self._sms_configuration_error()

    def start_normal_booking(
        self,
        *,
        patient_name: str,
        mobile: str,
        gender: str,
        appointment_date,
    ) -> Tuple[Optional[PendingVoiceBooking], str]:
        """Create an OTP_PENDING normal booking using the same public helpers."""
        ready, error = self.voice_booking_ready()
        if not ready:
            return None, error

        with self._voice_request_context():
            settings = self.app_module.get_public_appointment_booking_settings()
            form_data = self.app_module.public_appointment_form_data(
                {
                    "patient_name": patient_name,
                    "mobile": mobile,
                    "gender": gender,
                    "appointment_date": appointment_date.isoformat(),
                    "booking_type": "NORMAL",
                    "symptoms": "",
                }
            )
            availability = self.app_module.get_public_appointment_availability(appointment_date, settings)
            payload, error = self.app_module.validate_public_appointment_request(
                form_data,
                settings,
                availability,
            )
            if error:
                return None, error

            booking, error = self.app_module.create_public_appointment_reservation(payload, settings)
            if not booking:
                return None, error or "Unable to reserve an appointment right now."

            challenge, development_code, error = self.app_module.create_portal_otp_challenge(
                mobile=booking.mobile,
                purpose="PUBLIC_APPOINTMENT",
                context_ref=booking.booking_ref,
            )
            # Development codes must never be exposed by the voice channel.
            del development_code
            if not challenge:
                try:
                    self.app_module.db.session.delete(booking)
                    self.app_module.db.session.commit()
                except Exception:
                    self.app_module.db.session.rollback()
                return None, error or "Unable to send the verification code right now."

            arrival = self.app_module.public_appointment_arrival_window_context(settings)
            return PendingVoiceBooking(
                booking_ref=booking.booking_ref,
                appointment_date=appointment_date,
                arrival_window_label=str(arrival.get("arrival_window_label") or ""),
            ), ""

    def verify_normal_booking_otp(
        self,
        *,
        booking_ref: str,
        mobile: str,
        otp_code: str,
    ) -> Tuple[Optional[ConfirmedVoiceBooking], str]:
        """Reuse hash verification and capacity locking before creating an appointment."""
        with self._voice_request_context():
            booking = self.app_module.PublicAppointmentBooking.query.filter_by(booking_ref=booking_ref).first()
            normalized_mobile = self.app_module.normalize_portal_mobile(mobile)
            if not booking or booking.mobile != normalized_mobile:
                return None, "This verification request is no longer valid. Please start again."
            if (booking.status or "").upper() != "OTP_PENDING" or not self.app_module.public_appointment_booking_is_active(booking):
                if (booking.status or "").upper() == "OTP_PENDING":
                    booking.status = "EXPIRED"
                    self.app_module.db.session.commit()
                return None, "This verification request has expired. Please start again."

            _challenge, error = self.app_module.verify_portal_otp_challenge(
                mobile=booking.mobile,
                purpose="PUBLIC_APPOINTMENT",
                context_ref=booking.booking_ref,
                otp_code=otp_code,
            )
            if error:
                return None, error

            settings = self.app_module.get_public_appointment_booking_settings()
            appointment, error = self.app_module.confirm_public_normal_appointment(booking, settings)
            if error:
                fresh_booking = self.app_module.PublicAppointmentBooking.query.get(booking.id)
                if fresh_booking and fresh_booking.status == "OTP_PENDING":
                    fresh_booking.status = "EXPIRED"
                    self.app_module.db.session.commit()
                return None, error
            arrival = self.app_module.public_appointment_arrival_window_context(settings)
            return ConfirmedVoiceBooking(
                appointment_date=booking.appointment_date,
                arrival_window_label=str(arrival.get("arrival_window_label") or ""),
            ), ""

    def resend_normal_booking_otp(self, *, booking_ref: str, mobile: str) -> str:
        """Resend through the existing context-bound and rate-limited OTP helper."""
        if not self._has_real_sms_otp_transport():
            return self._sms_configuration_error()
        with self._voice_request_context():
            booking = self.app_module.PublicAppointmentBooking.query.filter_by(booking_ref=booking_ref).first()
            normalized_mobile = self.app_module.normalize_portal_mobile(mobile)
            if not booking or booking.mobile != normalized_mobile:
                return "This verification request is no longer valid. Please start again."
            if (booking.status or "").upper() != "OTP_PENDING" or not self.app_module.public_appointment_booking_is_active(booking):
                return "This verification request has expired. Please start again."
            challenge, development_code, error = self.app_module.create_portal_otp_challenge(
                mobile=booking.mobile,
                purpose="PUBLIC_APPOINTMENT",
                context_ref=booking.booking_ref,
            )
            del development_code
            if not challenge:
                return error or "Unable to resend the verification code right now."
            return ""

    def cancel_pending_booking(self, *, booking_ref: str, mobile: str) -> None:
        """Expire only this in-memory conversation's OTP_PENDING reservation."""
        with self._voice_request_context():
            booking = self.app_module.PublicAppointmentBooking.query.filter_by(booking_ref=booking_ref).first()
            normalized_mobile = self.app_module.normalize_portal_mobile(mobile)
            if not booking or booking.mobile != normalized_mobile:
                return
            if (booking.status or "").upper() == "OTP_PENDING":
                booking.status = "EXPIRED"
                booking.reservation_expires_at = self.app_module.datetime.utcnow()
                self.app_module.db.session.commit()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local voice receptionist connected to the Flask clinic app.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--text", help="Test one typed patient question without a microphone")
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_MAX_RECORDING_SECONDS,
        help="Maximum seconds to record after each prompt",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Stop recording shortly after you finish speaking when the microphone supports it; "
            "otherwise use the reliable full-duration recording"
        ),
    )
    parser.add_argument(
        "--silence-seconds",
        type=float,
        default=DEFAULT_EARLY_SILENCE_SECONDS,
        help="Trailing silence used by --fast before recognition starts",
    )
    parser.add_argument(
        "--model",
        default="base",
        help="Fast local Whisper model (used with local STT or if Sarvam is unavailable)",
    )
    parser.add_argument(
        "--stt-provider",
        choices=("local", "sarvam-public"),
        default="local",
        help=(
            "Speech recognition: local keeps audio on this Mac; sarvam-public sends only public questions "
            "to Sarvam and switches every booking detail to hidden local input"
        ),
    )
    parser.add_argument(
        "--beam-size",
        type=int,
        default=DEFAULT_WHISPER_BEAM_SIZE,
        help="Whisper decode candidates; 5 is the reliable default for Hindi/Hinglish speech",
    )
    parser.add_argument(
        "--recovery-model",
        default=DEFAULT_RECOVERY_WHISPER_MODEL,
        help=(
            "Local model used only when the fast model produces clear garbage or a generic reply; "
            "set to 'none' to disable this recovery"
        ),
    )
    parser.add_argument(
        "--language",
        choices=("auto", "hi", "en"),
        default="auto",
        help="Speech language hint; auto detects Hindi or English (use hi for a fixed Hindi hint)",
    )
    parser.add_argument("--device", type=int, help="Input device number; use --list-devices")
    parser.add_argument("--list-devices", action="store_true", help="Show microphones and exit")
    parser.add_argument("--no-speak", action="store_true", help="Print replies without Mac speech output")
    parser.add_argument(
        "--enable-booking",
        action="store_true",
        help=(
            "Enable normal appointment booking after voice confirmation. "
            "Requires a real SMS OTP provider; OTP is entered privately in the terminal."
        ),
    )
    parser.add_argument(
        "--voice-provider",
        choices=("auto", "sarvam", "macos"),
        default="auto",
        help="Voice output: auto uses Sarvam when SARVAM_API_KEY is configured",
    )
    parser.add_argument(
        "--sarvam-speaker",
        default="priya",
        help="Bulbul v3 speaker name for Sarvam output (default: priya)",
    )
    parser.add_argument(
        "--sarvam-pace",
        type=float,
        default=1.08,
        help="Sarvam speaking pace from 0.5 to 2.0",
    )
    return parser.parse_args()


def load_clinic_app() -> Any:
    load_project_environment()
    try:
        import app as clinic_app
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Could not load the Flask clinic app. Run this script from the folder containing app.py."
        ) from error
    return clinic_app


def load_audio_dependencies() -> Tuple[Any, Any, Any]:
    try:
        import numpy as np
        import sounddevice as sd
        from faster_whisper import WhisperModel
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing {}. Run: python -m pip install -r requirements-voice.txt".format(
                error.name or "a required voice package"
            )
        ) from error
    return np, sd, WhisperModel


def available_input_devices(sd: Any) -> Tuple[Tuple[int, Dict[str, Any]], ...]:
    devices = sd.query_devices()
    return tuple(
        (index, dict(device))
        for index, device in enumerate(devices)
        if device["max_input_channels"] > 0
    )


def print_input_devices(sd: Any) -> int:
    devices = available_input_devices(sd)
    if not devices:
        print("No microphone is visible to Python.")
        print_microphone_permission_help()
        return 1
    print("Available microphones:")
    for index, device in devices:
        print("  {}: {}".format(index, device["name"]))
    return 0


def print_microphone_permission_help() -> None:
    print(
        "\nOn macOS, open System Settings > Privacy & Security > Microphone, "
        "enable VS Code (or Terminal), then fully quit and reopen that app."
    )


def choose_input_device(sd: Any, requested_device: Optional[int]) -> Tuple[int, Dict[str, Any]]:
    devices = available_input_devices(sd)
    if not devices:
        raise AudioSetupError("No microphone is visible to Python.")
    if requested_device is not None:
        for index, device in devices:
            if index == requested_device:
                return index, device
        raise AudioSetupError(
            "Microphone {} is unavailable. Run --list-devices to see valid numbers.".format(
                requested_device
            )
        )
    try:
        default_device = int(sd.default.device[0])
    except (IndexError, TypeError, ValueError):
        default_device = -1
    for index, device in devices:
        if index == default_device:
            return index, device
    return devices[0]


def _record_fixed_audio(
    np: Any,
    sd: Any,
    device: int,
    sample_rate: int,
    duration: float,
) -> Any:
    """Capture a stable signed-16-bit WAV clip from the selected microphone.

    This deliberately uses the same proven `sounddevice.rec` + WAV path as
    the working microphone test. It is more reliable on the built-in macOS
    microphone than streaming a partially captured float array.
    """
    if duration <= 0:
        raise AudioSetupError("Recording duration must be positive.")

    try:
        sd.check_input_settings(device=device, samplerate=sample_rate, channels=1, dtype="int16")
        clip = sd.rec(
            int(round(sample_rate * duration)),
            samplerate=sample_rate,
            channels=1,
            device=device,
            dtype="int16",
        )
        sd.wait()
    except sd.PortAudioError as error:
        raise AudioSetupError("Could not record from the microphone: {}".format(error)) from error
    return np.squeeze(clip)


def _block_rms(np: Any, block: Any) -> float:
    """Return a stable amplitude estimate for one signed-16-bit audio block."""

    samples = np.asarray(block, dtype=np.float32)
    if not samples.size:
        return 0.0
    return float(np.sqrt(np.mean(np.square(samples))))


def record_audio_until_silence(
    np: Any,
    sd: Any,
    device: int,
    sample_rate: int,
    duration: float,
    *,
    silence_seconds: float = DEFAULT_EARLY_SILENCE_SECONDS,
    speech_threshold: int = DEFAULT_FAST_SPEECH_THRESHOLD,
    minimum_recording_seconds: float = DEFAULT_FAST_MIN_RECORDING_SECONDS,
) -> Any:
    """Record up to ``duration`` but stop soon after a caller finishes.

    This optional fast path is deliberately conservative: it first detects
    actual speech, retains a trailing silence buffer, and otherwise records
    the complete clip. That preserves the reliable fixed-WAV behaviour for
    quiet microphones while avoiding the old mandatory four-second wait after
    a normal short public question.
    """

    if duration <= 0:
        raise AudioSetupError("Recording duration must be positive.")
    if silence_seconds < 0.25 or silence_seconds > duration:
        raise AudioSetupError("Early-silence duration is invalid.")
    if minimum_recording_seconds <= 0 or minimum_recording_seconds > duration:
        raise AudioSetupError("Minimum recording duration is invalid.")
    if isinstance(speech_threshold, bool) or not isinstance(speech_threshold, int) or speech_threshold < 1:
        raise AudioSetupError("Speech threshold is invalid.")

    stream_factory = getattr(sd, "InputStream", None)
    if not callable(stream_factory):
        raise FastRecordingUnavailable("Low-latency microphone streaming is unavailable.")

    block_frames = max(64, int(round(sample_rate * FAST_AUDIO_BLOCK_SECONDS)))
    max_frames = max(1, int(round(sample_rate * duration)))
    minimum_frames = max(1, int(round(sample_rate * minimum_recording_seconds)))
    silence_frames_needed = max(1, int(round(sample_rate * silence_seconds)))
    chunks = []
    captured_frames = 0
    silent_frames = 0
    speech_detected = False

    try:
        sd.check_input_settings(device=device, samplerate=sample_rate, channels=1, dtype="int16")
        try:
            stream_context = stream_factory(
                samplerate=sample_rate,
                channels=1,
                device=device,
                dtype="int16",
                blocksize=block_frames,
                latency="low",
            )
        except (AttributeError, TypeError) as error:
            raise FastRecordingUnavailable("Low-latency microphone streaming is unavailable.") from error
        with stream_context as stream:
            while captured_frames < max_frames:
                frames_to_read = min(block_frames, max_frames - captured_frames)
                result = stream.read(frames_to_read)
                if not isinstance(result, tuple) or len(result) != 2:
                    raise FastRecordingUnavailable("Low-latency microphone streaming returned invalid audio.")
                block, _overflowed = result
                samples = np.asarray(block, dtype=np.int16)
                if samples.ndim == 1:
                    samples = samples.reshape((-1, 1))
                if samples.ndim != 2 or not samples.shape[0]:
                    raise FastRecordingUnavailable("Low-latency microphone streaming returned invalid audio.")
                chunks.append(samples.copy())
                captured_frames += samples.shape[0]

                if _block_rms(np, samples) >= speech_threshold:
                    speech_detected = True
                    silent_frames = 0
                elif speech_detected:
                    silent_frames += samples.shape[0]
                    if (
                        captured_frames >= minimum_frames
                        and silent_frames >= silence_frames_needed
                    ):
                        break
    except FastRecordingUnavailable:
        raise
    except sd.PortAudioError as error:
        raise AudioSetupError("Could not record from the microphone: {}".format(error)) from error
    except (AttributeError, TypeError, ValueError) as error:
        raise FastRecordingUnavailable("Low-latency microphone streaming is unavailable.") from error

    if not chunks:
        return np.zeros(0, dtype=np.int16)
    return np.squeeze(np.concatenate(chunks, axis=0)).astype(np.int16, copy=False)


def record_audio(
    np: Any,
    sd: Any,
    device: int,
    sample_rate: int,
    duration: float,
    *,
    stop_on_silence: bool = False,
    silence_seconds: float = DEFAULT_EARLY_SILENCE_SECONDS,
    speech_threshold: int = DEFAULT_FAST_SPEECH_THRESHOLD,
    minimum_recording_seconds: float = DEFAULT_FAST_MIN_RECORDING_SECONDS,
) -> Any:
    """Capture a stable signed-16-bit WAV clip, with an optional fast path."""

    if stop_on_silence:
        try:
            return record_audio_until_silence(
                np,
                sd,
                device,
                sample_rate,
                duration,
                silence_seconds=silence_seconds,
                speech_threshold=speech_threshold,
                minimum_recording_seconds=minimum_recording_seconds,
            )
        except FastRecordingUnavailable:
            # Keep the proven `sounddevice.rec` path as a safe fallback for
            # older drivers and microphone devices that cannot stream.
            pass
    return _record_fixed_audio(np, sd, device, sample_rate, duration)


def wav_audio_bytes(np: Any, sample_rate: int, audio: Any) -> bytes:
    """Encode one signed-16-bit mono WAV without retaining it on disk."""
    samples = np.asarray(audio)
    if not np.issubdtype(samples.dtype, np.integer):
        samples = np.clip(samples, -1.0, 1.0)
        samples = (samples * 32767).astype(np.int16)
    else:
        samples = samples.astype(np.int16, copy=False)
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(np.ascontiguousarray(samples).tobytes())
    return output.getvalue()


def write_audio_file(np: Any, path: Path, sample_rate: int, audio: Any) -> None:
    """Write one signed-16-bit mono WAV that faster-whisper can decode reliably."""
    path.write_bytes(wav_audio_bytes(np, sample_rate, audio))


def transcribe_audio(
    np: Any,
    model: Any,
    audio: Any,
    sample_rate: int,
    language_hint: Optional[str],
    *,
    initial_prompt: str = CLINIC_TRANSCRIPTION_PROMPT,
    hotwords: str = CLINIC_HOTWORDS,
    beam_size: int = DEFAULT_WHISPER_BEAM_SIZE,
) -> Tuple[str, str]:
    if not getattr(audio, "size", 0):
        return "", language_hint or "en"
    temporary_file = tempfile.NamedTemporaryFile(prefix="clinic_voice_", suffix=".wav", delete=False)
    path = Path(temporary_file.name)
    temporary_file.close()
    try:
        write_audio_file(np, path, sample_rate, audio)
        segments, info = model.transcribe(
            str(path),
            language=language_hint,
            task="transcribe",
            beam_size=max(1, beam_size),
            vad_filter=True,
            initial_prompt=initial_prompt,
            hotwords=hotwords,
        )
        return " ".join(segment.text.strip() for segment in segments).strip(), info.language or language_hint or "en"
    finally:
        path.unlink(missing_ok=True)


def transcribe_audio_with_sarvam(
    np: Any,
    client: SarvamSpeechToText,
    audio: Any,
    sample_rate: int,
    language_hint: Optional[str],
) -> Tuple[str, str]:
    """Upload one allowed public WAV directly from memory, not a temp file."""
    if not getattr(audio, "size", 0):
        return "", language_hint or "hi"
    return client.transcribe_wav_bytes(wav_audio_bytes(np, sample_rate, audio), language_hint)


def booking_stage_requires_private_input(receptionist: VoiceReceptionist) -> bool:
    """Keep patient identifiers local while allowing harmless voice choices."""
    stage = getattr(getattr(receptionist, "state", None), "booking_stage", None)
    return getattr(stage, "value", "") in {"ask_name", "ask_mobile"}


def should_use_sarvam_public_stt(receptionist: VoiceReceptionist) -> bool:
    """Cloud STT is allowed solely before any booking conversation begins."""
    return not receptionist.is_private_booking_active()


def transcribe_audio_compatibility_pass(
    np: Any,
    model: Any,
    audio: Any,
    sample_rate: int,
    language_hint: Optional[str],
) -> Tuple[str, str]:
    """Use the exact small-model WAV decode shape from the known-good mic test.

    The fast base model remains the default path.  This deliberately simpler
    second pass is only for a clip that failed the quality gate or routed to a
    generic response.  It avoids the long custom prompt/VAD configuration and
    gives the stronger local model a clean, independent interpretation.
    """
    if not getattr(audio, "size", 0):
        return "", language_hint or "en"
    temporary_file = tempfile.NamedTemporaryFile(prefix="clinic_voice_recovery_", suffix=".wav", delete=False)
    path = Path(temporary_file.name)
    temporary_file.close()
    try:
        write_audio_file(np, path, sample_rate, audio)
        options: Dict[str, Any] = {}
        if language_hint in {"hi", "en"}:
            options["language"] = language_hint
        segments, info = model.transcribe(str(path), **options)
        transcript = " ".join(segment.text.strip() for segment in segments).strip()
        return transcript, info.language or language_hint or "en"
    finally:
        path.unlink(missing_ok=True)


def recovery_language_hint(active_language_hint: Optional[str], transcript: str) -> Optional[str]:
    """Keep an explicit language lock, otherwise only lock clear Devanagari."""
    if active_language_hint in {"hi", "en"}:
        return active_language_hint
    if any("\u0900" <= character <= "\u097f" for character in transcript or ""):
        return "hi"
    return None


def transcription_context_for(receptionist: VoiceReceptionist) -> Tuple[str, str]:
    """Use a short, stage-specific Whisper hint for booking answers."""
    state = getattr(receptionist, "state", None)
    stage = getattr(getattr(state, "booking_stage", None), "value", "")
    return BOOKING_TRANSCRIPTION_CONTEXTS.get(stage, (CLINIC_TRANSCRIPTION_PROMPT, CLINIC_HOTWORDS))


def speak(
    text: str,
    language: str,
    disabled: bool,
    voice_provider: str,
    sarvam_speaker: str,
    sarvam_pace: float,
    *,
    speech_cache: Optional[SarvamSpeechCache] = None,
    cacheable: bool = False,
) -> None:
    if disabled:
        return

    if voice_provider != "macos":
        api_key = get_sarvam_api_key()
        if api_key:
            try:
                speaker = sarvam_speaker.strip().casefold()
                audio = (
                    speech_cache.get(text, language, speaker, sarvam_pace)
                    if cacheable and speech_cache is not None
                    else None
                )
                if audio is None:
                    audio = SarvamTextToSpeech(api_key).synthesize(
                        text,
                        language,
                        speaker=speaker,
                        pace=sarvam_pace,
                    )
                    if cacheable and speech_cache is not None:
                        speech_cache.put(text, language, speaker, sarvam_pace, audio)
                play_wav_audio(audio)
                return
            except SarvamTTSFailure as error:
                print("Sarvam voice unavailable ({}); using Mac voice instead.".format(error), file=sys.stderr)
        elif voice_provider == "sarvam":
            print("SARVAM_API_KEY was not found; using Mac voice instead.", file=sys.stderr)

    voice = "Lekha" if language == "hi" else "Rishi"
    try:
        subprocess.run(["say", "-v", voice, text], check=False)
    except FileNotFoundError:
        print("Speech output is configured for macOS only; reply printed above.")


def make_receptionist(enable_booking: bool = False) -> VoiceReceptionist:
    clinic_app = load_clinic_app()
    gateway = FlaskClinicVoiceGateway(clinic_app)
    return VoiceReceptionist(
        gateway,
        today_provider=lambda: clinic_app.clinic_now().date(),
        booking_gateway=gateway if enable_booking else None,
        allow_booking=enable_booking,
    )


def answer_typed_question(
    receptionist: VoiceReceptionist,
    text: str,
    language: str,
    no_speak: bool,
    voice_provider: str,
    sarvam_speaker: str,
    sarvam_pace: float,
) -> int:
    reply_language = language if language in {"hi", "en"} else "hi"
    reply = receptionist.answer(text, reply_language)
    if reply.redact_transcript:
        print("Patient: [private booking detail hidden]")
    else:
        print("Patient: {}".format(text))
    print("Assistant: {}".format(reply.text))
    speak(
        reply.text,
        reply.language,
        no_speak,
        voice_provider,
        sarvam_speaker,
        sarvam_pace,
    )
    return 0


def read_private_booking_answer(
    receptionist: VoiceReceptionist,
    detected_language: str,
) -> Any:
    """Read one non-OTP booking answer without echoing it to the terminal.

    This is the recovery path when a microphone has trouble hearing a short
    answer such as a gender or confirmation. It must never be used for OTPs:
    those stay in ``complete_private_otp_flow`` after an SMS has been sent.
    """
    private_input = ""
    try:
        private_input = getpass.getpass(receptionist.private_booking_input_prompt())
        return receptionist.answer_private_booking_input(private_input, detected_language)
    except (EOFError, KeyboardInterrupt):
        print("\nPrivate booking entry cancelled.")
        return receptionist.answer_private_booking_input("cancel", detected_language)
    finally:
        # Keep the user's name, mobile number, or choice out of the console
        # and discard our direct reference as soon as it has been processed.
        private_input = ""


def complete_private_otp_flow(
    receptionist: VoiceReceptionist,
    detected_language: str,
    *,
    no_speak: bool,
    voice_provider: str,
    sarvam_speaker: str,
    sarvam_pace: float,
) -> None:
    """Keep OTPs out of speech recognition, terminal history, and TTS requests."""
    while receptionist.is_private_booking_active():
        try:
            private_input = getpass.getpass("Private SMS OTP (or resend/cancel): ")
        except (EOFError, KeyboardInterrupt):
            print("\nPrivate OTP entry cancelled.")
            reply = receptionist.submit_private_otp("cancel", detected_language)
            print("Assistant: {}".format(reply.text))
            speak(
                reply.text,
                reply.language,
                no_speak,
                voice_provider,
                sarvam_speaker,
                sarvam_pace,
            )
            return

        reply = receptionist.submit_private_otp(private_input, detected_language)
        private_input = ""
        print("Assistant: {}".format(reply.text))
        speak(
            reply.text,
            reply.language,
            no_speak,
            voice_provider,
            sarvam_speaker,
            sarvam_pace,
        )
        if not reply.needs_private_otp:
            return


def main() -> int:
    args = parse_arguments()
    if args.duration < 1 or args.duration > 30:
        print("--duration must be between 1 and 30 seconds.", file=sys.stderr)
        return 2
    if args.silence_seconds < 0.25 or args.silence_seconds > args.duration:
        print("--silence-seconds must be at least 0.25 and no more than --duration.", file=sys.stderr)
        return 2
    if args.beam_size < 1 or args.beam_size > 10:
        print("--beam-size must be between 1 and 10.", file=sys.stderr)
        return 2
    if not 0.5 <= args.sarvam_pace <= 2.0:
        print("--sarvam-pace must be between 0.5 and 2.0.", file=sys.stderr)
        return 2

    if args.list_devices:
        try:
            _np, sd, _model = load_audio_dependencies()
        except RuntimeError as error:
            print(error, file=sys.stderr)
            return 1
        return print_input_devices(sd)

    try:
        receptionist = make_receptionist(args.enable_booking)
    except Exception as error:
        print("Could not connect to the Flask clinic app: {}".format(error), file=sys.stderr)
        return 1

    if args.text:
        return answer_typed_question(
            receptionist,
            args.text,
            args.language,
            args.no_speak,
            args.voice_provider,
            args.sarvam_speaker,
            args.sarvam_pace,
        )

    try:
        np, sd, WhisperModel = load_audio_dependencies()
        device, device_info = choose_input_device(sd, args.device)
    except (AudioSetupError, RuntimeError) as error:
        print("Microphone setup problem: {}".format(error), file=sys.stderr)
        print_microphone_permission_help()
        return 1

    # A 16 kHz signed-16-bit mono WAV works for both local Whisper and the
    # approved Sarvam REST endpoint.
    sample_rate = WHISPER_SAMPLE_RATE
    stt_provider = args.stt_provider
    sarvam_stt: Optional[SarvamSpeechToText] = None
    if stt_provider == "sarvam-public":
        sarvam_api_key = get_sarvam_api_key()
        if not sarvam_api_key:
            print("SARVAM_API_KEY was not found; use --stt-provider local or configure the key first.", file=sys.stderr)
            return 1
        sarvam_stt = SarvamSpeechToText(sarvam_api_key)

    local_model: Optional[Any] = None

    def load_local_whisper_model() -> Any:
        """Load local Whisper only when selected or needed as a cloud fallback."""
        nonlocal local_model
        if local_model is None:
            print("Loading the '{}' local Whisper model...".format(args.model))
            try:
                local_model = WhisperModel(args.model, device="cpu", compute_type="int8")
            except Exception as error:
                raise RuntimeError("Could not load the local Whisper model: {}".format(error)) from error
        return local_model

    print("Using microphone: {}".format(device_info["name"]))
    if args.fast:
        print(
            "Recording at {} Hz for up to {} seconds with early silence detection ({} seconds).".format(
                sample_rate,
                args.duration,
                args.silence_seconds,
            )
        )
    else:
        print("Recording at {} Hz for up to {} seconds.".format(sample_rate, args.duration))
    if stt_provider == "sarvam-public":
        print("Speech recognition: Sarvam Saaras for public Hindi, English, and Hinglish questions.")
        print("Privacy: after booking starts, recognition stays local; name, mobile, and OTP use hidden input.")
    else:
        try:
            load_local_whisper_model()
        except RuntimeError as error:
            print(error, file=sys.stderr)
            return 1
        print("Speech recognition: local Whisper '{}'.".format(args.model))

    recovery_model: Optional[Any] = None
    recovery_model_name = args.recovery_model.strip()

    def load_recovery_model() -> Optional[Any]:
        """Lazily load a stronger local model only after a failed fast pass."""
        nonlocal recovery_model
        if not recovery_model_name or recovery_model_name.casefold() in {"none", "off", args.model.casefold()}:
            return None
        if recovery_model is None:
            print(
                "Rechecking this clip with the '{}' local model (first use may download it once)...".format(
                    recovery_model_name
                )
            )
            try:
                recovery_model = WhisperModel(recovery_model_name, device="cpu", compute_type="int8")
            except Exception as error:
                raise RuntimeError("Could not load the recovery Whisper model: {}".format(error)) from error
        return recovery_model

    def recover_transcript(
        audio: Any,
        active_language_hint: Optional[str],
        first_transcript: str,
    ) -> Optional[Tuple[str, str]]:
        """Return a cleaner second local reading, or leave the first pass alone."""
        try:
            stronger_model = load_recovery_model()
            if stronger_model is None:
                return None
            transcript, detected_language = transcribe_audio_compatibility_pass(
                np,
                stronger_model,
                audio,
                sample_rate,
                recovery_language_hint(active_language_hint, first_transcript),
            )
        except Exception as error:
            print("Could not run the local recovery transcription: {}".format(error), file=sys.stderr)
            return None
        if not transcript or not is_usable_voice_transcript(transcript):
            return None
        return transcript, detected_language

    language_hint = None if args.language == "auto" else args.language
    speech_cache = SarvamSpeechCache()
    print(
        "\nPress Enter and speak normally for up to {} seconds{}. "
        "Type t during a booking for hidden keyboard input, or q to stop.".format(
            args.duration,
            "; recording stops shortly after you finish" if args.fast else "",
        )
    )
    print("For privacy, never type a name, mobile number, or OTP at the > prompt.")
    try:
        while True:
            # Sarvam is approved only for a public question. Once a booking
            # starts, name/mobile go through a hidden local prompt. Date,
            # gender, and confirmation remain speakable but use local Whisper
            # because cloud STT is blocked for every booking stage.
            if stt_provider == "sarvam-public" and booking_stage_requires_private_input(receptionist):
                private_language = receptionist.booking_transcription_language_hint() or language_hint or "hi"
                print("Booking details are private. Enter this answer in the hidden prompt below.")
                reply = read_private_booking_answer(receptionist, private_language)
                print("You: [private booking detail received]")
                print("Booking language: {}".format("Hindi" if reply.language == "hi" else "English"))
                print("Assistant: {}".format(reply.text))
                speak(
                    reply.text,
                    reply.language,
                    args.no_speak,
                    args.voice_provider,
                    args.sarvam_speaker,
                    args.sarvam_pace,
                    speech_cache=speech_cache,
                    cacheable=reply.intent in SAFE_TTS_CACHE_INTENTS,
                )
                if reply.needs_private_otp:
                    complete_private_otp_flow(
                        receptionist,
                        private_language,
                        no_speak=args.no_speak,
                        voice_provider=args.voice_provider,
                        sarvam_speaker=args.sarvam_speaker,
                        sarvam_pace=args.sarvam_pace,
                    )
                continue

            command = input("> ").strip().casefold()
            if command in {"q", "quit", "exit"}:
                break
            if command in PRIVATE_BOOKING_FALLBACK_COMMANDS:
                if not receptionist.is_private_booking_active():
                    print("Private entry is available after a booking starts. Press Enter to speak.")
                    continue
                private_language = receptionist.booking_transcription_language_hint() or language_hint or "hi"
                reply = read_private_booking_answer(receptionist, private_language)
                print("You: [private booking detail received]")
                print("Booking language: {}".format("Hindi" if reply.language == "hi" else "English"))
                print("Assistant: {}".format(reply.text))
                speak(
                    reply.text,
                    reply.language,
                    args.no_speak,
                    args.voice_provider,
                    args.sarvam_speaker,
                    args.sarvam_pace,
                    speech_cache=speech_cache,
                    cacheable=reply.intent in SAFE_TTS_CACHE_INTENTS,
                )
                if reply.needs_private_otp:
                    complete_private_otp_flow(
                        receptionist,
                        private_language,
                        no_speak=args.no_speak,
                        voice_provider=args.voice_provider,
                        sarvam_speaker=args.sarvam_speaker,
                        sarvam_pace=args.sarvam_pace,
                    )
                continue
            if command:
                print("Press Enter to speak, or type t for a hidden booking answer.")
                continue
            print("Listening now...")
            try:
                audio = record_audio(
                    np,
                    sd,
                    device,
                    sample_rate,
                    args.duration,
                    stop_on_silence=args.fast,
                    silence_seconds=args.silence_seconds,
                )
                # At the beginning we auto-detect the caller's language.  Once
                # booking begins, very short replies such as "kal", "female",
                # or "yes" are unreliable for auto-detection, so keep the
                # booking in the language chosen by the initial request.
                active_language_hint = (
                    receptionist.booking_transcription_language_hint() or language_hint
                )
                if stt_provider == "sarvam-public" and sarvam_stt is not None and should_use_sarvam_public_stt(receptionist):
                    try:
                        transcript, detected_language = transcribe_audio_with_sarvam(
                            np,
                            sarvam_stt,
                            audio,
                            sample_rate,
                            active_language_hint,
                        )
                    except SarvamSTTFailure as error:
                        # Do not retry the cloud request: an API retry would
                        # upload the same audio again. One local fallback is
                        # safer and preserves a usable offline path.
                        print(
                            "Sarvam recognition is unavailable ({}); using local Whisper for this public request.".format(error),
                            file=sys.stderr,
                        )
                        transcription_prompt, transcription_hotwords = transcription_context_for(receptionist)
                        transcript, detected_language = transcribe_audio(
                            np,
                            load_local_whisper_model(),
                            audio,
                            sample_rate,
                            active_language_hint,
                            initial_prompt=transcription_prompt,
                            hotwords=transcription_hotwords,
                            beam_size=args.beam_size,
                        )
                else:
                    transcription_prompt, transcription_hotwords = transcription_context_for(receptionist)
                    transcript, detected_language = transcribe_audio(
                        np,
                        load_local_whisper_model(),
                        audio,
                        sample_rate,
                        active_language_hint,
                        initial_prompt=transcription_prompt,
                        hotwords=transcription_hotwords,
                        beam_size=args.beam_size,
                    )
            except AudioSetupError as error:
                print("Microphone setup problem: {}".format(error), file=sys.stderr)
                print_microphone_permission_help()
                return 1
            except Exception as error:
                print("Could not transcribe the recording: {}".format(error), file=sys.stderr)
                continue

            if not transcript:
                print("I could not hear speech. Please try again.")
                speak(
                    "I could not hear speech. Please try again.",
                    "en",
                    args.no_speak,
                    args.voice_provider,
                    args.sarvam_speaker,
                    args.sarvam_pace,
                    speech_cache=speech_cache,
                    cacheable=True,
                )
                continue

            # Never let obvious ASR repetition/garbage enter the conversation
            # state machine.  In a booking this is important: a hallucinated
            # repeated "today" must not be treated as a selected date, and
            # garbled "yes" must not reach the OTP-SMS confirmation step.
            # Do not print the rejected transcript because it may have been a
            # patient detail from an active booking.
            recovery_attempted = False
            if not is_usable_voice_transcript(transcript):
                recovery_attempted = True
                recovered = recover_transcript(audio, active_language_hint, transcript)
                if recovered is not None:
                    transcript, detected_language = recovered

            if not is_usable_voice_transcript(transcript):
                retry_language = receptionist.booking_transcription_language_hint() or (
                    detected_language if detected_language in {"hi", "en"} else "hi"
                )
                retry_text = (
                    "मैं आवाज़ साफ़ नहीं समझ पाई। कृपया एक बार फिर बोलें।"
                    if retry_language == "hi"
                    else "I could not understand that clearly. Please say it once more."
                )
                print("I could not understand that clearly. Please say it once more.")
                speak(
                    retry_text,
                    retry_language,
                    args.no_speak,
                    args.voice_provider,
                    args.sarvam_speaker,
                    args.sarvam_pace,
                    speech_cache=speech_cache,
                    cacheable=True,
                )
                continue

            was_private_booking_active = receptionist.is_private_booking_active()
            reply = receptionist.answer(transcript, detected_language)

            # A plausible-but-wrong fast transcript can otherwise become the
            # generic reply seen in the console. Re-read that same public WAV
            # locally with the stronger model, but accept only a read-only
            # intent—never let a recovery decode silently start a booking or
            # send an OTP.
            if (
                reply.intent == "fallback"
                and not was_private_booking_active
                and not recovery_attempted
            ):
                recovery_attempted = True
                recovered = recover_transcript(audio, active_language_hint, transcript)
                if recovered is not None:
                    recovery_text, recovery_language = recovered
                    # Probe with booking disabled before asking the live
                    # receptionist. This guarantees a noisy recovery cannot
                    # mutate a conversation into a booking merely by being
                    # inspected.
                    recovery_probe = VoiceReceptionist(
                        receptionist.gateway,
                        today_provider=receptionist.today_provider,
                    )
                    probe_reply = recovery_probe.answer(recovery_text, recovery_language)
                    if probe_reply.intent in READ_ONLY_RECOVERY_INTENTS:
                        recovery_reply = receptionist.answer(recovery_text, recovery_language)
                        transcript = recovery_text
                        detected_language = recovery_language
                        reply = recovery_reply
            if reply.redact_transcript:
                print("You: [private booking detail received]")
                print("Booking language: {}".format("Hindi" if reply.language == "hi" else "English"))
            else:
                print("You: {}".format(transcript))
                print("Detected language: {}".format(detected_language))
            print("Assistant: {}".format(reply.text))
            if reply.intent in PRIVATE_BOOKING_RETRY_INTENTS:
                print("Tip: type t at > to enter this answer privately if speech is not recognized.")
            speak(
                reply.text,
                reply.language,
                args.no_speak,
                args.voice_provider,
                args.sarvam_speaker,
                args.sarvam_pace,
                speech_cache=speech_cache,
                cacheable=reply.intent in SAFE_TTS_CACHE_INTENTS,
            )
            if reply.needs_private_otp:
                complete_private_otp_flow(
                    receptionist,
                    detected_language,
                    no_speak=args.no_speak,
                    voice_provider=args.voice_provider,
                    sarvam_speaker=args.sarvam_speaker,
                    sarvam_pace=args.sarvam_pace,
                )
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
