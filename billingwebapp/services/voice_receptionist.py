"""Safe, text-first conversation logic for the local clinic voice receptionist.

This module intentionally does not import Flask or database models.  The
launcher supplies a small gateway that exposes only public, aggregate booking
availability.  Patient reports and bookings remain behind the existing OTP
flows in the Flask app.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Callable, Optional, Protocol, Tuple


@dataclass(frozen=True)
class AppointmentAvailability:
    """Public appointment information that is safe to say aloud."""

    appointment_date: date
    booking_open: bool
    normal_remaining: int
    priority_remaining: int
    priority_available: bool
    arrival_window_label: str
    normal_fee: float
    priority_fee: float
    message: str = ""


@dataclass(frozen=True)
class VoiceReply:
    """A response for the console voice layer or a future phone channel."""

    text: str
    language: str
    intent: str
    requires_otp: bool = False
    redact_transcript: bool = False
    needs_private_otp: bool = False


@dataclass(frozen=True)
class PendingVoiceBooking:
    """A non-displayable reference for one in-progress OTP booking."""

    booking_ref: str = field(repr=False)
    appointment_date: date
    arrival_window_label: str


@dataclass(frozen=True)
class ConfirmedVoiceBooking:
    """Only the safe details needed for a spoken booking confirmation."""

    appointment_date: date
    arrival_window_label: str


class ClinicBookingGateway(Protocol):
    """Narrow, OTP-preserving write boundary for normal appointments."""

    def voice_booking_ready(self) -> Tuple[bool, str]:
        """Report whether a real OTP transport is available before collecting details."""

    def start_normal_booking(
        self,
        *,
        patient_name: str,
        mobile: str,
        gender: str,
        appointment_date: date,
    ) -> Tuple[Optional[PendingVoiceBooking], str]:
        """Reserve capacity and send an existing secure OTP challenge."""

    def verify_normal_booking_otp(
        self,
        *,
        booking_ref: str,
        mobile: str,
        otp_code: str,
    ) -> Tuple[Optional[ConfirmedVoiceBooking], str]:
        """Confirm a normal appointment only after the existing OTP check."""

    def resend_normal_booking_otp(self, *, booking_ref: str, mobile: str) -> str:
        """Send a replacement OTP through the existing rate-limited helper."""

    def cancel_pending_booking(self, *, booking_ref: str, mobile: str) -> None:
        """Release an unfinished OTP reservation when the caller cancels."""


class BookingStage(str, Enum):
    """The local-only sequence used to gather a minimal booking request."""

    IDLE = "idle"
    ASK_DATE = "ask_date"
    ASK_NAME = "ask_name"
    ASK_MOBILE = "ask_mobile"
    ASK_GENDER = "ask_gender"
    REVIEW = "review"
    OTP_PENDING = "otp_pending"


@dataclass
class BookingDraft:
    """Sensitive fields kept in memory only until the request finishes."""

    appointment_date: Optional[date] = None
    patient_name: str = field(default="", repr=False)
    mobile: str = field(default="", repr=False)
    gender: str = field(default="", repr=False)


class ClinicVoiceGateway(Protocol):
    """The intentionally narrow bridge from conversation to Flask data."""

    def appointment_availability(self, appointment_date: date) -> AppointmentAvailability:
        """Return public capacity and arrival-window information for one day."""


@dataclass
class ConversationState:
    """Ephemeral state retained only while the local conversation runs."""

    selected_date: Optional[date] = None
    booking_stage: BookingStage = BookingStage.IDLE
    booking_language: str = field(default="", repr=False)
    booking_draft: BookingDraft = field(default_factory=BookingDraft, repr=False)
    pending_booking: Optional[PendingVoiceBooking] = field(default=None, repr=False)


class VoiceReceptionist:
    """Route Hindi, Hinglish, and English questions to safe clinic answers."""

    def __init__(
        self,
        gateway: ClinicVoiceGateway,
        *,
        today_provider: Callable[[], date] = date.today,
        booking_gateway: Optional[ClinicBookingGateway] = None,
        allow_booking: bool = False,
    ) -> None:
        self.gateway = gateway
        self.today_provider = today_provider
        self.booking_gateway = booking_gateway
        self.allow_booking = bool(allow_booking and booking_gateway is not None)
        self.state = ConversationState()

    def is_private_booking_active(self) -> bool:
        """Whether console transcript output must be hidden for this turn."""
        return self.state.booking_stage is not BookingStage.IDLE

    def booking_transcription_language_hint(self) -> Optional[str]:
        """Keep short booking answers in the caller's initial language."""
        return self.state.booking_language or None

    def booking_transcription_context(self) -> Optional[str]:
        """Return a small public ASR hint for the current booking step.

        The launcher may supply this to a local speech recognizer to make
        short choices more reliable.  It intentionally contains no caller
        name, phone number, booking reference, or OTP.
        """
        contexts = {
            BookingStage.ASK_DATE: (
                "Clinic appointment date. Hindi or English: today, tomorrow, kal, aaj, or a calendar date."
            ),
            BookingStage.ASK_NAME: "Patient full name for a clinic appointment.",
            BookingStage.ASK_MOBILE: "An Indian 10 digit mobile number, spoken digit by digit.",
            BookingStage.ASK_GENDER: (
                "Gender selection only: female, male, or other. Hindi: महिला, पुरुष, or अन्य."
            ),
            BookingStage.REVIEW: (
                "Booking confirmation only: yes, haan, or confirm; or change date, change name, change mobile, "
                "change gender, or cancel."
            ),
        }
        return contexts.get(self.state.booking_stage)

    def private_booking_input_prompt(self) -> str:
        """Return a non-sensitive prompt for the hidden keyboard fallback.

        This is intentionally separate from the OTP entry: it lets a caller
        privately type a booking answer only when their microphone repeatedly
        mishears it. The input itself is never echoed or logged by the
        launcher.
        """
        prompts = {
            BookingStage.ASK_DATE: "Private date (1=today, 2=tomorrow, or a date): ",
            BookingStage.ASK_NAME: "Private patient name: ",
            BookingStage.ASK_MOBILE: "Private 10-digit mobile number: ",
            BookingStage.ASK_GENDER: "Private gender (1=female, 2=male, 3=other): ",
            BookingStage.REVIEW: "Private choice (y=send SMS, c=cancel): ",
        }
        return prompts.get(self.state.booking_stage, "Private booking answer: ")

    def answer_private_booking_input(
        self,
        private_input: str,
        detected_language: str = "hi",
    ) -> VoiceReply:
        """Process a hidden fallback answer without exposing it to the console.

        Numeric shortcuts are valid only at their matching step. They are
        converted to the same explicit words handled by the regular booking
        state machine, so validation and OTP safety remain identical for voice
        and hidden typed input.
        """
        if not self.is_private_booking_active():
            return VoiceReply(
                text="No private booking step is active. Please start a booking first.",
                language="en",
                intent="private_booking_not_active",
            )

        normalized = self._normalize(private_input)
        stage = self.state.booking_stage
        shortcuts = {
            BookingStage.ASK_DATE: {"1": "today", "2": "tomorrow"},
            BookingStage.ASK_GENDER: {
                "1": "female",
                "2": "male",
                "3": "other",
                "f": "female",
                "m": "male",
                "o": "other",
            },
            BookingStage.REVIEW: {"y": "yes", "c": "cancel"},
        }
        safe_input = shortcuts.get(stage, {}).get(normalized, private_input)
        return self.answer(safe_input, detected_language)

    def answer(self, transcript: str, detected_language: str = "hi") -> VoiceReply:
        normalized = self._normalize(transcript)
        hindi = self._is_hindi(transcript, detected_language)

        # Once a private booking starts, do not route spoken text through the
        # normal conversational intents. In particular, a spoken OTP is never
        # treated as verification input.
        if self.is_private_booking_active():
            booking_hindi = self.state.booking_language != "en"
            return self._continue_booking(transcript, normalized, booking_hindi)

        selected_date = self._resolve_date(normalized)

        if self._contains_any(
            normalized,
            (
                "medicine change",
                "change medicine",
                "medicine",
                "medication",
                "dawa",
                "dawai",
                "dose",
                "दवा",
                "दवाई",
                "खुराक",
            ),
        ):
            return self._medical_safety_reply(hindi)

        if self._contains_any(
            normalized,
            ("report", "lab report", "thyroid", "blood test", "रिपोर्ट", "थायराइड", "ब्लड टेस्ट"),
        ):
            return self._report_reply(hindi)

        if self._contains_any(
            normalized,
            ("fee", "fees", "charge", "price", "कितनी फीस", "फीस", "चार्ज"),
        ):
            return self._fee_reply(selected_date, hindi)

        doctor_terms = (
            "doctor", "dr.", "dr ", "डॉक्टर", "डाक्टर", "डाक्तर", "डोक्टर", "डॉकटर",
            "डॉ", "डॉ.", "डा", "डा.", "डॉक्टर साहब", "डॉक्टर साहेब", "डाक्टर साहब",
            "डाक्टर साहेब", "डाक्तर साहब", "डाक्तर साहेब", "doctor sahib", "doctor saheb",
        )
        schedule_terms = (
            "clinic",
            "hospital",
            "available",
            "open",
            "today",
            "tomorrow",
            "aaj",
            "kal",
            "बैठेंगे",
            "बैठे",
            "बैठता",
            "बैठते",
            "बैठना",
            "होंगे",
            "होगे",
            "क्लिनिक",
            "क्लीनिक",
            "क्लिनीक",
            "अस्पताल",
            "आज",
            "कल",
            "कब",
            "कितने बजे",
        )
        if self._contains_any(normalized, doctor_terms) and self._contains_any(normalized, schedule_terms):
            return self._doctor_schedule_reply(selected_date, hindi)

        appointment_terms = (
            "appointment", "booking", "book", "अपॉइंटमेंट", "अपोइंटमेंट", "अप्वाइंटमेंट",
            "बुकिंग", "बुक",
        )
        if self._contains_any(normalized, appointment_terms):
            booking_request = self._contains_any(
                normalized,
                ("book", "booking", "कर दो", "करना", "बुक", "बुकिंग"),
            )
            if booking_request and self.allow_booking:
                return self._start_booking(normalized, hindi)
            return self._appointment_reply(selected_date, hindi, booking_request)

        if self._contains_any(normalized, ("time", "what time", "समय", "टाइम")):
            return VoiceReply(
                text=("क्लिनिक की arrival timing पूछने के लिए डॉक्टर या appointment के बारे में पूछें।"
                      if hindi else "Ask about the doctor or appointments to hear the clinic arrival timing."),
                language="hi" if hindi else "en",
                intent="clinic_time",
            )

        if self._contains_any(normalized, ("hello", "hi", "hey", "namaste", "नमस्ते", "हेलो")):
            return VoiceReply(
                text=("नमस्ते। मैं clinic receptionist हूँ। Appointment, fees या lab report के बारे में पूछ सकते हैं।"
                      if hindi else "Hello. I am the clinic receptionist. You can ask about appointments, fees, or lab reports."),
                language="hi" if hindi else "en",
                intent="greeting",
            )

        return VoiceReply(
            text=(
                "मैं appointment availability, clinic arrival timing, fees और lab reports में मदद कर सकती हूँ। "
                "दवा या इलाज के बारे में डॉक्टर से बात करें।"
                if hindi
                else "I can help with appointment availability, clinic arrival timing, fees, and lab reports. "
                "Please speak to a doctor about medicines or treatment."
            ),
            language="hi" if hindi else "en",
            intent="fallback",
        )

    def submit_private_otp(self, private_input: str, detected_language: str = "hi") -> VoiceReply:
        """Accept only hidden terminal input; microphone speech is never an OTP."""
        hindi = self.state.booking_language != "en" if self.is_private_booking_active() else detected_language != "en"
        if (
            self.state.booking_stage is not BookingStage.OTP_PENDING
            or not self.state.pending_booking
            or not self.booking_gateway
        ):
            return VoiceReply(
                text=(
                    "अभी कोई OTP verification pending नहीं है। पहले appointment booking शुरू करें।"
                    if hindi
                    else "There is no pending OTP verification. Please start an appointment booking first."
                ),
                language="hi" if hindi else "en",
                intent="otp_not_pending",
            )

        command = self._normalize(private_input)
        if command in {"cancel", "stop", "रद्द", "कैंसल"}:
            return self._cancel_booking(hindi)
        if command in {"resend", "send again", "फिर भेजो", "दोबारा भेजो"}:
            try:
                error = self.booking_gateway.resend_normal_booking_otp(
                    booking_ref=self.state.pending_booking.booking_ref,
                    mobile=self.state.booking_draft.mobile,
                )
            except Exception:
                error = "Unable to resend the verification code right now."
            if error:
                return self._private_otp_reply(
                    self._otp_error_text(error, hindi),
                    hindi,
                    intent="appointment_otp_resend_failed",
                )
            return self._private_otp_reply(
                "नया verification SMS भेज दिया गया है। कृपया छह अंकों का OTP private terminal input में type करें।"
                if hindi
                else "A new verification SMS was sent. Please type the six-digit OTP in the private terminal input.",
                hindi,
                intent="appointment_otp_resent",
            )

        otp_code = (private_input or "").strip()
        if len(otp_code) != 6 or not otp_code.isdigit():
            return self._private_otp_reply(
                "कृपया SMS में आया छह अंकों का OTP private terminal input में type करें, या resend अथवा cancel type करें।"
                if hindi
                else "Type the six-digit SMS OTP in the private terminal input, or type resend or cancel.",
                hindi,
                intent="appointment_otp_invalid_format",
            )

        try:
            confirmed, error = self.booking_gateway.verify_normal_booking_otp(
                booking_ref=self.state.pending_booking.booking_ref,
                mobile=self.state.booking_draft.mobile,
                otp_code=otp_code,
            )
        except Exception:
            confirmed, error = None, "Unable to verify the code right now."
        finally:
            # Keep the code out of any subsequent local variable or response.
            otp_code = ""

        if confirmed:
            date_label = self._date_label(confirmed.appointment_date)
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "Appointment {} के लिए confirm हो गया है। Arrival window {} है। "
                    "यह individual time slot नहीं है; token reception पर मिलेगा।"
                ).format(date_label, confirmed.arrival_window_label)
                if hindi
                else (
                    "The appointment for {} is confirmed. The arrival window is {}. "
                    "This is not an individual time slot; the token is issued at reception."
                ).format(date_label, confirmed.arrival_window_label),
                language="hi" if hindi else "en",
                intent="appointment_confirmed",
            )

        if self._otp_error_requires_new_booking(error):
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "Verification request अब valid नहीं है। कृपया appointment booking फिर से शुरू करें।"
                    if hindi
                    else "The verification request is no longer valid. Please start the appointment booking again."
                ),
                language="hi" if hindi else "en",
                intent="appointment_otp_expired",
            )
        return self._private_otp_reply(
            self._otp_error_text(error, hindi),
            hindi,
            intent="appointment_otp_failed",
        )

    def _start_booking(self, normalized: str, hindi: bool) -> VoiceReply:
        self._reset_booking_state()
        if self.booking_gateway:
            try:
                ready, error = self.booking_gateway.voice_booking_ready()
            except Exception:
                ready, error = False, "Voice booking is not available right now."
            if not ready:
                return VoiceReply(
                    text=(
                        "Voice booking अभी शुरू नहीं हो सकता। {}"
                    ).format(self._booking_error_text(error, hindi))
                    if hindi
                    else self._booking_error_text(error, hindi),
                    language="hi" if hindi else "en",
                    intent="appointment_booking_not_ready",
                )
        self.state.booking_language = "hi" if hindi else "en"
        self.state.booking_stage = BookingStage.ASK_DATE
        selected_date = self._extract_booking_date(normalized)
        if selected_date:
            return self._accept_booking_date(selected_date, hindi)
        return self._booking_reply(
            "ज़रूर। किस तारीख़ के लिए regular appointment चाहिए? आज, कल, या तारीख़ बोलें।"
            if hindi
            else "Sure. Which date do you need a regular appointment for? Say today, tomorrow, or the date.",
            hindi,
            intent="appointment_booking_ask_date",
        )

    def _continue_booking(self, transcript: str, normalized: str, hindi: bool) -> VoiceReply:
        if self._contains_any(normalized, ("cancel", "stop", "रद्द", "कैंसल")):
            return self._cancel_booking(hindi)

        stage = self.state.booking_stage
        if stage is BookingStage.OTP_PENDING:
            return self._private_otp_reply(
                "OTP को बोलना सुरक्षित नहीं है। कृपया terminal के private input में OTP type करें।"
                if hindi
                else "It is not safe to say an OTP aloud. Please type it in the terminal's private input.",
                hindi,
                intent="appointment_otp_must_be_private",
            )

        if stage is BookingStage.ASK_DATE:
            selected_date = self._extract_booking_date(normalized)
            if not selected_date:
                return self._booking_reply(
                    "कृपया आज, कल, या तारीख़ बोलें।"
                    if hindi
                    else "Please say today, tomorrow, or the appointment date.",
                    hindi,
                    intent="appointment_booking_date_retry",
                )
            return self._accept_booking_date(selected_date, hindi)

        if stage is BookingStage.ASK_NAME:
            patient_name = self._extract_patient_name(transcript)
            if not patient_name:
                return self._booking_reply(
                    "कृपया patient का पूरा नाम बोलें।"
                    if hindi
                    else "Please say the patient's full name.",
                    hindi,
                    intent="appointment_booking_name_retry",
                )
            self.state.booking_draft.patient_name = patient_name
            self.state.booking_stage = BookingStage.ASK_MOBILE
            return self._booking_reply(
                "अब patient का 10-digit mobile number धीरे-धीरे बोलें। मैं number दोहराऊँगी नहीं।"
                if hindi
                else "Now say the patient's 10-digit mobile number slowly. I will not repeat it aloud.",
                hindi,
                intent="appointment_booking_ask_mobile",
            )

        if stage is BookingStage.ASK_MOBILE:
            mobile = self._extract_mobile_number(transcript)
            if not mobile:
                return self._booking_reply(
                    "मुझे valid 10-digit mobile number नहीं मिला। कृपया digits फिर से धीरे-धीरे बोलें।"
                    if hindi
                    else "I could not get a valid 10-digit mobile number. Please say the digits slowly again.",
                    hindi,
                    intent="appointment_booking_mobile_retry",
                )
            self.state.booking_draft.mobile = mobile
            self.state.booking_stage = BookingStage.ASK_GENDER
            return self._booking_reply(
                "Patient का gender male, female, या other में से क्या है?"
                if hindi
                else "What is the patient's gender: male, female, or other?",
                hindi,
                intent="appointment_booking_ask_gender",
            )

        if stage is BookingStage.ASK_GENDER:
            gender = self._extract_gender(normalized)
            if not gender:
                return self._booking_reply(
                    "मुझे gender स्पष्ट नहीं मिला। सिर्फ female, male, या other बोलें। "
                    "हिंदी में महिला, पुरुष, या अन्य भी बोल सकते हैं।"
                    if hindi
                    else "I did not get a clear gender choice. Say only female, male, or other. "
                    "You can also say woman, man, or non-binary.",
                    hindi,
                    intent="appointment_booking_gender_retry",
                )
            self.state.booking_draft.gender = gender
            self.state.booking_stage = BookingStage.REVIEW
            return self._booking_review_reply(hindi)

        if stage is BookingStage.REVIEW:
            return self._handle_booking_review(normalized, hindi)

        self._reset_booking_state()
        return VoiceReply(
            text=(
                "Booking conversation reset हो गया है। कृपया appointment book करने के लिए फिर से बोलें।"
                if hindi
                else "The booking conversation was reset. Please ask to book an appointment again."
            ),
            language="hi" if hindi else "en",
            intent="appointment_booking_reset",
        )

    def _accept_booking_date(self, selected_date: date, hindi: bool) -> VoiceReply:
        try:
            availability = self._availability(selected_date)
        except Exception:
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "अभी appointment availability check नहीं हो पा रही है। कृपया थोड़ी देर बाद कोशिश करें।"
                    if hindi
                    else "Appointment availability cannot be checked right now. Please try again shortly."
                ),
                language="hi" if hindi else "en",
                intent="appointment_booking_availability_error",
            )

        date_label = self._date_label(selected_date)
        if not availability.booking_open:
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "{} के लिए regular online appointment उपलब्ध नहीं है। {}"
                ).format(date_label, availability.message or "कृपया दूसरी तारीख़ चुनें।")
                if hindi
                else "Regular online appointments are unavailable for {}. {}".format(
                    date_label, availability.message or "Please choose another date."
                ),
                language="hi" if hindi else "en",
                intent="appointment_booking_unavailable",
            )
        if availability.normal_remaining <= 0:
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "{} के लिए regular appointments भर चुके हैं। Priority request में staff payment verification जरूरी है, "
                    "इसलिए कृपया secure booking page या clinic reception इस्तेमाल करें।"
                ).format(date_label)
                if hindi
                else (
                    "Regular appointments are full for {}. A priority request requires staff payment verification, "
                    "so please use the secure booking page or clinic reception."
                ).format(date_label),
                language="hi" if hindi else "en",
                intent="appointment_booking_priority_handoff",
            )

        self.state.booking_draft.appointment_date = selected_date
        self.state.booking_stage = BookingStage.ASK_NAME
        return self._booking_reply(
            "{} के लिए regular appointment उपलब्ध है। अब patient का पूरा नाम बोलें।".format(date_label)
            if hindi
            else "A regular appointment is available for {}. Now say the patient's full name.".format(date_label),
            hindi,
            intent="appointment_booking_ask_name",
        )

    def _handle_booking_review(self, normalized: str, hindi: bool) -> VoiceReply:
        if self._contains_any(normalized, ("change date", "date change", "तारीख बदल", "डेट बदल")):
            self.state.booking_draft.appointment_date = None
            self.state.booking_stage = BookingStage.ASK_DATE
            return self._booking_reply(
                "ठीक है। नई appointment date बोलें।" if hindi else "Okay. Say the new appointment date.",
                hindi,
                intent="appointment_booking_change_date",
            )
        if self._contains_any(normalized, ("change name", "name change", "नाम बदल")):
            self.state.booking_draft.patient_name = ""
            self.state.booking_stage = BookingStage.ASK_NAME
            return self._booking_reply(
                "ठीक है। Patient का पूरा नाम फिर से बोलें।" if hindi else "Okay. Say the patient's full name again.",
                hindi,
                intent="appointment_booking_change_name",
            )
        if self._contains_any(normalized, ("change mobile", "mobile change", "number change", "नंबर बदल", "मोबाइल बदल")):
            self.state.booking_draft.mobile = ""
            self.state.booking_stage = BookingStage.ASK_MOBILE
            return self._booking_reply(
                "ठीक है। 10-digit mobile number फिर से बोलें।" if hindi else "Okay. Say the 10-digit mobile number again.",
                hindi,
                intent="appointment_booking_change_mobile",
            )
        if self._contains_any(normalized, ("change gender", "gender change", "gender बदल")):
            self.state.booking_draft.gender = ""
            self.state.booking_stage = BookingStage.ASK_GENDER
            return self._booking_reply(
                "ठीक है। Male, female, या other बोलें।" if hindi else "Okay. Say male, female, or other.",
                hindi,
                intent="appointment_booking_change_gender",
            )
        if not self._is_affirmative(normalized):
            return self._booking_reply(
                "OTP भेजने के लिए सिर्फ हाँ, yes, या confirm बोलें। बदलाव के लिए change date, change name, "
                "change mobile, change gender, या cancel बोल सकते हैं।"
                if hindi
                else "To send the OTP, say only yes, haan, or confirm. You can say change date, change name, "
                "change mobile, change gender, or cancel.",
                hindi,
                intent="appointment_booking_review_retry",
            )

        draft = self.state.booking_draft
        if not self.booking_gateway or not draft.appointment_date or not draft.patient_name or not draft.mobile or not draft.gender:
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "Booking details सुरक्षित रूप से reset हो गई हैं। कृपया appointment booking फिर से शुरू करें।"
                    if hindi
                    else "The booking details were safely reset. Please start the appointment booking again."
                ),
                language="hi" if hindi else "en",
                intent="appointment_booking_reset",
            )
        try:
            pending, error = self.booking_gateway.start_normal_booking(
                patient_name=draft.patient_name,
                mobile=draft.mobile,
                gender=draft.gender,
                appointment_date=draft.appointment_date,
            )
        except Exception:
            pending, error = None, "Unable to start the appointment request right now."
        if not pending:
            self._reset_booking_state()
            return VoiceReply(
                text=(
                    "Appointment request शुरू नहीं हो सका। {}"
                ).format(self._booking_error_text(error, hindi))
                if hindi
                else self._booking_error_text(error, hindi),
                language="hi" if hindi else "en",
                intent="appointment_booking_start_failed",
            )

        self.state.pending_booking = pending
        self.state.booking_stage = BookingStage.OTP_PENDING
        return self._private_otp_reply(
            "Verification SMS भेज दिया गया है। OTP को बोलें नहीं; terminal के private input में छह अंकों का OTP type करें।"
            if hindi
            else "A verification SMS was sent. Do not say the OTP aloud; type the six-digit OTP in the terminal's private input.",
            hindi,
            intent="appointment_booking_otp_sent",
        )

    def _booking_review_reply(self, hindi: bool) -> VoiceReply:
        selected_date = self.state.booking_draft.appointment_date
        if not selected_date:
            self._reset_booking_state()
            return VoiceReply(
                text=("कृपया appointment date फिर से बोलें।" if hindi else "Please say the appointment date again."),
                language="hi" if hindi else "en",
                intent="appointment_booking_date_retry",
            )
        availability = self._availability(selected_date)
        fee = "₹{:,.0f}".format(availability.normal_fee)
        return self._booking_reply(
            "Review: {} के लिए regular appointment, fee {}, और arrival window {} है। "
            "यह individual time slot नहीं है। Verification SMS भेजने के लिए हाँ बोलें।".format(
                self._date_label(selected_date), fee, availability.arrival_window_label
            )
            if hindi
            else "Review: a regular appointment for {}, fee {}, and arrival window {}. "
            "This is not an individual time slot. Say yes to send the verification SMS.".format(
                self._date_label(selected_date), fee, availability.arrival_window_label
            ),
            hindi,
            intent="appointment_booking_review",
        )

    def _cancel_booking(self, hindi: bool) -> VoiceReply:
        pending = self.state.pending_booking
        if pending and self.booking_gateway:
            try:
                self.booking_gateway.cancel_pending_booking(
                    booking_ref=pending.booking_ref,
                    mobile=self.state.booking_draft.mobile,
                )
            except Exception:
                # The server-side reservation also expires automatically; do
                # not expose implementation details to the caller.
                pass
        self._reset_booking_state()
        return VoiceReply(
            text=("Appointment booking request cancel कर दिया गया है।" if hindi else "The appointment booking request was cancelled."),
            language="hi" if hindi else "en",
            intent="appointment_booking_cancelled",
        )

    def _reset_booking_state(self) -> None:
        self.state.booking_stage = BookingStage.IDLE
        self.state.booking_language = ""
        self.state.booking_draft = BookingDraft()
        self.state.pending_booking = None

    @staticmethod
    def _extract_patient_name(text: str) -> str:
        candidate = re.sub(r"\s+", " ", text or "").strip()
        candidate = re.sub(
            r"^(?:my\s+name\s+is|mera\s+naam\s+(?:hai|is)?|नाम\s+(?:है|is)?|name\s+(?:is)?)\s+",
            "",
            candidate,
            flags=re.IGNORECASE,
        ).strip(" .,-")
        if len(candidate) < 2 or len(candidate) > 120 or any(character.isdigit() for character in candidate):
            return ""
        return candidate

    @staticmethod
    def _extract_mobile_number(text: str) -> str:
        number_words = {
            "zero": "0", "oh": "0", "o": "0", "shunya": "0", "शून्य": "0", "जीरो": "0",
            "one": "1", "ek": "1", "एक": "1",
            "two": "2", "do": "2", "दो": "2",
            "three": "3", "teen": "3", "तीन": "3",
            "four": "4", "char": "4", "चार": "4",
            "five": "5", "paanch": "5", "panch": "5", "पांच": "5", "पाँच": "5",
            "six": "6", "chhah": "6", "cheh": "6", "छह": "6",
            "seven": "7", "saat": "7", "सात": "7",
            "eight": "8", "aath": "8", "आठ": "8",
            "nine": "9", "nau": "9", "नो": "9", "नौ": "9",
        }
        digits = []
        tokens = re.findall(r"[A-Za-z]+|[\u0900-\u097F]+|[0-9\u0966-\u096F]+", (text or "").casefold())
        for token in tokens:
            replacement = number_words.get(token, token)
            for character in replacement:
                if not character.isdigit():
                    continue
                try:
                    digits.append(str(unicodedata.digit(character)))
                except (TypeError, ValueError):
                    continue
        number = "".join(digits)
        if len(number) == 12 and number.startswith("91"):
            number = number[-10:]
        elif len(number) == 11 and number.startswith("0"):
            number = number[-10:]
        return number if len(number) == 10 else ""

    @staticmethod
    def _extract_gender(normalized: str) -> str:
        """Classify only explicit, commonly transcribed gender answers.

        This runs while the assistant is specifically asking for gender.  It
        accepts common Hindi/Hinglish transcription variants, including
        spaced forms such as ``फी मेल`` / ``fe male``.  It deliberately does
        not infer a gender from a patient name, voice, or an ambiguous word
        such as ``email``.
        """
        tokens = VoiceReceptionist._booking_tokens(normalized)
        token_set = set(tokens)
        compact = "".join(tokens).rstrip("्")

        female_terms = {
            "female", "femail", "femal", "femel", "feemale", "femele", "femali",
            "woman", "women", "lady", "girl", "mahila", "maheela", "mahilla", "ladki",
            "फीमेल", "फिमेल", "फीमैल", "फिमैल", "महिला", "महीला", "स्त्री", "औरत", "लड़की", "लडकी",
        }
        male_terms = {
            "male", "man", "boy", "purush", "mard", "ladka", "aadmi", "admi",
            "पुरुष", "पुरूष", "मर्द", "आदमी", "आदमी", "लड़का", "लडका",
        }
        other_terms = {
            "other", "others", "othar", "adar", "third", "trans", "transgender", "nonbinary",
            "अन्य", "अदर", "अधर", "थर्ड", "तीसरा", "ट्रांस", "ट्रांसजेंडर", "नॉनबाइनरी",
        }

        # Test female before male because the word "female" includes "male".
        if token_set.intersection(female_terms) or compact in female_terms:
            return "FEMALE"
        if token_set.intersection(male_terms) or compact in male_terms:
            return "MALE"
        if token_set.intersection(other_terms) or compact in other_terms:
            return "OTHER"
        return ""

    def _extract_booking_date(self, normalized: str) -> Optional[date]:
        today = self.today_provider()
        normalized_digits_parts = []
        for character in normalized:
            if not character.isdigit():
                normalized_digits_parts.append(character)
                continue
            try:
                normalized_digits_parts.append(str(unicodedata.digit(character)))
            except (TypeError, ValueError):
                normalized_digits_parts.append(character)
        # Callers normally pass normalized text, but keep this parser safe for
        # direct use as well (for example, "23 August").
        normalized_digits = self._normalize("".join(normalized_digits_parts))
        if self._contains_any(
            normalized_digits,
            (
                "tomorrow", "tomoro", "tomarrow", "kal", "कल", "टुमॉरो",
                "टुमारो", "टुमरो", "टोमॉरो", "अगले दिन", "agle din", "next day",
            ),
        ):
            return today + timedelta(days=1)
        if self._contains_any(normalized_digits, ("day after tomorrow", "parso", "परसों")):
            return today + timedelta(days=2)
        if self._contains_any(normalized_digits, ("today", "aaj", "आज", "टुडे", "टुडे")):
            return today

        iso_match = re.search(r"(?<!\d)(20\d{2})[-/](\d{1,2})[-/](\d{1,2})(?!\d)", normalized_digits)
        dmy_match = re.search(r"(?<!\d)(\d{1,2})[-/](\d{1,2})[-/](20\d{2})(?!\d)", normalized_digits)
        match = iso_match or dmy_match
        if match:
            try:
                if iso_match:
                    return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date()
                return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1))).date()
            except ValueError:
                return None

        month_numbers = {
            "january": 1, "jan": 1, "जनवरी": 1,
            "february": 2, "feb": 2, "फरवरी": 2,
            "march": 3, "mar": 3, "मार्च": 3,
            "april": 4, "apr": 4, "अप्रैल": 4,
            "may": 5, "मई": 5,
            "june": 6, "jun": 6, "जून": 6,
            "july": 7, "jul": 7, "जुलाई": 7,
            "august": 8, "aug": 8, "अगस्त": 8,
            "september": 9, "sep": 9, "sept": 9, "सितंबर": 9, "सितम्बर": 9,
            "october": 10, "oct": 10, "अक्टूबर": 10,
            "november": 11, "nov": 11, "नवंबर": 11, "नवम्बर": 11,
            "december": 12, "dec": 12, "दिसंबर": 12, "दिसम्बर": 12,
        }
        month_pattern = "|".join(re.escape(month) for month in month_numbers)
        named_match = re.search(
            r"(?<!\d)(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?("
            + month_pattern
            + r")(?:\s+(20\d{2}))?(?!\d)",
            normalized_digits,
        )
        if not named_match:
            return None
        try:
            day = int(named_match.group(1))
            month = month_numbers[named_match.group(2)]
            year = int(named_match.group(3) or today.year)
            return date(year, month, day)
        except (KeyError, ValueError):
            return None

    @staticmethod
    def _is_affirmative(normalized: str) -> bool:
        """Return True only for a clear confirmation of an OTP SMS.

        Never use substring matching here: the old short alias ``ha`` could
        accept words such as ``nahin`` or ``what`` by accident.  A positive
        answer only triggers the verification SMS, but that still reserves
        clinic capacity, so ambiguity must remain a retry rather than an
        action.
        """
        text = VoiceReceptionist._normalize(normalized)
        tokens = VoiceReceptionist._booking_tokens(text)
        token_set = set(tokens)
        compact = "".join(tokens)

        negative_terms = {
            "no", "nope", "nahin", "nahi", "na", "mat", "cancel", "stop", "not",
            "नहीं", "नही", "ना", "मत", "रद्द", "कैंसल",
        }
        if token_set.intersection(negative_terms) or "do not" in text or "don't" in text or "dont" in compact:
            return False

        affirmative_terms = {
            "yes", "yeah", "yep", "yup", "okay", "ok", "haan", "ha", "haanji",
            "hanji", "confirm", "confirmed", "sure",
            "हाँ", "हां", "हा", "यस", "येस", "येस्स", "ओके", "ओक्के", "कन्फर्म", "कन्फर्म्ड",
        }
        if token_set.intersection(affirmative_terms) or compact in affirmative_terms:
            return True

        affirmative_phrases = (
            "हाँ जी", "हां जी", "जी हाँ", "जी हां", "ठीक है", "कर दो", "कर दीजिए",
            "भेज दो", "भेज दीजिए", "yes please", "yeah please", "send otp", "send the otp",
            "please send", "please confirm", "haan bhej do", "haan kar do", "kar dijiye",
        )
        return any(phrase in text for phrase in affirmative_phrases)

    @staticmethod
    def _booking_tokens(text: str) -> tuple[str, ...]:
        """Return safe words for short booking-answer classifiers."""
        return tuple(
            token.rstrip("्")
            for token in re.findall(r"[a-z]+|[\u0900-\u097f]+|\d+", (text or "").casefold())
            if token.rstrip("्")
        )

    @staticmethod
    def _otp_error_requires_new_booking(error: str) -> bool:
        normalized = VoiceReceptionist._normalize(error)
        return VoiceReceptionist._contains_any(
            normalized,
            ("expired", "no longer valid", "too many invalid", "not found", "no longer awaiting"),
        )

    @staticmethod
    def _otp_error_text(error: str, hindi: bool) -> str:
        safe_error = (error or "").strip()
        if safe_error:
            return safe_error
        return (
            "OTP verify नहीं हो पाया। कृपया private input में फिर से try करें।"
            if hindi
            else "The OTP could not be verified. Please try again in the private input."
        )

    @staticmethod
    def _booking_error_text(error: str, hindi: bool) -> str:
        normalized = VoiceReceptionist._normalize(error)
        if "sms otp delivery is not configured" in normalized:
            return (
                "SMS OTP service configure नहीं है। कृपया secure booking page इस्तेमाल करें या clinic team से संपर्क करें।"
                if hindi
                else "Voice booking cannot start because SMS OTP delivery is not configured. "
                "Please use the secure booking page or ask the clinic to configure SMS verification."
            )
        if hindi:
            return "कृपया secure booking page इस्तेमाल करें या थोड़ी देर बाद फिर कोशिश करें।"
        return error or "The appointment request could not be started. Please use the secure booking page."

    @staticmethod
    def _booking_reply(text: str, hindi: bool, *, intent: str) -> VoiceReply:
        return VoiceReply(
            text=text,
            language="hi" if hindi else "en",
            intent=intent,
            redact_transcript=True,
        )

    @staticmethod
    def _private_otp_reply(text: str, hindi: bool, *, intent: str) -> VoiceReply:
        return VoiceReply(
            text=text,
            language="hi" if hindi else "en",
            intent=intent,
            redact_transcript=True,
            needs_private_otp=True,
        )

    def _resolve_date(self, normalized: str) -> date:
        today = self.today_provider()
        if self._contains_any(normalized, ("tomorrow", "kal", "कल")):
            self.state.selected_date = today + timedelta(days=1)
        elif self._contains_any(normalized, ("today", "aaj", "आज")):
            self.state.selected_date = today
        elif self.state.selected_date is None:
            self.state.selected_date = today
        return self.state.selected_date

    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").casefold()).strip()

    @staticmethod
    def _contains_any(text: str, terms: tuple) -> bool:
        return any(term in text for term in terms)

    @staticmethod
    def _is_hindi(text: str, detected_language: str) -> bool:
        if any("\u0900" <= character <= "\u097f" for character in text):
            return True

        normalized = VoiceReceptionist._normalize(text)
        roman_hindi_terms = (
            "aaj", "kal", "kya", "hai", "haan", "haanji", "ji", "sahab", "saab", "milega",
            "chahiye", "kar do", "karna", "mera", "meri", "naam", "dawai", "dawa", "kitne",
        )
        if VoiceReceptionist._contains_any(normalized, roman_hindi_terms):
            return True

        english_signals = (
            "i want", "can i", "could i", "would like", "please", "what is", "when is", "where is",
            "how do", "book an appointment", "book appointment", "my appointment", "thank you",
        )
        if VoiceReceptionist._contains_any(normalized, english_signals):
            return False
        return detected_language == "hi"

    @staticmethod
    def _date_label(value: date) -> str:
        return value.strftime("%d-%m-%Y")

    def _availability(self, selected_date: date) -> AppointmentAvailability:
        return self.gateway.appointment_availability(selected_date)

    def _doctor_schedule_reply(self, selected_date: date, hindi: bool) -> VoiceReply:
        availability = self._availability(selected_date)
        date_label = self._date_label(selected_date)
        if availability.booking_open:
            text = (
                "{} के लिए online appointment booking खुली है। Clinic arrival window {} है। "
                "Doctor की अलग daily schedule अभी application में configure नहीं है, इसलिए मैं गलत availability नहीं बताऊँगी।"
            ).format(date_label, availability.arrival_window_label)
            english = (
                "Online appointment booking is open for {}. The clinic arrival window is {}. "
                "The doctor's separate daily schedule is not configured in the application, so I will not give an incorrect availability answer."
            ).format(date_label, availability.arrival_window_label)
        else:
            text = (
                "{} के लिए online booking अभी उपलब्ध नहीं है। {}"
            ).format(date_label, availability.message or "कृपया clinic reception से संपर्क करें।")
            english = (
                "Online booking is not available for {}. {}"
            ).format(date_label, availability.message or "Please contact clinic reception.")
        return VoiceReply(text=text if hindi else english, language="hi" if hindi else "en", intent="doctor_schedule")

    def _appointment_reply(self, selected_date: date, hindi: bool, booking_request: bool) -> VoiceReply:
        availability = self._availability(selected_date)
        date_label = self._date_label(selected_date)
        if not availability.booking_open:
            text = "{} के लिए online appointment उपलब्ध नहीं है। {}".format(
                date_label, availability.message or "कृपया clinic reception से संपर्क करें।"
            )
            english = "Online appointments are unavailable for {}. {}".format(
                date_label, availability.message or "Please contact clinic reception."
            )
            return VoiceReply(text=text if hindi else english, language="hi" if hindi else "en", intent="appointment_unavailable")

        if availability.normal_remaining > 0:
            base_hi = (
                "{} के लिए {} सामान्य appointments उपलब्ध हैं। Arrival window {} है। "
                "यह individual time slot नहीं है; token reception पर मिलेगा।"
            ).format(date_label, availability.normal_remaining, availability.arrival_window_label)
            base_en = (
                "{} regular appointments are available for {}. The arrival window is {}. "
                "This is not an individual time slot; the token is issued at reception."
            ).format(availability.normal_remaining, date_label, availability.arrival_window_label)
        elif availability.priority_available:
            base_hi = (
                "{} के लिए regular appointments भर चुके हैं। Limited priority request उपलब्ध है। "
                "Arrival window {} है।"
            ).format(date_label, availability.arrival_window_label)
            base_en = (
                "Regular appointments are full for {}. A limited priority request is available. "
                "The arrival window is {}."
            ).format(date_label, availability.arrival_window_label)
        else:
            base_hi = "{} के लिए appointment capacity भर चुकी है।".format(date_label)
            base_en = "Appointment capacity is full for {}.".format(date_label)

        if booking_request:
            base_hi += " Booking के लिए registered mobile पर OTP verification जरूरी है। अभी secure booking page इस्तेमाल करें।"
            base_en += " Booking requires OTP verification on the registered mobile. Please use the secure booking page for now."
        return VoiceReply(text=base_hi if hindi else base_en, language="hi" if hindi else "en", intent="appointment")

    def _fee_reply(self, selected_date: date, hindi: bool) -> VoiceReply:
        availability = self._availability(selected_date)
        normal_fee = "₹{:,.0f}".format(availability.normal_fee)
        priority_fee = "₹{:,.0f}".format(availability.priority_fee)
        return VoiceReply(
            text=(
                "Normal appointment fee {} है। Priority request fee {} है।"
                .format(normal_fee, priority_fee)
                if hindi
                else "The normal appointment fee is {}. The priority request fee is {}."
                .format(normal_fee, priority_fee)
            ),
            language="hi" if hindi else "en",
            intent="fees",
        )

    @staticmethod
    def _report_reply(hindi: bool) -> VoiceReply:
        return VoiceReply(
            text=(
                "Lab report की जानकारी सिर्फ registered mobile के OTP verification के बाद दी जा सकती है। "
                "Secure My Lab Reports portal इस्तेमाल करें।"
                if hindi
                else "Lab-report information is available only after OTP verification of the registered mobile. "
                "Please use the secure My Lab Reports portal."
            ),
            language="hi" if hindi else "en",
            intent="lab_report",
            requires_otp=True,
        )

    @staticmethod
    def _medical_safety_reply(hindi: bool) -> VoiceReply:
        return VoiceReply(
            text=(
                "दवा या dose बदलने की सलाह मैं नहीं दे सकती। कृपया doctor या clinic team से बात करें।"
                if hindi
                else "I cannot advise changing a medicine or dose. Please speak with the doctor or clinic team."
            ),
            language="hi" if hindi else "en",
            intent="medical_safety",
        )
