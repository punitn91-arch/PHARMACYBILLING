"""Patient identification and server-owned OTP verification sessions."""

from datetime import datetime, timedelta
import hashlib
import re
import secrets

from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy.exc import IntegrityError

try:
    from ..models import (
        db,
        Patient,
        PortalOtpChallenge,
        PatientVerificationIntent,
        PatientVerificationSession,
    )
    from .public_portal import mask_mobile, send_portal_otp
except ImportError:  # pragma: no cover
    from models import (
        db,
        Patient,
        PortalOtpChallenge,
        PatientVerificationIntent,
        PatientVerificationSession,
    )
    from services.public_portal import mask_mobile, send_portal_otp


class PatientAccessError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _hash(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def normalize_phone(value, default_country_code="91"):
    raw = str(value or "").strip()
    if not raw or len(raw) > 40:
        raise PatientAccessError("INVALID_REQUEST", "A valid mobile number is required", 400)
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("00"):
        digits = digits[2:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+{}{}".format(default_country_code, digits)
    if len(digits) == 11 and digits.startswith("0") and digits[1] in "6789":
        return "+{}{}".format(default_country_code, digits[1:])
    if len(digits) == 12 and digits.startswith(default_country_code) and digits[2] in "6789":
        return "+{}".format(digits)
    raise PatientAccessError("INVALID_REQUEST", "A valid mobile number is required", 400)


def _patient_for_phone(normalized_phone):
    local = normalized_phone[-10:]
    candidates = {normalized_phone, normalized_phone.lstrip("+"), local, "0{}".format(local)}
    return Patient.query.filter(Patient.mobile.in_(sorted(candidates))).order_by(Patient.id.asc()).first()


def create_identification_intent(
    db_session,
    *,
    api_client_id,
    mobile,
    claimed_name=None,
    call_id=None,
    session_id=None,
    now=None,
):
    now = now or datetime.utcnow()
    normalized = normalize_phone(mobile)
    patient = _patient_for_phone(normalized)
    safe_name = str(claimed_name or "").strip()
    if safe_name and (len(safe_name) < 2 or len(safe_name) > 120):
        raise PatientAccessError("INVALID_REQUEST", "Patient name must be 2-120 characters", 400)
    raw_reference = "pvi_{}".format(secrets.token_urlsafe(32))
    intent = PatientVerificationIntent(
        reference_hash=_hash(raw_reference),
        api_client_id=api_client_id,
        patient_id=patient.id if patient else None,
        mobile=normalized,
        claimed_name=safe_name or None,
        external_call_id=call_id,
        external_session_id=session_id,
        expires_at=now + timedelta(minutes=10),
        created_at=now,
    )
    db_session.add(intent)
    db_session.flush()
    # Identical public shape prevents a bulk lookup endpoint from becoming a
    # patient directory. OTP delivery is the proof-of-possession step.
    return intent, raw_reference, {
        "identification_ref": raw_reference,
        "verification_required": True,
        "delivery_target": mask_mobile(normalized),
        "expires_in": 600,
    }


def _intent_for_reference(reference, api_client_id, now=None):
    now = now or datetime.utcnow()
    intent = PatientVerificationIntent.query.filter_by(
        reference_hash=_hash(reference),
        api_client_id=api_client_id,
    ).first()
    if not intent or intent.used_at or intent.expires_at <= now:
        raise PatientAccessError("VERIFICATION_REQUIRED", "Patient verification is required", 401)
    return intent


def send_verification_otp(
    db_session,
    *,
    reference,
    api_client_id,
    otp_expiry_seconds=300,
    max_attempts=5,
    resend_cooldown_seconds=60,
    is_production=False,
    testing=False,
    now=None,
):
    now = now or datetime.utcnow()
    intent = _intent_for_reference(reference, api_client_id, now)
    context_ref = "AI_VERIFY:{}".format(intent.id)
    latest = PortalOtpChallenge.query.filter_by(
        purpose="AI_PATIENT_VERIFY",
        context_ref=context_ref,
    ).order_by(PortalOtpChallenge.sent_at.desc(), PortalOtpChallenge.id.desc()).first()
    if latest and latest.sent_at and (now - latest.sent_at).total_seconds() < resend_cooldown_seconds:
        raise PatientAccessError("OTP_RATE_LIMITED", "Please wait before requesting another OTP", 429)
    recent_count = PortalOtpChallenge.query.filter(
        PortalOtpChallenge.mobile == intent.mobile,
        PortalOtpChallenge.sent_at >= now - timedelta(minutes=15),
    ).count()
    if recent_count >= 3:
        raise PatientAccessError("OTP_RATE_LIMITED", "Too many OTP requests", 429)

    # Unknown callers without registration details receive the same public
    # response shape but no delivery, preventing patient enumeration.
    if intent.patient_id is None and not intent.claimed_name:
        return {"status": "OTP_SENT", "expires_in": int(otp_expiry_seconds)}

    code = "{:06d}".format(secrets.randbelow(900000) + 100000)
    challenge = PortalOtpChallenge(
        purpose="AI_PATIENT_VERIFY",
        context_ref=context_ref,
        mobile=intent.mobile,
        otp_hash=generate_password_hash(code, method="pbkdf2:sha256"),
        attempt_count=0,
        max_attempts=max(1, min(int(max_attempts), 10)),
        sent_at=now,
        expires_at=now + timedelta(seconds=max(60, min(int(otp_expiry_seconds), 900))),
        created_at=now,
    )
    db_session.add(challenge)
    db_session.flush()
    sent, development_code, error = send_portal_otp(
        mobile=intent.mobile,
        code=code,
        purpose="Patient Access",
        is_production=is_production,
        testing=testing,
    )
    if not sent:
        db_session.rollback()
        raise PatientAccessError("NOTIFICATION_FAILED", error or "Unable to send OTP", 503)
    db_session.commit()
    result = {"status": "OTP_SENT", "expires_in": int(otp_expiry_seconds)}
    if testing and development_code:
        result["development_otp"] = development_code
    return result


def verify_otp(
    db_session,
    *,
    reference,
    otp_code,
    api_client_id,
    session_ttl_seconds=900,
    now=None,
):
    now = now or datetime.utcnow()
    intent = _intent_for_reference(reference, api_client_id, now)
    submitted = str(otp_code or "").strip()
    if not re.fullmatch(r"\d{6}", submitted):
        raise PatientAccessError("OTP_INVALID", "The verification code is invalid", 400)
    if intent.patient_id is None and not intent.claimed_name:
        raise PatientAccessError("OTP_INVALID", "The verification code is invalid", 400)
    challenge = PortalOtpChallenge.query.filter_by(
        purpose="AI_PATIENT_VERIFY",
        context_ref="AI_VERIFY:{}".format(intent.id),
    ).order_by(PortalOtpChallenge.sent_at.desc(), PortalOtpChallenge.id.desc()).first()
    if not challenge or challenge.expires_at <= now:
        raise PatientAccessError("OTP_EXPIRED", "The verification code has expired", 400)
    if challenge.verified_at or challenge.attempt_count >= challenge.max_attempts:
        raise PatientAccessError("OTP_INVALID", "The verification code is invalid", 400)
    if not check_password_hash(challenge.otp_hash, submitted):
        challenge.attempt_count = int(challenge.attempt_count or 0) + 1
        db_session.commit()
        raise PatientAccessError("OTP_INVALID", "The verification code is invalid", 400)

    patient_id = intent.patient_id
    if patient_id is None:
        existing_patient = _patient_for_phone(intent.mobile)
        if existing_patient is not None:
            patient_id = existing_patient.id
        else:
            try:
                with db_session.begin_nested():
                    patient = Patient(name=intent.claimed_name, mobile=intent.mobile)
                    db_session.add(patient)
                    db_session.flush()
                    patient_id = patient.id
            except IntegrityError:
                existing_patient = _patient_for_phone(intent.mobile)
                if existing_patient is None:
                    raise PatientAccessError("INTERNAL_ERROR", "Patient registration failed", 500)
                patient_id = existing_patient.id
        intent.patient_id = patient_id

    raw_session = "pvs_{}".format(secrets.token_urlsafe(48))
    expires_in = max(300, min(int(session_ttl_seconds), 3600))
    verified_session = PatientVerificationSession(
        token_hash=_hash(raw_session),
        api_client_id=api_client_id,
        patient_id=patient_id,
        mobile=intent.mobile,
        purpose="AI_PATIENT_ACCESS",
        otp_challenge_id=challenge.id,
        external_call_id=intent.external_call_id,
        external_session_id=intent.external_session_id,
        verified_at=now,
        expires_at=now + timedelta(seconds=expires_in),
        created_at=now,
    )
    challenge.verified_at = now
    challenge.used_at = now
    intent.used_at = now
    db_session.add(verified_session)
    db_session.commit()
    patient = db_session.get(Patient, patient_id)
    return {
        "patient_session_token": raw_session,
        "expires_in": expires_in,
        "patient": {
            "id": patient.id,
            "display_name": patient.name,
            "mobile": mask_mobile(intent.mobile),
        },
    }


def require_patient_session(raw_token, api_client_id, *, patient_id=None, now=None):
    now = now or datetime.utcnow()
    session = PatientVerificationSession.query.filter_by(
        token_hash=_hash(raw_token),
        api_client_id=api_client_id,
    ).first()
    if (
        not session
        or session.revoked_at is not None
        or session.expires_at <= now
        or (patient_id is not None and int(session.patient_id) != int(patient_id))
    ):
        raise PatientAccessError("VERIFICATION_REQUIRED", "Patient verification is required", 401)
    session.last_used_at = now
    return session
