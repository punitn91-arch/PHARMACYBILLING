"""Transactional callback and complaint intake for the call assistant."""

from datetime import datetime
import secrets

try:
    from ..models import CallbackRequest, Complaint
    from .ai_patient_service import normalize_phone
except ImportError:  # pragma: no cover
    from models import CallbackRequest, Complaint
    from services.ai_patient_service import normalize_phone


class OperationsError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _bounded(value, maximum, *, required=False, field_name="value"):
    cleaned = str(value or "").strip()
    if required and not cleaned:
        raise OperationsError("INVALID_REQUEST", "{} is required".format(field_name), 400)
    if len(cleaned) > maximum:
        raise OperationsError(
            "INVALID_REQUEST", "{} must be at most {} characters".format(field_name, maximum), 400
        )
    return cleaned or None


def create_callback(
    db_session,
    *,
    mobile,
    reason,
    caller_name=None,
    category=None,
    priority="NORMAL",
    ai_summary=None,
    preferred_at=None,
    patient_id=None,
    call_id=None,
    session_id=None,
):
    normalized_mobile = normalize_phone(mobile)
    normalized_priority = str(priority or "NORMAL").strip().upper()
    if normalized_priority not in {"LOW", "NORMAL", "HIGH", "URGENT"}:
        raise OperationsError("INVALID_REQUEST", "Invalid callback priority", 400)
    row = CallbackRequest(
        callback_ref="CB-{}".format(secrets.token_hex(8).upper()),
        patient_id=patient_id,
        caller_name=_bounded(caller_name, 120),
        mobile=normalized_mobile,
        reason=_bounded(reason, 500, required=True, field_name="reason"),
        category=_bounded(category, 80),
        priority=normalized_priority,
        ai_summary=_bounded(ai_summary, 1000),
        preferred_at=preferred_at,
        status="PENDING",
        source="AI_CALL",
        external_call_id=call_id,
        external_session_id=session_id,
    )
    db_session.add(row)
    db_session.flush()
    return row


def create_complaint(
    db_session,
    *,
    mobile,
    category,
    summary,
    details=None,
    priority="NORMAL",
    caller_name=None,
    patient_id=None,
    call_id=None,
    session_id=None,
):
    normalized_priority = str(priority or "NORMAL").strip().upper()
    if normalized_priority not in {"LOW", "NORMAL", "HIGH", "URGENT"}:
        raise OperationsError("INVALID_REQUEST", "Invalid complaint priority", 400)
    row = Complaint(
        complaint_ref="CMP-{}".format(secrets.token_hex(8).upper()),
        patient_id=patient_id,
        caller_name=_bounded(caller_name, 120),
        mobile=normalize_phone(mobile),
        category=_bounded(category, 80, required=True, field_name="category"),
        summary=_bounded(summary, 240, required=True, field_name="summary"),
        details=_bounded(details, 4000),
        priority=normalized_priority,
        status="OPEN",
        source="AI_CALL",
        external_call_id=call_id,
        external_session_id=session_id,
    )
    db_session.add(row)
    db_session.flush()
    return row
