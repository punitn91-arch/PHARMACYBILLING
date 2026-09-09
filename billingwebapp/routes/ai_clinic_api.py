"""Public-safe clinic, doctor, schedule, knowledge and reception endpoints."""

from datetime import datetime

from flask import request

try:
    from ..models import ClinicLocation, Clinician
    from ..services.ai_clinic_service import (
        clinic_information,
        clinic_live_status,
        doctor_schedule_resolution,
        find_next_available_date,
        knowledge_entries,
        reception_status,
        resolve_schedule,
        schedule_payload,
        serialize_clinician,
        serialize_location,
    )
    from .ai_api import AIAPIError, ai_api_bp, nonnegative_int, positive_int, require_ai_scope, success_response
except ImportError:  # pragma: no cover
    from models import ClinicLocation, Clinician
    from services.ai_clinic_service import (
        clinic_information,
        clinic_live_status,
        doctor_schedule_resolution,
        find_next_available_date,
        knowledge_entries,
        reception_status,
        resolve_schedule,
        schedule_payload,
        serialize_clinician,
        serialize_location,
    )
    from routes.ai_api import AIAPIError, ai_api_bp, nonnegative_int, positive_int, require_ai_scope, success_response


def _date_arg(name="date", required=True):
    raw = str(request.args.get(name) or "").strip()
    if not raw and not required:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise AIAPIError("INVALID_REQUEST", "{} must use YYYY-MM-DD".format(name), 400)


def _location(location_id):
    location = ClinicLocation.query.filter_by(id=location_id, is_active=True).first()
    if not location:
        raise AIAPIError("INVALID_REQUEST", "Clinic location was not found", 404)
    return location


def _doctor(doctor_id):
    doctor = Clinician.query.filter_by(id=doctor_id, is_active=True).first()
    if not doctor:
        raise AIAPIError("DOCTOR_NOT_FOUND", "Doctor was not found", 404)
    return doctor


@ai_api_bp.get("/clinic")
@ai_api_bp.get("/clinic/info")
@require_ai_scope("clinic:read")
def get_clinic_information():
    return success_response(clinic_information())


@ai_api_bp.get("/locations")
@require_ai_scope("clinic:read")
def get_locations():
    rows = ClinicLocation.query.filter_by(is_active=True).order_by(
        ClinicLocation.is_default.desc(), ClinicLocation.display_name.asc()
    ).all()
    return success_response({"items": [serialize_location(row) for row in rows]})


@ai_api_bp.get("/clinic/status")
@require_ai_scope("clinic:read")
def get_clinic_status():
    location_id = request.args.get("location_id")
    if location_id:
        location = _location(positive_int(location_id, "location_id"))
        payload = clinic_live_status(location)
        payload["next_available"] = find_next_available_date(location.id)
        return success_response(payload)
    locations = ClinicLocation.query.filter_by(is_active=True).order_by(
        ClinicLocation.is_default.desc(), ClinicLocation.display_name.asc()
    ).all()
    statuses = []
    for location in locations:
        row = clinic_live_status(location)
        row["next_available"] = find_next_available_date(location.id)
        statuses.append(row)
    return success_response(
        {
            "currently_open": any(row["currently_open"] for row in statuses),
            "locations": statuses,
        }
    )


@ai_api_bp.get("/doctors")
@require_ai_scope("doctor:read")
def get_doctors():
    query = Clinician.query.filter_by(is_active=True)
    specialty = str(request.args.get("specialty") or "").strip()
    if specialty:
        query = query.filter(Clinician.specialty.ilike("%{}%".format(specialty[:120])))
    location_id = request.args.get("location_id")
    limit = min(100, positive_int(request.args.get("limit", 50), "limit"))
    offset = nonnegative_int(request.args.get("offset", 0), "offset")
    rows = query.order_by(Clinician.display_name.asc()).offset(offset).limit(limit).all()
    serialized = [serialize_clinician(row) for row in rows]
    if location_id:
        parsed_location = positive_int(location_id, "location_id")
        serialized = [row for row in serialized if parsed_location in row["location_ids"]]
    return success_response({"items": serialized, "count": len(serialized), "offset": offset})


@ai_api_bp.get("/doctors/<int:doctor_id>/schedule")
@require_ai_scope("doctor:read")
def get_doctor_schedule(doctor_id):
    return success_response(_doctor_schedule_payload(doctor_id))


def _doctor_schedule_payload(doctor_id):
    doctor = _doctor(doctor_id)
    location = _location(positive_int(request.args.get("location_id"), "location_id"))
    target_date = _date_arg()
    result, rule, _ = doctor_schedule_resolution(location.id, doctor.id, target_date)
    payload = schedule_payload(
        result,
        target_date,
        rule=rule,
        location_id=location.id,
        clinician_id=doctor.id,
    )
    payload["next_available"] = find_next_available_date(
        location.id, clinician_id=doctor.id, start_date=target_date
    )
    return payload


@ai_api_bp.get("/doctors/<int:doctor_id>/availability")
@require_ai_scope("doctor:read")
def get_doctor_availability(doctor_id):
    # Detailed appointment slots live at /appointments/slots; this endpoint
    # provides a low-cost schedule-level answer for conversation planning.
    payload = _doctor_schedule_payload(doctor_id)
    return success_response(
        {
            **payload,
            "available": payload.get("status") == "OPEN" and bool(payload.get("arrival_window")),
        }
    )


@ai_api_bp.get("/knowledge")
@require_ai_scope("clinic:read")
def get_knowledge():
    offset = nonnegative_int(request.args.get("offset", 0), "offset")
    items = knowledge_entries(
        category=str(request.args.get("category") or "").strip() or None,
        language=str(request.args.get("language") or "").strip().lower() or None,
        limit=min(100, positive_int(request.args.get("limit", 50), "limit")),
        offset=offset,
    )
    return success_response({"items": items, "count": len(items), "offset": offset})


@ai_api_bp.get("/reception/status")
@require_ai_scope("clinic:read")
def get_reception_status():
    location = _location(positive_int(request.args.get("location_id"), "location_id"))
    return success_response(reception_status(location))


@ai_api_bp.get("/locations/<int:location_id>/share")
@require_ai_scope("clinic:read")
def get_location_share(location_id):
    location = _location(location_id)
    return success_response(
        {
            "location": serialize_location(location),
            "share": {
                "maps_url": location.maps_url,
                "address_text": location.public_address,
            },
        }
    )
