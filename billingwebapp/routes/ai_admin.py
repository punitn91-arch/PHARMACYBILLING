"""Administrator controls for external AI API machine identities."""

from datetime import datetime, timedelta
from functools import wraps
import hmac
import json
import secrets

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

try:
    from ..models import (
        db, AIAPIClient, AIAccessToken, AIAPIRequestAudit, AuditLog, User,
        ClinicProfile, ClinicLocation, Clinician, ClinicScheduleRule,
        ClinicScheduleException, AppointmentSlotBlock, ReceptionSchedule,
        ReceptionScheduleOverride, ClinicKnowledgeEntry, CallbackRequest,
        Complaint, NotificationTemplate, NotificationDelivery, LabTest,
    )
    from ..services.ai_auth_service import (
        ALLOWED_AI_SCOPES,
        create_api_client,
        deserialize_allowed_ips,
        deserialize_scopes,
        normalize_allowed_ips,
        normalize_scopes,
        rotate_api_client_secret,
        serialize_allowed_ips,
        serialize_scopes,
    )
    from ..services.ai_clinic_service import _json_list
except ImportError:  # pragma: no cover - direct ``python app.py`` execution
    from models import (
        db, AIAPIClient, AIAccessToken, AIAPIRequestAudit, AuditLog, User,
        ClinicProfile, ClinicLocation, Clinician, ClinicScheduleRule,
        ClinicScheduleException, AppointmentSlotBlock, ReceptionSchedule,
        ReceptionScheduleOverride, ClinicKnowledgeEntry, CallbackRequest,
        Complaint, NotificationTemplate, NotificationDelivery, LabTest,
    )
    from services.ai_auth_service import (
        ALLOWED_AI_SCOPES,
        create_api_client,
        deserialize_allowed_ips,
        deserialize_scopes,
        normalize_allowed_ips,
        normalize_scopes,
        rotate_api_client_secret,
        serialize_allowed_ips,
        serialize_scopes,
    )
    from services.ai_clinic_service import _json_list


ai_admin_bp = Blueprint("ai_admin", __name__, url_prefix="/admin/ai-integration")
_CSRF_SESSION_KEY = "_ai_admin_csrf"


def _admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user_id = session.get("user_id")
        user = db.session.get(User, user_id) if user_id else None
        if user is None or not bool(user.is_active) or user.role != "admin":
            if user_id:
                flash("Only an administrator can manage AI Integration.", "danger")
                return redirect("/")
            return redirect("/login")
        return view(*args, **kwargs)

    return wrapped


def _csrf_token():
    token = session.get(_CSRF_SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[_CSRF_SESSION_KEY] = token
    return token


def _require_csrf():
    supplied = str(request.form.get("csrf_token") or "")
    expected = str(session.get(_CSRF_SESSION_KEY) or "")
    if not supplied or not expected or not hmac.compare_digest(supplied, expected):
        abort(400, description="Invalid form token")


def _audit_admin_action(action, client, extra=None):
    db.session.add(
        AuditLog(
            user=session.get("username"),
            action=action,
            entity_type="AI_API_CLIENT",
            entity_id=client.id,
            ref_code=client.client_id,
            extra_json=extra,
        )
    )


def _form_int(name, *, minimum=None, maximum=None, required=False):
    raw = str(request.form.get(name) or "").strip()
    if not raw and not required:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ValueError("{} must be a whole number".format(name.replace("_", " ").title()))
    if minimum is not None and value < minimum:
        raise ValueError("{} is below the allowed minimum".format(name.replace("_", " ").title()))
    if maximum is not None and value > maximum:
        raise ValueError("{} exceeds the allowed maximum".format(name.replace("_", " ").title()))
    return value


def _form_date(name, required=False):
    raw = str(request.form.get(name) or "").strip()
    if not raw and not required:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("{} must use YYYY-MM-DD".format(name.replace("_", " ").title()))


def _form_time(name, required=False):
    raw = str(request.form.get(name) or "").strip()
    if not raw and not required:
        return None
    try:
        return datetime.strptime(raw, "%H:%M").time()
    except ValueError:
        raise ValueError("{} must use HH:MM".format(name.replace("_", " ").title()))


def _format_clock_12h(value):
    """Present stored 24-hour schedule values as a simple clinic-facing time."""
    raw_value = str(value or "").strip()
    try:
        return datetime.strptime(raw_value, "%H:%M").strftime("%I:%M %p")
    except ValueError:
        return raw_value


def _date_ranges_overlap(left_start, left_end, right_start, right_end):
    minimum = datetime.min.date()
    maximum = datetime.max.date()
    return (left_start or minimum) <= (right_end or maximum) and (
        right_start or minimum
    ) <= (left_end or maximum)


def _form_bool(name):
    return str(request.form.get(name) or "").lower() in {"1", "true", "yes", "on"}


def _json_array_text(value):
    items = [item.strip() for item in str(value or "").replace("\n", ",").split(",") if item.strip()]
    return json.dumps(items, separators=(",", ":"))


def _location_type(value):
    normalized = str(value or "CLINIC").strip().upper()
    if normalized not in {"CLINIC", "HOSPITAL", "BRANCH"}:
        raise ValueError("Location type must be Clinic, Hospital or Branch")
    return normalized


def _location_form_values():
    code = str(request.form.get("code") or "").strip().upper()
    name = str(request.form.get("display_name") or "").strip()
    if not code or len(code) > 40 or not name or len(name) > 120:
        raise ValueError("Valid location code and name are required.")
    location_type = _location_type(request.form.get("location_type"))
    is_default = _form_bool("is_default")
    if location_type == "HOSPITAL" and is_default:
        raise ValueError("Hospital is information-only and cannot be the default booking location.")
    return {
        "code": code,
        "display_name": name,
        "location_type": location_type,
        "public_address": str(request.form.get("public_address") or "").strip()[:255] or None,
        "public_phone": str(request.form.get("public_phone") or "").strip()[:30] or None,
        "landmark": str(request.form.get("landmark") or "").strip()[:160] or None,
        "city": str(request.form.get("city") or "").strip()[:100] or None,
        "state": str(request.form.get("state") or "").strip()[:100] or None,
        "postal_code": str(request.form.get("postal_code") or "").strip()[:12] or None,
        "maps_url": str(request.form.get("maps_url") or "").strip()[:500] or None,
        "parking_information": str(request.form.get("parking_information") or "").strip()[:500] or None,
        "is_default": is_default,
    }


def _make_only_default_location(location):
    if not location.is_default:
        return
    ClinicLocation.query.filter(ClinicLocation.id != location.id).update(
        {ClinicLocation.is_default: False}, synchronize_session=False
    )


def _ensure_active_default_location():
    active_default = ClinicLocation.query.filter_by(is_active=True, is_default=True).first()
    if active_default:
        return
    replacement = ClinicLocation.query.filter(
        ClinicLocation.is_active.is_(True),
        ClinicLocation.location_type != "HOSPITAL",
    ).order_by(ClinicLocation.id.asc()).first()
    if replacement:
        replacement.is_default = True


def _operation_redirect(message, category="success"):
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        normalized_category = str(category or "info").lower()
        return jsonify(
            {
                "ok": normalized_category not in {"danger", "error"},
                "messages": [
                    {"message": str(message), "category": normalized_category}
                ],
            }
        )
    flash(message, category)
    return redirect(url_for("ai_admin.overview"))


def _secret_response(message, client_id, raw_secret):
    """Return a one-time client secret without navigating an AJAX caller."""
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(
            {
                "ok": True,
                "messages": [{"message": message, "category": "success"}],
                "secret": {
                    "client_id": str(client_id),
                    "client_secret": str(raw_secret),
                },
            }
        )
    flash(message, "success")
    return render_template(
        "ai_integration.html",
        **_dashboard_context(new_secret=raw_secret, secret_client_id=client_id),
    )


def _dashboard_context(new_secret=None, secret_client_id=None):
    clients = AIAPIClient.query.order_by(AIAPIClient.created_at.desc(), AIAPIClient.id.desc()).all()
    recent_audits = AIAPIRequestAudit.query.order_by(
        AIAPIRequestAudit.created_at.desc(),
        AIAPIRequestAudit.id.desc(),
    ).limit(50).all()
    locations = ClinicLocation.query.order_by(
        ClinicLocation.is_active.desc(), ClinicLocation.display_name.asc()
    ).all()
    clinicians = Clinician.query.order_by(Clinician.is_active.desc(), Clinician.display_name.asc()).all()
    callback_query = CallbackRequest.query
    callback_status = str(request.args.get("callback_status") or "").upper()
    callback_priority = str(request.args.get("callback_priority") or "").upper()
    if callback_status in {"PENDING", "ASSIGNED", "CONTACTED", "COMPLETED", "CANCELLED"}:
        callback_query = callback_query.filter(CallbackRequest.status == callback_status)
    if callback_priority in {"LOW", "NORMAL", "HIGH", "URGENT"}:
        callback_query = callback_query.filter(CallbackRequest.priority == callback_priority)
    complaint_query = Complaint.query
    complaint_status = str(request.args.get("complaint_status") or "").upper()
    complaint_priority = str(request.args.get("complaint_priority") or "").upper()
    if complaint_status in {"OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"}:
        complaint_query = complaint_query.filter(Complaint.status == complaint_status)
    if complaint_priority in {"LOW", "NORMAL", "HIGH", "URGENT"}:
        complaint_query = complaint_query.filter(Complaint.priority == complaint_priority)
    operations_date_from = str(request.args.get("operations_date_from") or "").strip()
    operations_date_to = str(request.args.get("operations_date_to") or "").strip()
    try:
        parsed_from = datetime.strptime(operations_date_from, "%Y-%m-%d") if operations_date_from else None
        parsed_to = (
            datetime.strptime(operations_date_to, "%Y-%m-%d") + timedelta(days=1)
            if operations_date_to
            else None
        )
    except ValueError:
        parsed_from = parsed_to = None
        operations_date_from = operations_date_to = ""
    for model, query_name in ((CallbackRequest, "callback"), (Complaint, "complaint")):
        query = callback_query if query_name == "callback" else complaint_query
        if parsed_from:
            query = query.filter(model.created_at >= parsed_from)
        if parsed_to:
            query = query.filter(model.created_at < parsed_to)
        if query_name == "callback":
            callback_query = query
        else:
            complaint_query = query
    return {
        "api_enabled": bool(current_app.config.get("AI_API_ENABLED", False)),
        "clients": clients,
        "active_client_count": sum(1 for client in clients if client.is_active),
        "recent_audits": recent_audits,
        "failed_request_count": sum(1 for item in recent_audits if item.status_code >= 400),
        "allowed_scopes": sorted(ALLOWED_AI_SCOPES),
        "client_scopes": {client.id: sorted(deserialize_scopes(client.allowed_scopes_json)) for client in clients},
        "client_ips": {client.id: deserialize_allowed_ips(client.allowed_ips_json) for client in clients},
        "csrf_token": _csrf_token(),
        "new_secret": new_secret,
        "secret_client_id": secret_client_id,
        "clinic_profile": ClinicProfile.query.order_by(ClinicProfile.id.asc()).first(),
        "locations": locations,
        "locations_by_id": {row.id: row for row in locations},
        "clinicians": clinicians,
        "clinicians_by_id": {row.id: row for row in clinicians},
        "clinician_detail_display": {
            row.id: {
                "languages": ", ".join(_json_list(row.languages_json)),
                "sub_specialties": ", ".join(_json_list(row.sub_specialties_json)),
                "conditions_treated": ", ".join(_json_list(row.conditions_treated_json)),
                "services_offered": ", ".join(_json_list(row.services_offered_json)),
            }
            for row in clinicians
        },
        "weekday_names": (
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
        ),
        "format_clock_12h": _format_clock_12h,
        "schedule_rules": ClinicScheduleRule.query.order_by(
            ClinicScheduleRule.location_id.asc(), ClinicScheduleRule.clinician_id.asc(),
            ClinicScheduleRule.weekday.asc(), ClinicScheduleRule.id.desc()
        ).limit(200).all(),
        "schedule_exceptions": ClinicScheduleException.query.order_by(
            ClinicScheduleException.schedule_date.desc(), ClinicScheduleException.id.desc()
        ).limit(100).all(),
        "slot_blocks": AppointmentSlotBlock.query.order_by(
            AppointmentSlotBlock.block_date.desc(), AppointmentSlotBlock.id.desc()
        ).limit(100).all(),
        "reception_schedules": ReceptionSchedule.query.order_by(
            ReceptionSchedule.location_id.asc(), ReceptionSchedule.weekday.asc()
        ).limit(100).all(),
        "reception_overrides": ReceptionScheduleOverride.query.order_by(
            ReceptionScheduleOverride.schedule_date.desc()
        ).limit(100).all(),
        "knowledge_entries": ClinicKnowledgeEntry.query.order_by(
            ClinicKnowledgeEntry.category.asc(), ClinicKnowledgeEntry.id.desc()
        ).limit(100).all(),
        "callbacks": callback_query.order_by(CallbackRequest.created_at.desc()).limit(100).all(),
        "complaints": complaint_query.order_by(Complaint.created_at.desc()).limit(100).all(),
        "callback_status_filter": callback_status,
        "callback_priority_filter": callback_priority,
        "complaint_status_filter": complaint_status,
        "complaint_priority_filter": complaint_priority,
        "operations_date_from": operations_date_from,
        "operations_date_to": operations_date_to,
        "notification_templates": NotificationTemplate.query.order_by(
            NotificationTemplate.event_code.asc(), NotificationTemplate.channel.asc()
        ).limit(100).all(),
        "notification_deliveries": NotificationDelivery.query.order_by(
            NotificationDelivery.created_at.desc()
        ).limit(50).all(),
        "lab_test_count": LabTest.query.filter_by(is_active=True).count(),
        "lab_test_alias_count": LabTest.query.filter(
            LabTest.is_active.is_(True), LabTest.aliases_json.notin_(["[]", "", None])
        ).count(),
    }


@ai_admin_bp.get("")
@ai_admin_bp.get("/")
@_admin_required
def overview():
    return render_template("ai_integration.html", **_dashboard_context())


@ai_admin_bp.post("/clients")
@_admin_required
def create_client():
    _require_csrf()
    try:
        client, raw_secret = create_api_client(
            db.session,
            name=request.form.get("name"),
            scopes=request.form.getlist("scopes"),
            allowed_ips=request.form.get("allowed_ips", ""),
            created_by=session.get("username"),
        )
        _audit_admin_action("AI_API_CLIENT_CREATED", client)
        db.session.commit()
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")
    return _secret_response(
        "AI API client created. Copy the secret now; it will not be shown again.",
        client.client_id,
        raw_secret,
    )


@ai_admin_bp.post("/clients/<int:client_pk>/settings")
@_admin_required
def update_client_settings(client_pk):
    _require_csrf()
    client = db.session.get(AIAPIClient, client_pk)
    if client is None:
        abort(404)
    try:
        scopes = normalize_scopes(request.form.getlist("scopes"))
        if not scopes:
            raise ValueError("At least one API scope is required")
        allowed_ips = normalize_allowed_ips(request.form.get("allowed_ips", ""))
        client.name = str(request.form.get("name") or "").strip()
        if not client.name or len(client.name) > 120:
            raise ValueError("Client name is required and must be at most 120 characters")
        client.allowed_scopes_json = serialize_scopes(scopes)
        client.allowed_ips_json = serialize_allowed_ips(allowed_ips)
        client.updated_by = session.get("username")
        _audit_admin_action("AI_API_CLIENT_SETTINGS_UPDATED", client)
        db.session.commit()
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")
    return _operation_redirect("AI API client settings updated.")


@ai_admin_bp.post("/clients/<int:client_pk>/toggle")
@_admin_required
def toggle_client(client_pk):
    _require_csrf()
    client = db.session.get(AIAPIClient, client_pk)
    if client is None:
        abort(404)
    client.is_active = not bool(client.is_active)
    client.updated_by = session.get("username")
    if not client.is_active:
        now = datetime.utcnow()
        AIAccessToken.query.filter(
            AIAccessToken.api_client_id == client.id,
            AIAccessToken.revoked_at.is_(None),
        ).update({AIAccessToken.revoked_at: now}, synchronize_session=False)
    _audit_admin_action(
        "AI_API_CLIENT_ENABLED" if client.is_active else "AI_API_CLIENT_DISABLED",
        client,
    )
    db.session.commit()
    return _operation_redirect("AI API client status updated.")


@ai_admin_bp.post("/clients/<int:client_pk>/rotate")
@_admin_required
def rotate_client_secret(client_pk):
    _require_csrf()
    client = db.session.get(AIAPIClient, client_pk)
    if client is None:
        abort(404)
    raw_secret = rotate_api_client_secret(
        db.session,
        client,
        updated_by=session.get("username"),
    )
    _audit_admin_action("AI_API_CLIENT_SECRET_ROTATED", client)
    db.session.commit()
    return _secret_response(
        "Client secret rotated. All previous access tokens were revoked.",
        client.client_id,
        raw_secret,
    )


@ai_admin_bp.post("/clinic-profile")
@_admin_required
def save_clinic_profile():
    _require_csrf()
    clinic_name = str(request.form.get("clinic_name") or "").strip()
    if not clinic_name or len(clinic_name) > 160:
        return _operation_redirect("Clinic name is required and must be at most 160 characters.", "danger")
    profile = ClinicProfile.query.order_by(ClinicProfile.id.asc()).first() or ClinicProfile(
        clinic_name=clinic_name
    )
    profile.clinic_name = clinic_name
    profile.phone = str(request.form.get("phone") or "").strip()[:30] or None
    profile.email = str(request.form.get("email") or "").strip()[:160] or None
    profile.reception_phone = str(request.form.get("reception_phone") or "").strip()[:30] or None
    profile.website_url = str(request.form.get("website_url") or "").strip()[:500] or None
    profile.emergency_wording = str(request.form.get("emergency_wording") or "").strip()[:500] or None
    profile.consultation_information = str(request.form.get("consultation_information") or "").strip() or None
    profile.general_policies = str(request.form.get("general_policies") or "").strip() or None
    profile.payment_methods_json = _json_array_text(request.form.get("payment_methods"))
    profile.available_services_json = _json_array_text(request.form.get("available_services"))
    timezone_name = str(request.form.get("timezone_name") or "Asia/Kolkata").strip()[:64]
    profile.timezone_name = timezone_name
    # Some upgraded SQLite databases still have the original NOT NULL
    # ``timezone`` column. Keep it synchronized for backward compatibility.
    profile.legacy_timezone = timezone_name
    try:
        profile.late_grace_minutes = _form_int(
            "late_grace_minutes", minimum=0, maximum=240
        ) or 0
        profile.auto_reschedule_after_minutes = _form_int(
            "auto_reschedule_after_minutes", minimum=1, maximum=1440
        )
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")
    profile.late_staff_notification_required = _form_bool("late_staff_notification_required")
    profile.is_active = True
    profile.updated_by = session.get("username")
    db.session.add(profile)
    db.session.commit()
    return _operation_redirect("Clinic information saved.")


@ai_admin_bp.post("/locations")
@_admin_required
def create_location():
    _require_csrf()
    try:
        values = _location_form_values()
        if ClinicLocation.query.filter_by(code=values["code"]).first():
            raise ValueError("That location code already exists.")
        location = ClinicLocation(
            **values,
            is_active=True,
            created_by=session.get("username"),
            updated_by=session.get("username"),
        )
        db.session.add(location)
        db.session.flush()
        _make_only_default_location(location)
        _ensure_active_default_location()
        db.session.commit()
        return _operation_redirect("Clinic location created.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/locations/<int:row_id>/edit")
@_admin_required
def edit_location(row_id):
    _require_csrf()
    location = db.session.get(ClinicLocation, row_id)
    if location is None:
        abort(404)
    try:
        values = _location_form_values()
        duplicate = ClinicLocation.query.filter(
            ClinicLocation.code == values["code"], ClinicLocation.id != location.id
        ).first()
        if duplicate:
            raise ValueError("That location code already exists.")
        for field, value in values.items():
            setattr(location, field, value)
        location.updated_by = session.get("username")
        _make_only_default_location(location)
        _ensure_active_default_location()
        db.session.add(
            AuditLog(
                user=session.get("username"),
                action="AI_LOCATION_UPDATED",
                entity_type="CLINIC_LOCATION",
                entity_id=location.id,
                ref_code=location.code,
                extra_json=json.dumps(
                    {
                        "location_type": location.location_type,
                        "is_default": bool(location.is_default),
                    }
                ),
            )
        )
        db.session.commit()
        return _operation_redirect("Location updated.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/clinicians")
@_admin_required
def create_clinician():
    _require_csrf()
    code = str(request.form.get("code") or "").strip().upper()
    name = str(request.form.get("display_name") or "").strip()
    if not code or not name or Clinician.query.filter_by(code=code).first():
        return _operation_redirect("Use a unique doctor code and valid display name.", "danger")
    try:
        location_id = _form_int("default_location_id", minimum=1)
        doctor = Clinician(
            code=code[:40],
            display_name=name[:120],
            public_title=str(request.form.get("public_title") or "").strip()[:80] or None,
            specialty=str(request.form.get("specialty") or "").strip()[:120] or None,
            qualification=str(request.form.get("qualification") or "").strip()[:180] or None,
            consultation_fee=request.form.get("consultation_fee") or None,
            follow_up_fee=request.form.get("follow_up_fee") or None,
            follow_up_days=_form_int("follow_up_days", minimum=0, maximum=365),
            public_bio=str(request.form.get("public_bio") or "").strip()[:500] or None,
            sub_specialties_json=_json_array_text(request.form.get("sub_specialties")),
            languages_json=_json_array_text(request.form.get("languages")),
            conditions_treated_json=_json_array_text(request.form.get("conditions_treated")),
            services_offered_json=_json_array_text(request.form.get("services_offered")),
            default_location_id=location_id,
            is_active=True,
            created_by=session.get("username"),
            updated_by=session.get("username"),
        )
        db.session.add(doctor)
        db.session.commit()
        return _operation_redirect("Doctor created.")
    except (ValueError, TypeError):
        db.session.rollback()
        return _operation_redirect("Doctor fees and follow-up fields must be valid numbers.", "danger")


@ai_admin_bp.post("/clinicians/<int:row_id>/edit")
@_admin_required
def edit_clinician(row_id):
    _require_csrf()
    doctor = db.session.get(Clinician, row_id)
    if doctor is None:
        abort(404)
    code = str(request.form.get("code") or "").strip().upper()
    name = str(request.form.get("display_name") or "").strip()
    if not code or not name:
        return _operation_redirect("Use a unique doctor code and valid display name.", "danger")
    duplicate = Clinician.query.filter(
        Clinician.code == code, Clinician.id != doctor.id
    ).first()
    if duplicate:
        return _operation_redirect("Use a unique doctor code and valid display name.", "danger")
    try:
        location_id = _form_int("default_location_id", minimum=1)
        doctor.code = code[:40]
        doctor.display_name = name[:120]
        doctor.public_title = str(request.form.get("public_title") or "").strip()[:80] or None
        doctor.specialty = str(request.form.get("specialty") or "").strip()[:120] or None
        doctor.qualification = str(request.form.get("qualification") or "").strip()[:180] or None
        doctor.consultation_fee = request.form.get("consultation_fee") or None
        doctor.follow_up_fee = request.form.get("follow_up_fee") or None
        doctor.follow_up_days = _form_int("follow_up_days", minimum=0, maximum=365)
        doctor.public_bio = str(request.form.get("public_bio") or "").strip()[:500] or None
        doctor.sub_specialties_json = _json_array_text(request.form.get("sub_specialties"))
        doctor.languages_json = _json_array_text(request.form.get("languages"))
        doctor.conditions_treated_json = _json_array_text(request.form.get("conditions_treated"))
        doctor.services_offered_json = _json_array_text(request.form.get("services_offered"))
        doctor.default_location_id = location_id
        doctor.updated_by = session.get("username")
        db.session.commit()
        return _operation_redirect("Doctor updated.")
    except (ValueError, TypeError):
        db.session.rollback()
        return _operation_redirect("Doctor fees and follow-up fields must be valid numbers.", "danger")


def _schedule_rule_form_values():
    starts = _form_time("arrival_window_start", required=True)
    ends = _form_time("arrival_window_end", required=True)
    if starts >= ends:
        raise ValueError("Schedule end time must be after start time")

    values = {
        "location_id": _form_int("location_id", minimum=1, required=True),
        "clinician_id": _form_int("clinician_id", minimum=1),
        "weekday": _form_int("weekday", minimum=0, maximum=6, required=True),
        "booking_enabled": _form_bool("booking_enabled"),
        "individual_time_slots": _form_bool("individual_time_slots"),
        "arrival_window_start": starts.strftime("%H:%M"),
        "arrival_window_end": ends.strftime("%H:%M"),
        "normal_daily_limit": _form_int(
            "normal_daily_limit", minimum=0, maximum=1000
        )
        or 0,
        "priority_daily_limit": _form_int(
            "priority_daily_limit", minimum=0, maximum=1000
        )
        or 0,
        "slot_duration_minutes": _form_int(
            "slot_duration_minutes", minimum=5, maximum=180
        )
        or 20,
        "max_patients_per_slot": _form_int(
            "max_patients_per_slot", minimum=1, maximum=20
        )
        or 1,
        "effective_from": _form_date("effective_from"),
        "effective_to": _form_date("effective_to"),
        "public_note": str(request.form.get("public_note") or "").strip()[:240]
        or None,
    }
    if (
        values["effective_from"]
        and values["effective_to"]
        and values["effective_from"] > values["effective_to"]
    ):
        raise ValueError("Effective date range is invalid")
    return values


def _ensure_schedule_rule_does_not_overlap(values, *, exclude_rule_id=None):
    query = ClinicScheduleRule.query.filter_by(
        location_id=values["location_id"],
        clinician_id=values["clinician_id"],
        weekday=values["weekday"],
        is_active=True,
    )
    if exclude_rule_id is not None:
        query = query.filter(ClinicScheduleRule.id != exclude_rule_id)
    if any(
        _date_ranges_overlap(
            values["effective_from"],
            values["effective_to"],
            existing.effective_from,
            existing.effective_to,
        )
        for existing in query.all()
    ):
        raise ValueError(
            "An active schedule already covers this doctor, location and date range"
        )


@ai_admin_bp.post("/schedule-rules")
@_admin_required
def create_schedule_rule():
    _require_csrf()
    try:
        values = _schedule_rule_form_values()
        _ensure_schedule_rule_does_not_overlap(values)
        rule = ClinicScheduleRule(
            **values,
            is_active=True,
            created_by=session.get("username"),
            updated_by=session.get("username"),
        )
        db.session.add(rule)
        db.session.commit()
        return _operation_redirect("Schedule rule created.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/schedule-rules/<int:row_id>/edit")
@_admin_required
def edit_schedule_rule(row_id):
    """Edit one recurring schedule without changing its active state."""
    _require_csrf()
    rule = db.session.get(ClinicScheduleRule, row_id)
    if rule is None:
        abort(404)
    try:
        values = _schedule_rule_form_values()
        if rule.is_active:
            _ensure_schedule_rule_does_not_overlap(
                values, exclude_rule_id=rule.id
            )
        for field, value in values.items():
            setattr(rule, field, value)
        rule.updated_by = session.get("username")
        db.session.add(
            AuditLog(
                user=session.get("username"),
                action="AI_SCHEDULE_RULE_UPDATED",
                entity_type="SCHEDULE_RULE",
                entity_id=rule.id,
                ref_code=str(rule.id),
                extra_json=json.dumps(
                    {
                        "weekday": rule.weekday,
                        "arrival_window_start": rule.arrival_window_start,
                        "arrival_window_end": rule.arrival_window_end,
                        "slot_duration_minutes": rule.slot_duration_minutes,
                    }
                ),
            )
        )
        db.session.commit()
        return _operation_redirect("Doctor schedule updated.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/schedule-rules/<int:row_id>/delete")
@_admin_required
def delete_schedule_rule(row_id):
    """Permanently remove one recurring availability rule.

    A rule must first be disabled, so it cannot accidentally change live
    availability. Appointments keep their own location, clinician, date and
    time, and the deleted rule's essential values are retained in the audit log.
    """
    _require_csrf()
    rule = ClinicScheduleRule.query.filter_by(id=row_id).with_for_update().first()
    if rule is None:
        abort(404)
    if rule.is_active:
        return _operation_redirect(
            "Disable the schedule rule before deleting it.", "danger"
        )

    db.session.add(
        AuditLog(
            user=session.get("username"),
            action="AI_SCHEDULE_RULE_DELETED",
            entity_type="SCHEDULE_RULE",
            entity_id=rule.id,
            ref_code=str(rule.id),
            extra_json=json.dumps(
                {
                    "location_id": rule.location_id,
                    "clinician_id": rule.clinician_id,
                    "weekday": rule.weekday,
                    "booking_enabled": bool(rule.booking_enabled),
                    "arrival_window_start": rule.arrival_window_start,
                    "arrival_window_end": rule.arrival_window_end,
                    "normal_daily_limit": rule.normal_daily_limit,
                    "priority_daily_limit": rule.priority_daily_limit,
                    "individual_time_slots": bool(rule.individual_time_slots),
                    "effective_from": (
                        rule.effective_from.isoformat() if rule.effective_from else None
                    ),
                    "effective_to": (
                        rule.effective_to.isoformat() if rule.effective_to else None
                    ),
                }
            ),
        )
    )
    db.session.delete(rule)
    db.session.commit()
    return _operation_redirect(
        "Schedule rule deleted. Existing booked appointments were not changed."
    )


@ai_admin_bp.post("/schedule-exceptions")
@_admin_required
def create_schedule_exception():
    _require_csrf()
    try:
        exception_type = str(request.form.get("exception_type") or "").upper()
        if exception_type not in {"CLOSED", "HOLIDAY", "LEAVE", "OVERRIDE"}:
            raise ValueError("Invalid schedule exception type")
        starts = _form_time("arrival_window_start")
        ends = _form_time("arrival_window_end")
        if exception_type == "OVERRIDE" and (not starts or not ends or starts >= ends):
            raise ValueError("An override requires a valid start and end time")
        location_id = _form_int("location_id", minimum=1, required=True)
        clinician_id = _form_int("clinician_id", minimum=1)
        schedule_date = _form_date("schedule_date", required=True)
        if ClinicScheduleException.query.filter_by(
            location_id=location_id,
            clinician_id=clinician_id,
            schedule_date=schedule_date,
            is_active=True,
        ).first():
            raise ValueError("An active schedule exception already exists for this date")
        row = ClinicScheduleException(
            location_id=location_id,
            clinician_id=clinician_id,
            schedule_date=schedule_date,
            exception_type=exception_type,
            booking_enabled=exception_type == "OVERRIDE",
            arrival_window_start=starts.strftime("%H:%M") if starts else None,
            arrival_window_end=ends.strftime("%H:%M") if ends else None,
            normal_daily_limit=_form_int("normal_daily_limit", minimum=0, maximum=1000),
            priority_daily_limit=_form_int("priority_daily_limit", minimum=0, maximum=1000),
            slot_duration_minutes=_form_int("slot_duration_minutes", minimum=5, maximum=180),
            max_patients_per_slot=_form_int("max_patients_per_slot", minimum=1, maximum=20),
            public_note=str(request.form.get("public_note") or "").strip()[:240] or None,
            is_active=True,
            created_by=session.get("username"),
            updated_by=session.get("username"),
        )
        db.session.add(row)
        db.session.commit()
        return _operation_redirect("Schedule exception created.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/slot-blocks")
@_admin_required
def create_slot_block():
    _require_csrf()
    try:
        all_day = _form_bool("all_day")
        starts = _form_time("start_time")
        ends = _form_time("end_time")
        if not all_day and (not starts or not ends or starts >= ends):
            raise ValueError("A timed block requires a valid start and end time")
        row = AppointmentSlotBlock(
            clinician_id=_form_int("clinician_id", minimum=1, required=True),
            location_id=_form_int("location_id", minimum=1, required=True),
            block_date=_form_date("block_date", required=True),
            start_time=None if all_day else starts,
            end_time=None if all_day else ends,
            all_day=all_day,
            public_reason=str(request.form.get("public_reason") or "").strip()[:240] or None,
            is_active=True,
            created_by=session.get("username"),
        )
        db.session.add(row)
        db.session.commit()
        return _operation_redirect("Appointment slot block created.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/reception-schedules")
@_admin_required
def create_reception_schedule():
    _require_csrf()
    try:
        is_open = _form_bool("is_open")
        opens = _form_time("open_time")
        closes = _form_time("close_time")
        if is_open and (not opens or not closes or opens >= closes):
            raise ValueError("Open reception hours require a valid time range")
        location_id = _form_int("location_id", minimum=1, required=True)
        weekday = _form_int("weekday", minimum=0, maximum=6, required=True)
        if ReceptionSchedule.query.filter_by(
            location_id=location_id, weekday=weekday, is_active=True
        ).first():
            raise ValueError("An active reception schedule already exists for this weekday")
        row = ReceptionSchedule(
            location_id=location_id,
            weekday=weekday,
            is_open=is_open,
            open_time=opens if is_open else None,
            close_time=closes if is_open else None,
            public_note=str(request.form.get("public_note") or "").strip()[:240] or None,
            is_active=True,
            created_by=session.get("username"),
            updated_by=session.get("username"),
        )
        db.session.add(row)
        db.session.commit()
        return _operation_redirect("Reception schedule created.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/reception-overrides")
@_admin_required
def create_reception_override():
    _require_csrf()
    try:
        location_id = _form_int("location_id", minimum=1, required=True)
        schedule_date = _form_date("schedule_date", required=True)
        is_open = _form_bool("is_open")
        opens = _form_time("open_time")
        closes = _form_time("close_time")
        if is_open and (not opens or not closes or opens >= closes):
            raise ValueError("An open reception override requires a valid time range")
        if ReceptionScheduleOverride.query.filter_by(
            location_id=location_id, schedule_date=schedule_date, is_active=True
        ).first():
            raise ValueError("An active reception override already exists for this date")
        row = ReceptionScheduleOverride(
            location_id=location_id,
            schedule_date=schedule_date,
            is_open=is_open,
            open_time=opens if is_open else None,
            close_time=closes if is_open else None,
            public_note=str(request.form.get("public_note") or "").strip()[:240] or None,
            is_active=True,
            created_by=session.get("username"),
        )
        db.session.add(row)
        db.session.commit()
        return _operation_redirect("Reception override created.")
    except ValueError as exc:
        db.session.rollback()
        return _operation_redirect(str(exc), "danger")


@ai_admin_bp.post("/knowledge")
@_admin_required
def create_knowledge_entry():
    _require_csrf()
    category = str(request.form.get("category") or "").strip()[:80]
    question = str(request.form.get("question") or "").strip()[:240]
    answer = str(request.form.get("answer") or "").strip()
    if not category or not question or not answer or len(answer) > 5000:
        return _operation_redirect("Knowledge category, question and answer are required.", "danger")
    row = ClinicKnowledgeEntry(
        category=category,
        question=question,
        answer=answer,
        language=str(request.form.get("language") or "en").lower()[:12],
        keywords_json=_json_array_text(request.form.get("keywords")),
        is_active=True,
        created_by=session.get("username"),
        updated_by=session.get("username"),
    )
    db.session.add(row)
    db.session.commit()
    return _operation_redirect("Knowledge entry created.")


@ai_admin_bp.post("/callbacks/<int:row_id>/status")
@_admin_required
def update_callback_status(row_id):
    _require_csrf()
    row = db.session.get(CallbackRequest, row_id)
    status = str(request.form.get("status") or "").upper()
    if not row or status not in {"PENDING", "ASSIGNED", "CONTACTED", "COMPLETED", "CANCELLED"}:
        abort(400)
    row.status = status
    row.assigned_to = str(request.form.get("assigned_to") or "").strip()[:50] or None
    row.resolution_note = str(request.form.get("resolution_note") or "").strip()[:500] or None
    if status == "CONTACTED" and row.contacted_at is None:
        row.contacted_at = datetime.utcnow()
    row.completed_at = datetime.utcnow() if status == "COMPLETED" else None
    db.session.commit()
    return _operation_redirect("Callback updated.")


@ai_admin_bp.post("/complaints/<int:row_id>/status")
@_admin_required
def update_complaint_status(row_id):
    _require_csrf()
    row = db.session.get(Complaint, row_id)
    status = str(request.form.get("status") or "").upper()
    if not row or status not in {"OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"}:
        abort(400)
    row.status = status
    row.assigned_to = str(request.form.get("assigned_to") or "").strip()[:50] or None
    row.resolution_note = str(request.form.get("resolution_note") or "").strip()[:1000] or None
    row.resolved_at = datetime.utcnow() if status in {"RESOLVED", "CLOSED"} else None
    db.session.commit()
    return _operation_redirect("Complaint updated.")


@ai_admin_bp.post("/notification-templates")
@_admin_required
def create_notification_template():
    _require_csrf()
    event_code = str(request.form.get("event_code") or "").upper()[:80]
    channel = str(request.form.get("channel") or "").upper()
    language = str(request.form.get("language") or "en").lower()[:12]
    body = str(request.form.get("body_template") or "").strip()
    if not event_code or channel not in {"SMS", "WHATSAPP", "EMAIL"} or not body or len(body) > 2000:
        return _operation_redirect("Valid template event, channel and body are required.", "danger")
    existing = NotificationTemplate.query.filter_by(
        event_code=event_code, channel=channel, language=language
    ).first()
    row = existing or NotificationTemplate(event_code=event_code, channel=channel, language=language)
    row.body_template = body
    row.is_active = True
    row.updated_by = session.get("username")
    row.created_by = row.created_by or session.get("username")
    db.session.add(row)
    db.session.commit()
    return _operation_redirect("Notification template saved.")


@ai_admin_bp.post("/settings/<entity>/<int:row_id>/toggle")
@_admin_required
def toggle_operational_setting(entity, row_id):
    """Deactivate/reactivate settings without deleting operational history."""
    _require_csrf()
    model_map = {
        "location": ClinicLocation,
        "clinician": Clinician,
        "schedule-rule": ClinicScheduleRule,
        "schedule-exception": ClinicScheduleException,
        "slot-block": AppointmentSlotBlock,
        "reception-schedule": ReceptionSchedule,
        "reception-override": ReceptionScheduleOverride,
        "knowledge": ClinicKnowledgeEntry,
        "notification-template": NotificationTemplate,
    }
    model = model_map.get(entity)
    row = db.session.get(model, row_id) if model else None
    if row is None:
        abort(404)
    row.is_active = not bool(row.is_active)
    if entity == "location":
        if not row.is_active:
            row.is_default = False
        _ensure_active_default_location()
    if hasattr(row, "updated_by"):
        row.updated_by = session.get("username")
    db.session.add(
        AuditLog(
            user=session.get("username"),
            action="AI_OPERATIONAL_SETTING_TOGGLED",
            entity_type=entity.upper().replace("-", "_"),
            entity_id=row.id,
            ref_code=str(row.id),
            extra_json=json.dumps({"is_active": bool(row.is_active)}),
        )
    )
    db.session.commit()
    return _operation_redirect("Operational setting updated.")
