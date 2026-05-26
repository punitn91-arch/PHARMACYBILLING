from __future__ import annotations

from copy import deepcopy


USER_PERMISSION_FIELDS = (
    "can_view_medicine",
    "can_add_medicine",
    "can_edit_medicine",
    "can_delete_medicine",
    "can_edit_invoice",
    "can_delete_invoice",
    "can_invoice_action",
    "can_view_stock_history",
    "can_view_reports",
    "can_manage_users",
    "can_manage_purchases",
    "can_view_audit_logs",
    "can_view_profit_dashboard",
)


ACCESS_PROFILE_PRESETS = {
    "custom": {},
    "report_only": {
        "can_view_reports": True,
    },
    "billing_only": {
        "can_invoice_action": True,
        "can_edit_invoice": True,
    },
    "stock_manager": {
        "can_view_medicine": True,
        "can_add_medicine": True,
        "can_edit_medicine": True,
        "can_view_stock_history": True,
    },
    "purchase_manager": {
        "can_manage_purchases": True,
        "can_view_medicine": True,
        "can_add_medicine": True,
        "can_edit_medicine": True,
        "can_view_stock_history": True,
    },
    "admin_audit": {
        "can_view_reports": True,
        "can_manage_users": True,
        "can_view_audit_logs": True,
    },
}


def _to_int(value, default=0):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def bool_from_form(form_data, field_name):
    return bool(form_data.get(field_name))


def normalize_permission_state(form_data):
    permissions = {
        field_name: bool_from_form(form_data, field_name)
        for field_name in USER_PERMISSION_FIELDS
    }
    if permissions["can_edit_invoice"] or permissions["can_delete_invoice"]:
        permissions["can_invoice_action"] = True
    return permissions


def permissions_for_profile(profile_key):
    permissions = {field_name: False for field_name in USER_PERMISSION_FIELDS}
    preset = ACCESS_PROFILE_PRESETS.get((profile_key or "").strip().lower(), {})
    permissions.update({key: bool(value) for key, value in preset.items() if key in permissions})
    if permissions["can_edit_invoice"] or permissions["can_delete_invoice"]:
        permissions["can_invoice_action"] = True
    return permissions


def derive_access_profile(permissions):
    normalized = {field_name: bool(permissions.get(field_name)) for field_name in USER_PERMISSION_FIELDS}
    for profile_key, preset in ACCESS_PROFILE_PRESETS.items():
        if profile_key == "custom":
            continue
        signature = permissions_for_profile(profile_key)
        if all(normalized.get(field_name) == signature.get(field_name) for field_name in USER_PERMISSION_FIELDS):
            return profile_key
    return "custom"


def validate_user_form(form_data, *, is_create=False):
    username = (form_data.get("username") or "").strip()
    password = form_data.get("password") or ""
    role = (form_data.get("role") or "staff").strip().lower()
    access_profile = (form_data.get("access_profile") or "custom").strip().lower()

    errors = []
    if not username:
        errors.append("Username is required.")
    if is_create and not password:
        errors.append("Password is required.")
    if role not in {"admin", "staff"}:
        errors.append("Role must be admin or staff.")
        role = "staff"
    if access_profile not in ACCESS_PROFILE_PRESETS:
        access_profile = "custom"

    if role == "admin":
        permissions = {field_name: True for field_name in USER_PERMISSION_FIELDS}
        access_profile = "admin"
    elif access_profile != "custom":
        permissions = permissions_for_profile(access_profile)
    else:
        permissions = normalize_permission_state(form_data)
        access_profile = derive_access_profile(permissions)

    return {
        "username": username,
        "password": password,
        "role": role,
        "access_profile": access_profile,
        "permissions": permissions,
        "errors": errors,
    }


def validate_billing_submission(form_data):
    medicine_names = form_data.getlist("medicine_name")
    qty_list = form_data.getlist("qty") or form_data.getlist("qty[]")
    batch_overrides = form_data.getlist("batch_override[]") or form_data.getlist("batch_override")

    if medicine_names and qty_list and len(qty_list) < len(medicine_names):
        return "Billing items are incomplete. Please review medicine rows."
    if batch_overrides and medicine_names and len(batch_overrides) < len(medicine_names):
        return "Billing batch selection is incomplete. Please review medicine rows."

    has_positive_line = False
    for idx, name in enumerate(medicine_names):
        cleaned_name = (name or "").strip()
        qty = _to_int(qty_list[idx] if idx < len(qty_list) else 0, 0)
        if cleaned_name and qty > 0:
            has_positive_line = True
            break
    if not has_positive_line:
        return "Please add at least one medicine with quantity greater than zero."
    return None


def validate_return_submission(form_data):
    mode = (form_data.get("mode") or "invoice").strip().lower()
    if mode == "manual":
        ids = form_data.getlist("manual_medicine_id")
        qtys = form_data.getlist("manual_qty")
        for idx, med_id in enumerate(ids):
            qty = _to_int(qtys[idx] if idx < len(qtys) else 0, 0)
            if med_id and qty > 0:
                return None
        return "Please select at least one medicine to return."

    invoice_no = (form_data.get("invoice_no") or "").strip()
    if not invoice_no:
        return "Please enter invoice number."
    requested_qty = 0
    for key in form_data.keys():
        if key.startswith("return_qty_"):
            requested_qty += max(_to_int(form_data.get(key), 0), 0)
    if requested_qty <= 0:
        return "Please enter return quantity."
    return None


def validate_report_request(form_data, *, parse_date):
    report_type = (form_data.get("report_type") or "").strip().lower()
    if report_type == "monthly":
        month = _to_int(form_data.get("month"), 0)
        year = _to_int(form_data.get("year"), 0)
        if month < 1 or month > 12 or year < 1900:
            return "Please enter a valid month and year."
    elif report_type in {"custom", "profit", "patient_medicine"}:
        from_date = (form_data.get("from_date") or "").strip()
        to_date = (form_data.get("to_date") or "").strip()
        if not from_date or not to_date:
            return "Please select both from date and to date."
        start_date = parse_date(from_date)
        end_date = parse_date(to_date)
        if not start_date or not end_date:
            return "Please enter valid from/to dates."
        if end_date < start_date:
            return "To date must be greater than or equal to from date."
    elif report_type == "patient":
        if not (form_data.get("patient") or "").strip():
            return "Please enter patient name."
    elif report_type == "mobile":
        if not (form_data.get("mobile") or "").strip():
            return "Please enter mobile number."
    return None


def validate_mark_paid_transition(appointment):
    status = (getattr(appointment, "status", "") or "").strip().upper()
    payment_status = (getattr(appointment, "payment_status", "") or "").strip().upper()
    if status == "CANCELLED":
        return "Cancelled appointment cannot be marked as paid."
    if payment_status == "PAID":
        return "Appointment is already marked as paid."
    return None


def snapshot_profile_options():
    return deepcopy(ACCESS_PROFILE_PRESETS)
