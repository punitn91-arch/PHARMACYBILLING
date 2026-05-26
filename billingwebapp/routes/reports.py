from __future__ import annotations

from flask import flash, render_template

from services.reports import build_reports_page_state


def render_reports_page(
    *,
    request_method,
    form_data,
    prevalidated_error=None,
    clinic_now,
    parse_date,
    to_int_safe,
    local_date_range_to_storage_bounds,
    normalize_patient_mobile,
    or_,
    build_patient_medicine_usage_report,
    build_profit_report_summary,
    build_medicine_report_data,
):
    payload = build_reports_page_state(
        request_method,
        form_data,
        prevalidated_error=prevalidated_error,
        clinic_now=clinic_now,
        parse_date=parse_date,
        to_int_safe=to_int_safe,
        local_date_range_to_storage_bounds=local_date_range_to_storage_bounds,
        normalize_patient_mobile=normalize_patient_mobile,
        or_=or_,
        build_patient_medicine_usage_report=build_patient_medicine_usage_report,
        build_profit_report_summary=build_profit_report_summary,
        build_medicine_report_data=build_medicine_report_data,
    )
    for category, message in payload.pop("messages", []):
        flash(message, category)
    return render_template("reports.html", **payload)
