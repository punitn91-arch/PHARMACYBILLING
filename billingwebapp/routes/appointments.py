from __future__ import annotations

from datetime import date

from flask import flash, redirect, render_template, request, url_for

from models import Appointment, db
from services.appointments import appointment_redirect_date, build_appointments_page_context


def render_appointments_page(
    request_args,
    *,
    all_statuses,
    build_appointment_report_payload,
    build_appointment_summary,
    build_live_queue_snapshot,
    build_appointment_calendar_days,
    clinic_now,
):
    context = build_appointments_page_context(
        request_args,
        build_appointment_report_payload=build_appointment_report_payload,
        build_appointment_summary=build_appointment_summary,
        build_live_queue_snapshot=build_live_queue_snapshot,
        build_appointment_calendar_days=build_appointment_calendar_days,
        clinic_now=clinic_now,
    )
    context["all_statuses"] = all_statuses
    return render_template("appointments.html", **context)


def mark_appointment_paid(
    appointment_id,
    *,
    build_appointment_audit_snapshot,
    record_audit_event,
    validate_mark_paid_transition,
):
    appointment = Appointment.query.filter(
        Appointment.id == appointment_id,
        Appointment.deleted_at.is_(None),
    ).first_or_404()
    before_snapshot = build_appointment_audit_snapshot(appointment)
    selected_date = appointment_redirect_date(appointment, date.today().isoformat())
    error_message = validate_mark_paid_transition(appointment)
    if error_message:
        flash(error_message, "danger" if "already" not in error_message.lower() else "info")
        return redirect(
            request.referrer
            or url_for("appointments", appointment_report_type="day", appointment_day_date=selected_date)
        )
    appointment.payment_status = "PAID"
    db.session.commit()
    record_audit_event(
        action="Marked appointment as paid",
        entity_type="APPOINTMENT",
        entity_id=appointment.id,
        ref_code=appointment.appointment_no,
        before=before_snapshot,
        after=build_appointment_audit_snapshot(appointment),
    )
    flash("Appointment marked as paid.", "success")
    return redirect(
        request.referrer
        or url_for("appointments", appointment_report_type="day", appointment_day_date=selected_date)
    )
