from __future__ import annotations


def build_appointments_page_context(
    args,
    *,
    build_appointment_report_payload,
    build_appointment_summary,
    build_live_queue_snapshot,
    build_appointment_calendar_days,
    clinic_now,
):
    payload = build_appointment_report_payload(args, flash_errors=True)
    appointments = payload["appointments"]
    list_only = (args.get("view") or "").strip().lower() == "list"
    summary_payload = payload
    if payload["search_query"]:
        summary_payload = build_appointment_report_payload(
            args,
            flash_errors=False,
            include_search=False,
        )
    appointment_summary = build_appointment_summary(summary_payload["appointments"])
    queue_preview_date = payload["focus_date"] or clinic_now().date()
    queue_preview = build_live_queue_snapshot(queue_preview_date)
    calendar_days = build_appointment_calendar_days(
        appointments,
        payload["calendar_view"],
        payload["focus_date"],
    )
    return {
        "appointments": appointments,
        "list_only": list_only,
        "report_filters": payload["report_filters"],
        "report_label": payload["report_label"],
        "quick_filter": payload["quick_filter"],
        "search_query": payload["search_query"],
        "calendar_view": payload["calendar_view"],
        "calendar_days": calendar_days,
        "queue_preview": queue_preview,
        "queue_board_date": queue_preview_date.isoformat(),
        "summary_label": summary_payload["report_label"],
        "summary_counts": appointment_summary["counts"],
        "summary_revenue": appointment_summary["revenue"],
        "summary_revenue_breakdown": appointment_summary["revenue_breakdown"],
    }


def appointment_redirect_date(appointment, fallback_date):
    appointment_date = getattr(appointment, "appointment_date", None)
    if appointment_date:
        return appointment_date.isoformat()
    return fallback_date

