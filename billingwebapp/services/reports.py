from __future__ import annotations

try:
    from ..models import Invoice, db
except ImportError:  # pragma: no cover - script/local fallback
    from models import Invoice, db


def default_report_filters():
    return {
        "month": "",
        "year": "",
        "from_date": "",
        "to_date": "",
        "patient": "",
        "mobile": "",
        "search_query": "",
        "medicine_query": "",
        "top_n": "10",
    }


def build_reports_page_state(
    request_method,
    form_data,
    *,
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
    invoices = []
    total = 0
    profit_summary = None
    medicine_summary = []
    fast_movers = []
    medicine_totals = None
    patient_medicine_patients = []
    patient_medicine_rows = []
    patient_medicine_summary = None
    report_filters = default_report_filters()
    messages = []

    report_type = form_data.get("report_type")
    if request_method == "POST":
        for key in report_filters.keys():
            report_filters[key] = (form_data.get(key) or "").strip()
        if not report_filters["top_n"]:
            report_filters["top_n"] = "10"
        if prevalidated_error:
            messages.append(("danger", prevalidated_error))
            return {
                "invoices": invoices,
                "total": round(total, 2),
                "report_type": report_type,
                "profit_summary": profit_summary,
                "medicine_summary": medicine_summary,
                "fast_movers": fast_movers,
                "medicine_totals": medicine_totals,
                "patient_medicine_patients": patient_medicine_patients,
                "patient_medicine_rows": patient_medicine_rows,
                "patient_medicine_summary": patient_medicine_summary,
                "report_filters": report_filters,
                "messages": messages,
            }

        if report_type == "daily":
            today = clinic_now().date()
            start_bound, end_bound = local_date_range_to_storage_bounds(today, today)
            invoices = Invoice.query.filter(
                Invoice.created_at >= start_bound,
                Invoice.created_at < end_bound,
            ).all()
        elif report_type == "monthly":
            month = to_int_safe(form_data.get("month"), 0)
            year = to_int_safe(form_data.get("year"), 0)
            if month < 1 or month > 12 or year < 1900:
                messages.append(("danger", "Please enter a valid month and year."))
            else:
                from datetime import date
                from calendar import monthrange

                _, last_day = monthrange(year, month)
                start_bound, end_bound = local_date_range_to_storage_bounds(
                    date(year, month, 1),
                    date(year, month, last_day),
                )
                invoices = Invoice.query.filter(
                    Invoice.created_at >= start_bound,
                    Invoice.created_at < end_bound,
                ).all()
        elif report_type == "custom":
            from_date = form_data.get("from_date")
            to_date = form_data.get("to_date")
            if from_date and to_date:
                from_date_value = parse_date(from_date)
                to_date_value = parse_date(to_date)
                if not from_date_value or not to_date_value:
                    messages.append(("danger", "Please enter valid from/to dates."))
                elif to_date_value < from_date_value:
                    messages.append(("danger", "To date must be greater than or equal to from date."))
                else:
                    from_dt, to_dt = local_date_range_to_storage_bounds(from_date_value, to_date_value)
                    invoices = Invoice.query.filter(
                        Invoice.created_at >= from_dt,
                        Invoice.created_at < to_dt,
                    ).all()
            else:
                messages.append(("danger", "Please select both from date and to date."))
        elif report_type == "patient":
            patient = (form_data.get("patient") or "").strip()
            if not patient:
                messages.append(("danger", "Please enter patient name."))
            else:
                invoices = Invoice.query.filter(Invoice.customer.ilike(f"%{patient}%")).all()
        elif report_type == "mobile":
            mobile_raw = (form_data.get("mobile") or "").strip()
            if not mobile_raw:
                messages.append(("danger", "Please enter mobile number."))
            else:
                mobile_digits = normalize_patient_mobile(mobile_raw)
                normalized_mobile = db.func.replace(
                    db.func.replace(
                        db.func.replace(
                            db.func.replace(
                                db.func.replace(
                                    db.func.replace(db.func.coalesce(Invoice.mobile, ""), " ", ""),
                                    "-",
                                    "",
                                ),
                                "+",
                                "",
                            ),
                            "(",
                            "",
                        ),
                        ")",
                        "",
                    ),
                    ".",
                    "",
                )
                if mobile_digits:
                    invoices = Invoice.query.filter(
                        or_(
                            normalized_mobile.like(f"%{mobile_digits}%"),
                            Invoice.mobile.ilike(f"%{mobile_raw}%"),
                        )
                    ).all()
                else:
                    invoices = Invoice.query.filter(Invoice.mobile.ilike(f"%{mobile_raw}%")).all()
        elif report_type == "patient_medicine":
            (
                patient_medicine_patients,
                patient_medicine_rows,
                patient_medicine_summary,
                usage_error,
            ) = build_patient_medicine_usage_report(
                report_filters["from_date"],
                report_filters["to_date"],
                search_query=report_filters["search_query"],
            )
            if usage_error:
                messages.append(("danger", usage_error))
        elif report_type == "profit":
            profit_summary, profit_error = build_profit_report_summary(
                form_data.get("from_date"),
                form_data.get("to_date"),
            )
            if profit_error:
                messages.append(("danger", profit_error))
        elif report_type == "medicine":
            medicine_report, medicine_errors = build_medicine_report_data(
                report_filters["from_date"],
                report_filters["to_date"],
                medicine_query=report_filters["medicine_query"],
                top_n=report_filters["top_n"],
            )
            for error in medicine_errors:
                messages.append(("danger", error))
            report_filters["top_n"] = str(medicine_report["top_n"])
            medicine_summary = medicine_report["medicine_summary"]
            fast_movers = medicine_report["fast_movers"]
            medicine_totals = medicine_report["medicine_totals"]

        total = sum(invoice.total for invoice in invoices)

    return {
        "invoices": invoices,
        "total": round(total, 2),
        "report_type": report_type,
        "profit_summary": profit_summary,
        "medicine_summary": medicine_summary,
        "fast_movers": fast_movers,
        "medicine_totals": medicine_totals,
        "patient_medicine_patients": patient_medicine_patients,
        "patient_medicine_rows": patient_medicine_rows,
        "patient_medicine_summary": patient_medicine_summary,
        "report_filters": report_filters,
        "messages": messages,
    }
