from __future__ import annotations

try:
    from ..models import Invoice, Return, db
except ImportError:  # pragma: no cover - script/local fallback
    from models import Invoice, Return, db

import re
from collections import Counter
from datetime import datetime

from sqlalchemy import String, cast

NO_DOCTOR_LABEL = "Doctor not entered"
_DOCTOR_TITLE_RE = re.compile(r"^(?:(?:DR|DOCTOR)\b\s*)+")
_NO_DOCTOR_VALUES = {"NA", "N A", "NONE", "NIL", "NO"}


def _doctor_text(name):
    text = re.sub(r"[^A-Z0-9]+", " ", (name or "").upper()).strip()
    return _DOCTOR_TITLE_RE.sub("", text).strip()


def doctor_group_key(name):
    """Group key for the free-text doctor field on bills.

    "DR.ABHISHEK PRAKASH", "Dr Abhishek Prakash" and "abhishek prakash" share
    one key. Blank, one-letter ("d") and "NA"-style entries return "" and are
    reported as "Doctor not entered". Spelling mistakes are not merged.
    """
    text = _doctor_text(name)
    if len(text) < 2 or text in _NO_DOCTOR_VALUES:
        return ""
    return text


def doctor_name_suggestions(name_counts, *, min_uses=2, limit=50):
    """Doctor names for the billing Doctor field: the most used spelling of
    each doctor, busiest doctors first. ``name_counts`` is (name, uses) pairs.

    Doctors written on fewer than ``min_uses`` bills are left out, which drops
    most one-off typos.
    """
    groups = {}
    for raw_name, uses in name_counts:
        key = doctor_group_key(raw_name)
        if not key:
            continue
        groups.setdefault(key, Counter())[" ".join((raw_name or "").split())] += int(uses or 0)
    ranked = sorted(groups.values(), key=lambda spellings: -sum(spellings.values()))
    return [
        spellings.most_common(1)[0][0]
        for spellings in ranked
        if sum(spellings.values()) >= min_uses
    ][:limit]


def _active_return_filter(or_):
    """Include returns that have not been cancelled across supported databases.

    Older SQLite databases may store this flag as 0/1 or text, while Railway's
    PostgreSQL database stores it as a native BOOLEAN.  Comparing a PostgreSQL
    BOOLEAN directly to integer ``0`` raises ``operator does not exist:
    boolean = integer``.  Normalising the value to text keeps the report query
    portable without losing legacy return records.
    """
    normalized = db.func.lower(db.func.trim(cast(Return.is_cancelled, String)))
    return or_(
        Return.is_cancelled.is_(None),
        normalized.in_(("0", "false", "f", "no", "off", "")),
    )


def _active_invoice_query():
    """Invoices that count as sales: cancelled invoices are kept for GST
    records but must not be added to sales totals."""
    from sqlalchemy import or_ as _or

    normalized = db.func.lower(db.func.trim(cast(Invoice.is_cancelled, String)))
    return Invoice.query.filter(
        _or(Invoice.is_cancelled.is_(None), normalized.in_(("0", "false", "f", "no", "off", "")))
    )


def default_report_filters(fresh_start_date=None, current_date=None):
    from_date = ""
    to_date = ""
    if fresh_start_date and current_date and current_date >= fresh_start_date:
        from_date = fresh_start_date.isoformat()
        to_date = current_date.isoformat()
    return {
        "month": "",
        "year": "",
        "from_date": from_date,
        "to_date": to_date,
        "patient": "",
        "mobile": "",
        "search_query": "",
        "medicine_query": "",
        "doctor_query": "",
        "top_n": "10",
    }


def _invoice_patient_key(invoice):
    if getattr(invoice, "patient_id", None):
        return f"id:{invoice.patient_id}"
    mobile_digits = re.sub(r"\D", "", getattr(invoice, "mobile", "") or "")[-10:]
    if mobile_digits:
        return f"mobile:{mobile_digits}"
    name = " ".join((getattr(invoice, "customer", "") or "").lower().split())
    return f"name:{name}" if name else None


def build_doctor_wise_report(
    invoices,
    returns,
    *,
    return_doctor_by_invoice_id,
    summarize_invoice_collection,
    doctor_query="",
):
    """Pharmacy sale per doctor written on the bill.

    A return counts against the doctor on its original bill (looked up through
    ``return_doctor_by_invoice_id``), so the doctors' net sales add up to the
    Custom Date report's net sale for the same dates.
    """
    groups = {}

    def group_for(raw_name):
        key = doctor_group_key(raw_name)
        group = groups.setdefault(key, {"key": key, "spellings": Counter(), "invoices": [], "returns": []})
        if key:
            group["spellings"][" ".join((raw_name or "").split())] += 1
        return group

    for invoice in invoices:
        group_for(invoice.doctor)["invoices"].append(invoice)
    for return_bill in returns:
        group_for(return_doctor_by_invoice_id.get(return_bill.invoice_id))["returns"].append(return_bill)

    query_text = _doctor_text(doctor_query)
    if query_text:
        groups = {key: group for key, group in groups.items() if key and query_text in key}

    rows = []
    for group in groups.values():
        summary = summarize_invoice_collection(group["invoices"], group["returns"])
        spellings = [name for name, _count in group["spellings"].most_common()]
        bill_count = summary["invoice_count"]
        rows.append({
            "doctor": spellings[0] if spellings else NO_DOCTOR_LABEL,
            "other_spellings": spellings[1:],
            "is_unassigned": not group["key"],
            "bill_count": bill_count,
            "patient_count": len({k for k in map(_invoice_patient_key, group["invoices"]) if k}),
            "sale_total": summary["sale_total"],
            "return_total": summary["refund_total"],
            "net_sale": summary["net_sale_total"],
            "avg_bill": round(summary["sale_total"] / bill_count, 2) if bill_count else 0.0,
        })

    net_total = round(sum(row["net_sale"] for row in rows), 2)
    for row in rows:
        row["share_percent"] = round(row["net_sale"] * 100 / net_total, 1) if net_total > 0 else 0.0
    rows.sort(key=lambda row: (row["is_unassigned"], -row["net_sale"], row["doctor"].lower()))

    matched_invoices = [invoice for group in groups.values() for invoice in group["invoices"]]
    matched_returns = [return_bill for group in groups.values() for return_bill in group["returns"]]
    matched_invoices.sort(key=lambda invoice: invoice.created_at or datetime.min, reverse=True)
    totals = {
        "doctor_count": sum(1 for row in rows if not row["is_unassigned"]),
        "bill_count": sum(row["bill_count"] for row in rows),
        "patient_count": len({k for k in map(_invoice_patient_key, matched_invoices) if k}),
        "sale_total": round(sum(row["sale_total"] for row in rows), 2),
        "return_total": round(sum(row["return_total"] for row in rows), 2),
        "net_sale": net_total,
    }
    return {
        "rows": rows,
        "totals": totals,
        "doctor_filter": (doctor_query or "").strip(),
        "invoices": matched_invoices,
        "returns": matched_returns,
    }


def load_doctor_wise_report(
    from_date_value,
    to_date_value,
    *,
    doctor_query,
    local_date_range_to_storage_bounds,
    or_,
    summarize_invoice_collection,
):
    from_dt, to_dt = local_date_range_to_storage_bounds(from_date_value, to_date_value)
    invoices = _active_invoice_query().filter(
        Invoice.created_at >= from_dt,
        Invoice.created_at < to_dt,
    ).all()
    returns = Return.query.filter(
        Return.created_at >= from_dt,
        Return.created_at < to_dt,
        _active_return_filter(or_),
    ).all()
    doctor_by_invoice_id = {invoice.id: invoice.doctor for invoice in invoices}
    older_invoice_ids = {r.invoice_id for r in returns if r.invoice_id not in doctor_by_invoice_id}
    if older_invoice_ids:
        doctor_by_invoice_id.update(
            db.session.query(Invoice.id, Invoice.doctor).filter(Invoice.id.in_(older_invoice_ids)).all()
        )
    report = build_doctor_wise_report(
        invoices,
        returns,
        return_doctor_by_invoice_id=doctor_by_invoice_id,
        summarize_invoice_collection=summarize_invoice_collection,
        doctor_query=doctor_query,
    )
    report["from_date"] = from_date_value.isoformat()
    report["to_date"] = to_date_value.isoformat()
    return report


def build_reports_page_state(
    request_method,
    form_data,
    *,
    prevalidated_error=None,
    clinic_now,
    fresh_start_date,
    parse_date,
    to_int_safe,
    local_date_range_to_storage_bounds,
    normalize_patient_mobile,
    or_,
    build_patient_medicine_usage_report,
    build_profit_report_summary,
    build_medicine_report_data,
    build_invoice_payment_breakdown,
    summarize_invoice_collection,
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
    collection_summary = None
    invoice_rows = []
    returns = []
    doctor_report = None
    current_date = clinic_now().date()
    report_filters = default_report_filters(fresh_start_date=fresh_start_date, current_date=current_date)
    messages = []

    report_type = (form_data.get("report_type") or "").strip()
    default_cutover_view = False
    if request_method == "GET" and not report_type and fresh_start_date and current_date >= fresh_start_date:
        report_type = "custom"
        default_cutover_view = True

    should_build_report = request_method == "POST" or bool(report_type)

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
                "collection_summary": collection_summary,
                "invoice_rows": invoice_rows,
                "doctor_report": doctor_report,
                "report_filters": report_filters,
                "messages": messages,
            }
    elif should_build_report:
        for key in report_filters.keys():
            raw_value = (form_data.get(key) or "").strip()
            if raw_value:
                report_filters[key] = raw_value

    if should_build_report:
        if report_type == "daily":
            today = clinic_now().date()
            start_bound, end_bound = local_date_range_to_storage_bounds(today, today)
            invoices = _active_invoice_query().filter(
                Invoice.created_at >= start_bound,
                Invoice.created_at < end_bound,
            ).all()
            returns = Return.query.filter(
                Return.created_at >= start_bound,
                Return.created_at < end_bound,
                _active_return_filter(or_),
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
                invoices = _active_invoice_query().filter(
                    Invoice.created_at >= start_bound,
                    Invoice.created_at < end_bound,
                ).all()
                returns = Return.query.filter(
                    Return.created_at >= start_bound,
                    Return.created_at < end_bound,
                    _active_return_filter(or_),
                ).all()
        elif report_type == "custom":
            from_date = report_filters["from_date"]
            to_date = report_filters["to_date"]
            if from_date and to_date:
                from_date_value = parse_date(from_date)
                to_date_value = parse_date(to_date)
                if not from_date_value or not to_date_value:
                    messages.append(("danger", "Please enter valid from/to dates."))
                elif to_date_value < from_date_value:
                    messages.append(("danger", "To date must be greater than or equal to from date."))
                else:
                    from_dt, to_dt = local_date_range_to_storage_bounds(from_date_value, to_date_value)
                    invoices = _active_invoice_query().filter(
                        Invoice.created_at >= from_dt,
                        Invoice.created_at < to_dt,
                    ).all()
                    returns = Return.query.filter(
                        Return.created_at >= from_dt,
                        Return.created_at < to_dt,
                        _active_return_filter(or_),
                    ).all()
            else:
                messages.append(("danger", "Please select both from date and to date."))
        elif report_type == "patient":
            patient = (form_data.get("patient") or "").strip()
            if not patient:
                messages.append(("danger", "Please enter patient name."))
            else:
                invoices = _active_invoice_query().filter(Invoice.customer.ilike(f"%{patient}%")).all()
                returns = Return.query.filter(
                    Return.customer.ilike(f"%{patient}%"),
                    _active_return_filter(or_),
                ).all()
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
                    invoices = _active_invoice_query().filter(
                        or_(
                            normalized_mobile.like(f"%{mobile_digits}%"),
                            Invoice.mobile.ilike(f"%{mobile_raw}%"),
                        )
                    ).all()
                    normalized_return_mobile = db.func.replace(
                        db.func.replace(
                            db.func.replace(
                                db.func.replace(
                                    db.func.replace(
                                        db.func.replace(db.func.coalesce(Return.mobile, ""), " ", ""),
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
                    returns = Return.query.filter(
                        or_(
                            normalized_return_mobile.like(f"%{mobile_digits}%"),
                            Return.mobile.ilike(f"%{mobile_raw}%"),
                        ),
                        _active_return_filter(or_),
                    ).all()
                else:
                    invoices = _active_invoice_query().filter(Invoice.mobile.ilike(f"%{mobile_raw}%")).all()
                    returns = Return.query.filter(
                        Return.mobile.ilike(f"%{mobile_raw}%"),
                        _active_return_filter(or_),
                    ).all()
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
                report_filters["from_date"],
                report_filters["to_date"],
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
        elif report_type == "doctor":
            from_date_value = parse_date(report_filters["from_date"])
            to_date_value = parse_date(report_filters["to_date"])
            if not from_date_value or not to_date_value:
                messages.append(("danger", "Please select both from date and to date."))
            elif to_date_value < from_date_value:
                messages.append(("danger", "To date must be greater than or equal to from date."))
            else:
                doctor_report = load_doctor_wise_report(
                    from_date_value,
                    to_date_value,
                    doctor_query=report_filters["doctor_query"],
                    local_date_range_to_storage_bounds=local_date_range_to_storage_bounds,
                    or_=or_,
                    summarize_invoice_collection=summarize_invoice_collection,
                )
                invoices = doctor_report["invoices"]
                returns = doctor_report["returns"]

        collection_summary = summarize_invoice_collection(invoices, returns) if invoices or returns else None
        total = collection_summary["net_sale_total"] if collection_summary else sum(invoice.total for invoice in invoices)
        invoice_rows = [
            {
                "invoice": invoice,
                "payment": build_invoice_payment_breakdown(invoice),
            }
            for invoice in invoices
        ]

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
        "collection_summary": collection_summary,
        "invoice_rows": invoice_rows,
        "doctor_report": doctor_report,
        "report_filters": report_filters,
        "fresh_start_date": fresh_start_date.isoformat() if fresh_start_date else "",
        "default_cutover_view": default_cutover_view,
        "messages": messages,
    }
