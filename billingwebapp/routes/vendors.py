from __future__ import annotations

from flask import render_template

from services.inventory import build_vendor_purchase_summary


def render_vendor_reports_page(vendors):
    purchase_summary = build_vendor_purchase_summary(vendors)
    return render_template("vendor_reports.html", purchase_summary=purchase_summary)

