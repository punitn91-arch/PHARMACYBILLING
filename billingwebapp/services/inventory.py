from __future__ import annotations

from models import VendorPurchase, VendorPurchaseItem, db


def build_billing_page_context(medicines):
    medicine_names = sorted({(medicine.name or "").strip() for medicine in medicines if (medicine.name or "").strip()})
    medicine_data = []
    for medicine in medicines:
        medicine_data.append({
            "name": (medicine.name or "").strip().upper(),
            "batch": medicine.batch or "",
            "expiry": medicine.expiry or "",
            "stock": medicine.qty or 0,
            "mrp": float(medicine.mrp or 0),
            "discount": medicine.discount_percent or 0,
            "barcode": (medicine.barcode or "").strip(),
            "created_at": medicine.created_at.strftime("%Y-%m-%d") if medicine.created_at else "",
        })
    return {
        "medicine_names": medicine_names,
        "medicine_data": medicine_data,
    }


def build_vendor_purchase_summary(vendors):
    purchase_summary = []
    for vendor in vendors:
        total = (
            db.session.query(db.func.sum(VendorPurchase.total_amount))
            .filter_by(vendor_id=vendor.id)
            .scalar()
            or 0
        )
        count = (
            db.session.query(db.func.count(VendorPurchase.id))
            .filter_by(vendor_id=vendor.id)
            .scalar()
            or 0
        )
        mrp_value = (
            db.session.query(
                db.func.coalesce(
                    db.func.sum(
                        (db.func.coalesce(VendorPurchaseItem.qty, 0) + db.func.coalesce(VendorPurchaseItem.free_qty, 0))
                        * db.func.coalesce(VendorPurchaseItem.mrp, 0)
                    ),
                    0,
                )
            )
            .filter(
                VendorPurchaseItem.vendor_id == vendor.id,
                db.func.coalesce(VendorPurchaseItem.mrp, 0) > 0,
            )
            .scalar()
            or 0
        )
        cost_value = (
            db.session.query(
                db.func.coalesce(
                    db.func.sum(db.func.coalesce(VendorPurchaseItem.total_value, 0)),
                    0,
                )
            )
            .filter(
                VendorPurchaseItem.vendor_id == vendor.id,
                db.func.coalesce(VendorPurchaseItem.mrp, 0) > 0,
            )
            .scalar()
            or 0
        )
        margin_amount = mrp_value - cost_value
        margin_percent = (margin_amount / mrp_value * 100) if mrp_value else 0
        purchase_summary.append({
            "vendor": vendor,
            "total": round(total, 2),
            "count": count,
            "outstanding": round(vendor.outstanding_balance or 0, 2),
            "margin_amount": round(margin_amount, 2),
            "margin_percent": round(margin_percent, 2),
        })
    return purchase_summary

