#!/usr/bin/env python3
"""One-off cancelled-invoice rollback with explicit stock-shortage reconciliation.

This script does NOT invent purchases, medicine batches, or sellable quantity.
For the five identified unavailable replacement allocations it preserves the
later sales unchanged, reactivates the original cancelled invoice, and records
any unsupported historical quantity as an outstanding reconciliation shortage
in the existing audit_log table. This leaves a genuine inventory discrepancy
requiring later physical reconciliation; it does not fix that discrepancy.

Default is preview-only. Apply requires --auto-replace, --acknowledge-shortfall,
--apply. Operator must independently verify a fresh backup and an exclusive
maintenance window (the script cannot check these preconditions). Do not
run against a live active billing session. Use only on the four named invoices.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections import Counter
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
APP_DIR = ROOT / "billingwebapp"
for candidate in (str(ROOT), str(APP_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

os.environ.setdefault("DISABLE_SCHEDULER", "1")

try:
    from billingwebapp import app as app_module
except Exception:
    import app as app_module  # type: ignore


app = app_module.app
db = app_module.db
Invoice = app_module.Invoice
InvoiceItem = app_module.InvoiceItem
Medicine = app_module.Medicine
SalesAllocation = app_module.SalesAllocation
StockHistory = app_module.StockHistory
VendorPurchaseItem = app_module.VendorPurchaseItem
Return = app_module.Return
AuditLog = app_module.AuditLog


TARGET_INVOICES = ("INV-2026-1622", "INV-2026-1621", "INV-2026-1620", "INV-2026-1619")

# These are the ONLY five later-sale allocations that must remain unchanged.
# (cancelled invoice, later invoice, medicine, old batch, qty)
PROTECTED_SHORTAGES = frozenset({
    ("INV-2026-1620", "INV-00433", "GEMER 2 TAB 1*15", "GTH0870A", 30),
    ("INV-2026-1620", "INV-00434", "GLIZID M XR 60 TAB 1*15", "Z5FZZ001", 30),
    ("INV-2026-1619", "INV-00432", "CALCITAS D3 CAPSULE 1*1", "VHD2601006", 8),
    ("INV-2026-1619", "INV-00433", "CALCITAS D3 LIQUID 5 ML 1*1", "OBIK2519", 4),
    ("INV-2026-1619", "INV-00436", "DUXFOL D TAB 1*10", "GT32469D", 30),
})
EXPECTED_REGULAR_MOVES = 13
EXPECTED_SHORTFALL_QTY = sum(entry[4] for entry in PROTECTED_SHORTAGES)


def shortage_identity(move):
    return (
        str(move.invoice_no).strip().upper(),
        str(move.later_invoice_no).strip().upper(),
        " ".join(str(move.medicine_name).strip().upper().split()),
        str(move.old_batch).strip().upper(),
        int(move.qty),
    )


def is_protected_shortage(move):
    return shortage_identity(move) in PROTECTED_SHORTAGES


def same_tenant(*records):
    """Fail closed on mixed or missing tenant identifiers in scoped records."""
    scoped = [getattr(record, "tenant_id", None) for record in records
              if record is not None and hasattr(record, "tenant_id")]
    values = {v for v in scoped if v is not None}
    if len(values) > 1:
        raise RuntimeError(f"Tenant scope mismatch: {sorted(str(v) for v in values)}")
    if values and any(v is None for v in scoped):
        raise RuntimeError("Tenant scope is missing on one or more linked inventory records.")


def serialize_move(move):
    return (move.invoice_no, move.later_invoice_no, move.invoice_item_id,
            move.allocation_id, move.qty, move.old_purchase_item_id,
            move.new_purchase_item_id, move.medicine_name, move.old_batch,
            move.new_batch, move.blocked_reason)


def scoped_fields(model, record, fallback=None):
    """Carry tenant scope when inserting repair-derived rows."""
    if not hasattr(model, "tenant_id"):
        return {}
    tenant_id = getattr(record, "tenant_id", None)
    if tenant_id is None and fallback is not None:
        tenant_id = getattr(fallback, "tenant_id", None)
    return {"tenant_id": tenant_id}



@dataclass
class MovePlan:
    invoice_no: str
    invoice_item_id: int
    allocation_id: int
    qty: int
    old_purchase_item_id: int
    new_purchase_item_id: int | None = None
    later_invoice_no: str = ""
    medicine_name: str = ""
    old_batch: str = ""
    new_batch: str = ""
    blocked_reason: str = ""


@dataclass
class RollbackPlan:
    invoice_no: str
    invoice_id: int
    moves: list[MovePlan] = field(default_factory=list)
    direct_restore: list[tuple[int, int]] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)


def to_int(value, default=0):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def falsey_invoice_filter():
    return app_module.falsey_or_null_column_expr(Invoice.is_cancelled)


def get_invoice(invoice_no):
    matches = Invoice.query.filter(
        db.func.upper(Invoice.invoice_no) == invoice_no.upper()
    ).limit(2).all()
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous invoice {invoice_no}: multiple tenants or duplicate numbers.")
    return matches[0] if matches else None


def active_later_sales(old_purchase_item_id, target_invoice_id, cancelled_at):
    query = (
        db.session.query(SalesAllocation, InvoiceItem, Invoice)
        .join(InvoiceItem, InvoiceItem.id == SalesAllocation.invoice_item_id)
        .join(Invoice, Invoice.id == InvoiceItem.invoice_id)
        .filter(
            SalesAllocation.purchase_item_id == old_purchase_item_id,
            Invoice.id != target_invoice_id,
            falsey_invoice_filter(),
            SalesAllocation.qty > SalesAllocation.returned_qty,
        )
    )
    if cancelled_at:
        query = query.filter(Invoice.created_at >= cancelled_at)
    return query.order_by(Invoice.created_at.desc(), Invoice.id.desc(), SalesAllocation.id.desc()).all()


def replacement_lots_for(item, old_purchase_item_id, excluded_purchase_item_ids):
    today = datetime.now(timezone.utc).date()
    old_med = app_module.find_medicine_for_invoice_item(item)
    name = (getattr(item, "name", "") or "").strip()
    query = (
        db.session.query(VendorPurchaseItem, Medicine)
        .outerjoin(Medicine, VendorPurchaseItem.medicine_id == Medicine.id)
        .filter(
            db.func.lower(db.func.coalesce(VendorPurchaseItem.medicine_name, "")) == name.lower(),
            VendorPurchaseItem.id != old_purchase_item_id,
            db.func.coalesce(VendorPurchaseItem.remaining_qty, 0) > 0,
        )
    )
    if old_med and getattr(old_med, "medicine_code", None):
        query = query.filter(
            db.or_(
                VendorPurchaseItem.medicine_id == old_med.id,
                VendorPurchaseItem.medicine_code == old_med.medicine_code,
                db.func.lower(db.func.coalesce(VendorPurchaseItem.medicine_name, "")) == old_med.name.lower(),
            )
        )
    rows = []
    for purchase_item, med in query.order_by(VendorPurchaseItem.expiry.asc(), VendorPurchaseItem.created_at.asc()).all():
        if purchase_item.id in excluded_purchase_item_ids:
            continue
        batch = (purchase_item.batch or getattr(med, "batch", "") or "").strip()
        if not batch or batch == (item.batch or "").strip():
            continue
        expiry_value = (purchase_item.expiry or getattr(med, "expiry", "") or "").strip()
        expiry_date = app_module.parse_expiry_date(expiry_value)
        if expiry_date and expiry_date < today:
            continue
        candidate_med = med or Medicine.query.filter_by(name=name, batch=batch).first()
        if not candidate_med or to_int(candidate_med.qty) <= 0:
            continue
        if (candidate_med.name or "").strip().casefold() != name.casefold():
            continue
        if (candidate_med.batch or "").strip().casefold() != batch.casefold():
            continue
        if getattr(purchase_item, "tenant_id", None) != getattr(candidate_med, "tenant_id", None):
            # Mixed-scoped lots are never accepted as an automatic replacement.
            continue
        rows.append((purchase_item, candidate_med))
    return rows


def build_plan(invoice_numbers, auto_replace=False, preserve_known_shortages=False):
    plans = []
    for invoice_no in invoice_numbers:
        invoice = get_invoice(invoice_no)
        if not invoice:
            plans.append(RollbackPlan(invoice_no=invoice_no, invoice_id=0, blocked=["Invoice not found."]))
            continue
        plan = RollbackPlan(invoice_no=invoice.invoice_no, invoice_id=invoice.id)
        if not bool(getattr(invoice, "is_cancelled", False)):
            plan.blocked.append("Invoice is already active.")
            plans.append(plan)
            continue
        items = InvoiceItem.query.filter_by(invoice_id=invoice.id).order_by(InvoiceItem.id.asc()).all()
        for item in items:
            needed = to_int(item.qty)
            allocations = (
                SalesAllocation.query.filter_by(invoice_item_id=item.id)
                .order_by(SalesAllocation.id.desc())
                .all()
            )
            for alloc in allocations:
                if needed <= 0:
                    break
                restorable = min(to_int(alloc.returned_qty), needed)
                if restorable <= 0:
                    continue
                needed -= restorable
                if not alloc.purchase_item_id:
                    plan.direct_restore.append((alloc.id, restorable))
                    continue
                old_pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
                if not old_pi:
                    plan.blocked.append(f"Purchase lot missing for allocation {alloc.id}.")
                    continue
                available_now = to_int(old_pi.remaining_qty)
                shortage = max(0, restorable - available_now)
                if shortage <= 0:
                    plan.direct_restore.append((alloc.id, restorable))
                    continue
                remaining_shortage = shortage
                for later_alloc, later_item, later_invoice in active_later_sales(old_pi.id, invoice.id, invoice.cancelled_at):
                    if remaining_shortage <= 0:
                        break
                    movable = min(to_int(later_alloc.qty) - to_int(later_alloc.returned_qty), remaining_shortage)
                    if movable <= 0:
                        continue
                    move = MovePlan(
                        invoice_no=invoice.invoice_no,
                        later_invoice_no=later_invoice.invoice_no or str(later_invoice.id),
                        invoice_item_id=later_item.id,
                        allocation_id=later_alloc.id,
                        qty=movable,
                        old_purchase_item_id=old_pi.id,
                        medicine_name=later_item.name,
                        old_batch=later_item.batch or old_pi.batch or "",
                    )
                    if preserve_known_shortages and is_protected_shortage(move):
                        move.blocked_reason = "PROTECTED SHORTFALL: keep later sale unchanged"
                    elif auto_replace:
                        used = {m.new_purchase_item_id for p in plans + [plan] for m in p.moves if m.new_purchase_item_id}
                        candidates = replacement_lots_for(later_item, old_pi.id, used)
                        picked = None
                        for candidate_pi, candidate_med in candidates:
                            if to_int(candidate_pi.remaining_qty) >= movable and to_int(candidate_med.qty) >= movable:
                                picked = (candidate_pi, candidate_med)
                                break
                        if picked:
                            move.new_purchase_item_id = picked[0].id
                            move.new_batch = picked[0].batch or picked[1].batch or ""
                        else:
                            move.blocked_reason = "No replacement batch with enough available stock."
                    plan.moves.append(move)
                    remaining_shortage -= movable
                if remaining_shortage > 0:
                    plan.blocked.append(
                        f"{item.name} ({item.batch}) needs {shortage}, but only {shortage - remaining_shortage} later sale qty could be identified."
                    )
        plans.append(plan)
    return plans


def can_represent_move(later_item, move_qty):
    # A returned sale has separate return/inventory history attached to the
    # original line. Do not rewrite its invoice item or cost/batch history.
    active_allocs = SalesAllocation.query.filter_by(invoice_item_id=later_item.id).all()
    if not active_allocs or any(to_int(a.returned_qty) != 0 for a in active_allocs):
        return "blocked"
    if db.session.query(app_module.ReturnItem.id).join(
        Return, Return.id == app_module.ReturnItem.return_id
    ).filter(
        app_module.ReturnItem.invoice_item_id == later_item.id,
        app_module.falsey_or_null_column_expr(Return.is_cancelled),
    ).first():
        return "blocked"
    total_qty = sum(to_int(a.qty) for a in active_allocs)
    item_qty = to_int(later_item.qty)
    if total_qty != item_qty or not (0 < move_qty <= item_qty):
        return "blocked"
    return "replace" if move_qty == item_qty else "split"


def clone_split_item(item, qty, new_batch, new_expiry, new_cost_rate):
    original_qty = to_int(item.qty)
    if qty <= 0 or qty >= original_qty:
        return None
    ratio = qty / float(original_qty)
    old_remaining_qty = original_qty - qty

    def proportional(value):
        return app_module.round_currency(app_module.to_float_safe(value, 0) * ratio)

    parent_invoice = db.session.get(Invoice, item.invoice_id)
    new_item = InvoiceItem(
        **scoped_fields(InvoiceItem, item, parent_invoice),
        invoice_id=item.invoice_id,
        name=item.name,
        qty=qty,
        price=item.price,
        amount=proportional(item.amount),
        batch=new_batch,
        expiry=new_expiry,
        discount_percent=item.discount_percent,
        discount_amount=proportional(item.discount_amount),
        net_amount=proportional(item.net_amount),
        cost_price=new_cost_rate,
        cost_amount=app_module.round_currency(new_cost_rate * qty),
        gst_percent=item.gst_percent,
        taxable_amount=proportional(getattr(item, "taxable_amount", 0)),
        gst_amount=proportional(getattr(item, "gst_amount", 0)),
    )
    item.qty = old_remaining_qty
    item.amount = app_module.round_currency(app_module.to_float_safe(item.amount, 0) - new_item.amount)
    item.discount_amount = app_module.round_currency(app_module.to_float_safe(item.discount_amount, 0) - new_item.discount_amount)
    item.net_amount = app_module.round_currency(app_module.to_float_safe(item.net_amount, 0) - new_item.net_amount)
    item.taxable_amount = app_module.round_currency(app_module.to_float_safe(getattr(item, "taxable_amount", 0), 0) - new_item.taxable_amount)
    item.gst_amount = app_module.round_currency(app_module.to_float_safe(getattr(item, "gst_amount", 0), 0) - new_item.gst_amount)
    # Do not subtract replacement-batch cost from the original-batch cost.
    # Those batches may have different purchase rates. The exact remaining
    # allocation cost is recomputed immediately after splitting the allocation.
    item.cost_amount = app_module.round_currency(
        app_module.to_float_safe(getattr(item, "cost_amount", 0), 0) * old_remaining_qty / float(original_qty)
    )
    return new_item


def apply_move(move, actor):
    alloc = SalesAllocation.query.get(move.allocation_id)
    item = InvoiceItem.query.get(move.invoice_item_id)
    old_pi = VendorPurchaseItem.query.get(move.old_purchase_item_id)
    new_pi = VendorPurchaseItem.query.get(move.new_purchase_item_id)
    if not alloc or not item or not old_pi or not new_pi:
        raise RuntimeError(f"Move for allocation {move.allocation_id} has missing rows.")
    new_med = Medicine.query.filter(
        db.or_(
            Medicine.id == new_pi.medicine_id,
            db.and_(Medicine.name == new_pi.medicine_name, Medicine.batch == new_pi.batch),
        )
    ).order_by(Medicine.id.asc()).first()
    old_med = app_module.find_medicine_for_invoice_item(item)
    if not new_med or not old_med:
        raise RuntimeError(f"Move for allocation {move.allocation_id} has missing medicine rows.")
    later_invoice = db.session.get(Invoice, item.invoice_id)
    same_tenant(later_invoice, item, alloc, old_pi, new_pi, old_med, new_med)
    if old_med.id == new_med.id:
        raise RuntimeError(f"Replacement batch resolves to the SAME medicine row for {move.allocation_id}.")
    if (new_med.name or "").strip().casefold() != (item.name or "").strip().casefold():
        raise RuntimeError(f"Replacement medicine name differs for {move.allocation_id}.")
    if (new_pi.batch or "").strip().casefold() != (new_med.batch or "").strip().casefold():
        raise RuntimeError(f"Replacement batch/medicine row mismatch for {move.allocation_id}.")
    if (old_pi.batch or "").strip().casefold() != (old_med.batch or "").strip().casefold():
        raise RuntimeError(f"Original batch/medicine row mismatch for {move.allocation_id}.")
    if to_int(new_pi.remaining_qty) < move.qty or to_int(new_med.qty) < move.qty:
        raise RuntimeError(f"Replacement stock is insufficient for allocation {move.allocation_id}.")

    mode = can_represent_move(item, move.qty)
    if mode == "blocked":
        raise RuntimeError(f"Cannot safely represent move for invoice item {item.id}.")

    old_stock = to_int(old_med.qty)
    new_stock = to_int(new_med.qty)
    old_med.qty = old_stock + move.qty
    new_med.qty = new_stock - move.qty
    old_pi.remaining_qty = to_int(old_pi.remaining_qty) + move.qty
    new_pi.remaining_qty = to_int(new_pi.remaining_qty) - move.qty

    if mode == "replace":
        item.batch = new_pi.batch or new_med.batch
        item.expiry = new_pi.expiry or new_med.expiry
        item.cost_price = app_module.to_float_safe(new_pi.purchase_rate, item.cost_price or 0)
        item.cost_amount = app_module.round_currency(app_module.to_float_safe(item.cost_price, 0) * to_int(item.qty))
        alloc.purchase_item_id = new_pi.id
        alloc.cost_rate = app_module.to_float_safe(new_pi.purchase_rate, alloc.cost_rate or 0)
    else:
        new_item = clone_split_item(
            item,
            move.qty,
            new_pi.batch or new_med.batch,
            new_pi.expiry or new_med.expiry,
            app_module.to_float_safe(new_pi.purchase_rate, 0),
        )
        db.session.add(new_item)
        db.session.flush()
        alloc.qty = to_int(alloc.qty) - move.qty
        remaining_allocations = SalesAllocation.query.filter_by(invoice_item_id=item.id).all()
        remaining_cost = sum(
            to_int(row.qty) * app_module.to_float_safe(row.cost_rate, 0)
            for row in remaining_allocations
        )
        if sum(to_int(row.qty) for row in remaining_allocations) != to_int(item.qty):
            raise RuntimeError(f"Split left inconsistent quantity for invoice item {item.id}.")
        item.cost_amount = app_module.round_currency(remaining_cost)
        item.cost_price = round(remaining_cost / to_int(item.qty), 4)
        db.session.add(SalesAllocation(
            **scoped_fields(SalesAllocation, alloc, later_invoice),
            invoice_item_id=new_item.id,
            purchase_item_id=new_pi.id,
            qty=move.qty,
            cost_rate=app_module.to_float_safe(new_pi.purchase_rate, 0),
            returned_qty=0,
        ))

    db.session.add(StockHistory(
        **scoped_fields(StockHistory, old_med, later_invoice),
        medicine_id=old_med.id,
        medicine_name=old_med.name,
        batch=old_med.batch,
        action="ADJUSTMENT",
        stock_before=old_stock,
        qty_change=move.qty,
        stock_after=old_med.qty,
        user=actor,
        remark=f"Rollback repair: sale moved from invoice {move.later_invoice_no} to batch {new_pi.batch}",
        ref_table="invoice_item",
        ref_id=item.id,
    ))
    db.session.add(StockHistory(
        **scoped_fields(StockHistory, new_med, later_invoice),
        medicine_id=new_med.id,
        medicine_name=new_med.name,
        batch=new_med.batch,
        action="ADJUSTMENT",
        stock_before=new_stock,
        qty_change=-move.qty,
        stock_after=new_med.qty,
        user=actor,
        remark=f"Rollback repair: replacement for invoice {move.later_invoice_no}",
        ref_table="invoice_item",
        ref_id=item.id,
    ))



def validate_production_plan(plans):
    """Fail closed if today's production rows differ from the agreed 13+5 plan."""
    if set(p.invoice_no for p in plans) != set(TARGET_INVOICES) or len(plans) != 4:
        raise RuntimeError("Production shortage repair is limited to the four named invoices.")
    for plan in plans:
        if not plan.invoice_id or plan.blocked:
            raise RuntimeError(f"{plan.invoice_no}: blocked: {plan.blocked}")
    moves = [move for plan in plans for move in plan.moves]
    protected = [m for m in moves if is_protected_shortage(m)]
    regular = [m for m in moves if not is_protected_shortage(m)]
    if len(protected) != 5 or {shortage_identity(m) for m in protected} != PROTECTED_SHORTAGES:
        raise RuntimeError("The five protected allocations changed. Refusing to apply.")
    if len(regular) != EXPECTED_REGULAR_MOVES or len(moves) != 18:
        raise RuntimeError(f"Expected 13 regular + 5 protected allocations, found {len(regular)} + {len(protected)}.")
    if len({m.allocation_id for m in moves}) != len(moves):
        raise RuntimeError("Same sale allocation appears in more than one repair action.")
    if {m.invoice_item_id for m in protected} & {m.invoice_item_id for m in regular}:
        raise RuntimeError("A protected invoice item also has a regular move; cannot guarantee it stays unchanged.")
    if len({m.invoice_item_id for m in regular}) != len(regular):
        raise RuntimeError("Multiple planned moves target one later invoice item; manual review required.")
    for m in protected:
        if m.new_purchase_item_id is not None or m.blocked_reason != "PROTECTED SHORTFALL: keep later sale unchanged":
            raise RuntimeError(f"Protected allocation {m.allocation_id} was assigned a replacement.")
    for m in regular:
        if not m.new_purchase_item_id or m.blocked_reason:
            raise RuntimeError(f"Regular allocation {m.allocation_id} has no safe replacement.")
        if m.qty <= 0 or m.new_purchase_item_id == m.old_purchase_item_id:
            raise RuntimeError(f"Invalid allocation {m.allocation_id}.")
    return regular, protected


def lock_repair_rows(plans):
    """Lock the involved invoice, item, allocation, purchase, and medicine rows."""
    moves = [move for plan in plans for move in plan.moves]
    invoice_ids = {plan.invoice_id for plan in plans}
    for m in moves:
        item = db.session.get(InvoiceItem, m.invoice_item_id)
        if item:
            invoice_ids.add(item.invoice_id)
    for id_ in sorted(invoice_ids):
        Invoice.query.filter_by(id=id_).with_for_update().populate_existing().first()
    original_item_ids = set()
    for plan in plans:
        original_item_ids.update(i.id for i in InvoiceItem.query.filter_by(invoice_id=plan.invoice_id).all())
    item_ids = original_item_ids | {m.invoice_item_id for m in moves}
    for id_ in sorted(item_ids):
        InvoiceItem.query.filter_by(id=id_).with_for_update().populate_existing().first()
    allocation_ids = {m.allocation_id for m in moves}
    for item_id in original_item_ids:
        allocation_ids.update(a.id for a in SalesAllocation.query.filter_by(invoice_item_id=item_id).all())
    lot_ids = {m.old_purchase_item_id for m in moves}
    lot_ids.update(m.new_purchase_item_id for m in moves if m.new_purchase_item_id)
    for id_ in sorted(allocation_ids):
        row = SalesAllocation.query.filter_by(id=id_).with_for_update().populate_existing().first()
        if row and row.purchase_item_id:
            lot_ids.add(row.purchase_item_id)
    medicine_ids = set()
    for id_ in sorted(lot_ids):
        row = VendorPurchaseItem.query.filter_by(id=id_).with_for_update().populate_existing().first()
        if row and row.medicine_id:
            medicine_ids.add(row.medicine_id)
    for id_ in sorted(item_ids):
        item = db.session.get(InvoiceItem, id_)
        if item:
            med = app_module.find_medicine_for_invoice_item(item)
            if med:
                medicine_ids.add(med.id)
    for id_ in sorted(medicine_ids):
        Medicine.query.filter_by(id=id_).with_for_update().populate_existing().first()


def check_no_active_returns(invoice):
    if Return.query.filter(
        Return.invoice_id == invoice.id,
        app_module.falsey_or_null_column_expr(Return.is_cancelled),
    ).first():
        raise RuntimeError(f"{invoice.invoice_no}: active return bill exists.")
    if Return.query.filter(
        Return.adjusted_invoice_id == invoice.id,
        app_module.falsey_or_null_column_expr(Return.is_cancelled),
    ).first():
        raise RuntimeError(f"{invoice.invoice_no}: active exchange return exists.")


def add_shortfall_audit(invoice, item, allocation, purchase_lot, shortage_qty, actor):
    """Record an unresolved debt in audit_log, NOT a new stock/purchase lot."""
    med = app_module.find_medicine_for_invoice_item(item)
    if not med:
        raise RuntimeError(f"Missing medicine {item.name}/{item.batch} while auditing shortage.")
    details = {
        "type": "UNRESOLVED_INVENTORY_SHORTFALL",
        "invoice": invoice.invoice_no,
        "invoice_item_id": item.id,
        "allocation_id": allocation.id,
        "purchase_item_id": getattr(purchase_lot, "id", None),
        "medicine": item.name,
        "batch": item.batch,
        "expiry": item.expiry,
        "catalog_mrp_at_repair": getattr(med, "mrp", None),
        "original_invoice_price": item.price,
        "gst_percent": app_module.invoice_item_gst_percent(invoice, item),
        "unresolved_qty": shortage_qty,
        "status": "OPEN_PHYSICAL_STOCK_RECONCILIATION_REQUIRED",
    }
    kwargs = dict(
        user=actor,
        action="Invoice rollback stock shortfall",
        entity_type="INVOICE",
        entity_id=invoice.id,
        ref_code=invoice.invoice_no,
        before_json=None,
        after_json=json.dumps(details, sort_keys=True),
        extra_json=json.dumps({"actor": actor, "resolution_required": True}),
    )
    if hasattr(AuditLog, "tenant_id"):
        kwargs["tenant_id"] = getattr(invoice, "tenant_id", None)
    db.session.add(AuditLog(**kwargs))
    db.session.add(StockHistory(
        **scoped_fields(StockHistory, med, invoice),
        medicine_id=med.id,
        medicine_name=med.name,
        batch=med.batch,
        action="REPAIR_SHORTFALL",
        stock_before=to_int(med.qty),
        qty_change=0,
        stock_after=to_int(med.qty),
        user=actor,
        remark=f"Invoice {invoice.invoice_no} shortage qty={shortage_qty}, allocation={allocation.id}; reconcile physical stock"[:255],
        ref_table="invoice",
        ref_id=invoice.id,
    ))


def restore_with_recorded_shortfalls(invoice, actor, expected_shortfalls):
    """Undo cancellation while leaving a traceable, non-sellable stock deficit.

    Physical stock and purchase layer balances are never driven below zero.
    An audit row is written for each unit deficit. Original invoice item rows
    remain unchanged (name, batch, MRP snapshot/price, expiry, GST, discount).
    """
    if not bool(getattr(invoice, "is_cancelled", False)):
        raise RuntimeError(f"{invoice.invoice_no} is already active.")
    check_no_active_returns(invoice)
    actual = Counter()
    items = InvoiceItem.query.filter_by(invoice_id=invoice.id).order_by(InvoiceItem.id.asc()).all()
    for item in items:
        needed = to_int(item.qty)
        if needed < 0:
            raise RuntimeError(f"{invoice.invoice_no}: invalid invoice qty for item {item.id}.")
        if not needed:
            continue
        med = app_module.find_medicine_for_invoice_item(item)
        if not med:
            raise RuntimeError(f"Medicine row missing: {item.name} ({item.batch}).")
        same_tenant(invoice, item, med)
        original_med_qty = to_int(med.qty)
        if original_med_qty < 0:
            raise RuntimeError(f"Pre-existing negative medicine stock for {item.name}/{item.batch}.")
        actual_medicine_deduct = 0
        shortfall_for_item = 0
        allocations = SalesAllocation.query.filter_by(invoice_item_id=item.id).order_by(SalesAllocation.id.desc()).all()
        for alloc in allocations:
            if needed <= 0:
                break
            take = min(to_int(alloc.returned_qty), needed)
            if take <= 0:
                continue
            needed -= take
            same_tenant(invoice, alloc, item)
            pi = db.session.get(VendorPurchaseItem, alloc.purchase_item_id) if alloc.purchase_item_id else None
            if alloc.purchase_item_id and not pi:
                raise RuntimeError(f"Missing purchase item {alloc.purchase_item_id}.")
            same_tenant(invoice, pi)
            available_med = to_int(med.qty)
            available_pi = to_int(pi.remaining_qty) if pi else take
            if available_med < 0 or available_pi < 0:
                raise RuntimeError("Negative pre-existing inventory detected.")
            consumed = min(take, available_med, available_pi)
            if pi:
                pi.remaining_qty = available_pi - consumed
            med.qty = available_med - consumed
            alloc.returned_qty = to_int(alloc.returned_qty) - take
            actual_medicine_deduct += consumed
            missing = take - consumed
            if missing:
                shortfall_for_item += missing
                actual[(invoice.invoice_no, getattr(pi, "id", None))] += missing
                add_shortfall_audit(invoice, item, alloc, pi, missing, actor)
        if needed:
            raise RuntimeError(f"{invoice.invoice_no}: incomplete original sales allocation for {item.name}; missing {needed}.")
        db.session.add(StockHistory(
            **scoped_fields(StockHistory, med, invoice),
            medicine_id=med.id,
            medicine_name=med.name,
            batch=med.batch,
            action="INVOICE_RESTORE",
            stock_before=original_med_qty,
            qty_change=-actual_medicine_deduct,
            stock_after=to_int(med.qty),
            user=actor,
            remark=(f"Invoice {invoice.invoice_no} restored; stock deducted {actual_medicine_deduct}; "
                    f"unresolved shortage {shortfall_for_item}")[:255],
            ref_table="invoice",
            ref_id=invoice.id,
        ))
    expected = Counter({(invoice.invoice_no, lot_id): n
                        for (inv_no, lot_id), n in expected_shortfalls.items()
                        if inv_no == invoice.invoice_no})
    if actual != expected:
        raise RuntimeError(f"{invoice.invoice_no}: actual shortage {dict(actual)} differs from approved {dict(expected)}.")
    invoice.is_cancelled = False
    invoice.cancelled_at = None
    invoice.cancelled_by = None
    invoice.cancel_reason = None
    return actual



def unchanged_protected_sales(protected):
    """Snapshot later sale rows that user explicitly forbids modifying."""
    result = {}
    for move in protected:
        item = db.session.get(InvoiceItem, move.invoice_item_id)
        alloc = db.session.get(SalesAllocation, move.allocation_id)
        if item is None or alloc is None:
            raise RuntimeError("Protected later-sale row is missing.")
        result[move.allocation_id] = (
            alloc.invoice_item_id, alloc.purchase_item_id, to_int(alloc.qty), to_int(alloc.returned_qty),
            item.invoice_id, item.name, item.batch, item.expiry, to_int(item.qty),
            item.price, item.amount, item.discount_percent, item.discount_amount,
            item.net_amount, item.gst_percent, item.taxable_amount, item.gst_amount,
            item.cost_price, item.cost_amount,
        )
    return result


def later_invoice_amounts(plans):
    """Snapshot issued invoice headers, so repair cannot alter GST/paid totals."""
    ids = set()
    for plan in plans:
        for move in plan.moves:
            item = db.session.get(InvoiceItem, move.invoice_item_id)
            if item is None:
                raise RuntimeError(f"Missing invoice item {move.invoice_item_id}.")
            ids.add(item.invoice_id)
    result = {}
    for invoice_id in sorted(ids):
        inv = db.session.get(Invoice, invoice_id)
        if inv is None:
            raise RuntimeError(f"Missing later invoice {invoice_id}.")
        result[invoice_id] = (
            inv.invoice_no, inv.subtotal, inv.discount, inv.cgst, inv.sgst,
            inv.total, inv.payment_mode, inv.cash_amount, inv.online_amount,
            inv.is_cancelled,
        )
    return result


def apply_agreed_repair(plans, actor):
    """All four bills in one transaction, preserving five later sale allocations."""
    regular, protected = validate_production_plan(plans)
    original_signature = [serialize_move(m) for p in plans for m in p.moves]
    lock_repair_rows(plans)
    db.session.expire_all()
    latest_plans = build_plan(TARGET_INVOICES, auto_replace=True, preserve_known_shortages=True)
    validate_production_plan(latest_plans)
    latest_signature = [serialize_move(m) for p in latest_plans for m in p.moves]
    if original_signature != latest_signature:
        raise RuntimeError("The live invoice/stock plan changed during row locking. Nothing applied.")
    target_invoices = [db.session.get(Invoice, plan.invoice_id) for plan in latest_plans]
    same_tenant(*target_invoices)
    protected_snapshot = unchanged_protected_sales(protected)
    later_invoice_snapshot = later_invoice_amounts(latest_plans)
    for move in regular:
        alloc = db.session.get(SalesAllocation, move.allocation_id)
        item = db.session.get(InvoiceItem, move.invoice_item_id)
        old_pi = db.session.get(VendorPurchaseItem, move.old_purchase_item_id)
        new_pi = db.session.get(VendorPurchaseItem, move.new_purchase_item_id)
        same_tenant(item, alloc, old_pi, new_pi, *target_invoices)
        if alloc is None or item is None or old_pi is None or new_pi is None:
            raise RuntimeError("Move references a missing database row.")
        if alloc.invoice_item_id != item.id or alloc.purchase_item_id != old_pi.id:
            raise RuntimeError(f"Allocation {move.allocation_id} changed since preview.")
        if to_int(alloc.qty) - to_int(alloc.returned_qty) < move.qty:
            raise RuntimeError(f"Allocation {move.allocation_id} has insufficient unreturned qty.")
        apply_move(move, actor)
    db.session.flush()
    expected = Counter()
    for m in protected:
        expected[(m.invoice_no, m.old_purchase_item_id)] += m.qty
    actual = Counter()
    for plan in latest_plans:
        invoice = db.session.get(Invoice, plan.invoice_id)
        actual.update(restore_with_recorded_shortfalls(invoice, actor, expected))
    if actual != expected or sum(actual.values()) != EXPECTED_SHORTFALL_QTY:
        raise RuntimeError("Shortage reconciliation did not match the five approved exceptions.")
    db.session.flush()
    if unchanged_protected_sales(protected) != protected_snapshot:
        raise RuntimeError("Protected later sale changed; aborting all changes.")
    if later_invoice_amounts(latest_plans) != later_invoice_snapshot:
        raise RuntimeError("An issued later invoice header changed; aborting all changes.")
    for invoice in target_invoices:
        if bool(getattr(invoice, "is_cancelled", False)):
            raise RuntimeError(f"Invoice {invoice.invoice_no} was not restored.")
    return {"moved": len(regular), "preserved": len(protected), "unresolved_qty": sum(actual.values())}


def print_plan(plans):
    for plan in plans:
        print(f"\n{plan.invoice_no}")
        if plan.blocked:
            for reason in plan.blocked:
                print(f"  BLOCKED: {reason}")
        if not plan.moves:
            print("  No later sale moves required.")
        for move in plan.moves:
            target = f" -> purchase_item {move.new_purchase_item_id} batch {move.new_batch}" if move.new_purchase_item_id else ""
            blocked = (f" PRESERVED: {move.blocked_reason}" if is_protected_shortage(move)
                       else (f" BLOCKED: {move.blocked_reason}" if move.blocked_reason else ""))
            print(
                f"  later {move.later_invoice_no}: move {move.qty} x {move.medicine_name} "
                f"from batch {move.old_batch}{target}{blocked}"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Four-invoice cancellation rollback with explicit auditable inventory shortfall."
    )
    parser.add_argument("--invoices", nargs="+", default=list(TARGET_INVOICES))
    parser.add_argument("--auto-replace", action="store_true", help="Identify 13 safe replacement lots.")
    parser.add_argument("--apply", action="store_true", help="Mutate production DB (requires acknowledgement).")
    parser.add_argument("--acknowledge-shortfall", action="store_true",
                        help="Acknowledge 102 units remain genuinely short and must be physically reconciled.")
    parser.add_argument("--actor", default="system-repair")
    args = parser.parse_args()
    if set(a.upper() for a in args.invoices) != set(TARGET_INVOICES) or len(args.invoices) != 4:
        parser.error("This repair can only run on exactly the four pre-identified invoices.")
    if args.apply and not args.auto_replace:
        parser.error("--apply requires --auto-replace.")
    if args.apply and not args.acknowledge_shortfall:
        parser.error("--apply requires --acknowledge-shortfall and explicit stock reconciliation consent.")
    with app.app_context():
        plans = build_plan(TARGET_INVOICES,
                           auto_replace=args.auto_replace,
                           preserve_known_shortages=True)
        print_plan(plans)
        try:
            regular, protected = validate_production_plan(plans)
        except Exception as exc:
            db.session.rollback()
            print(f"\nPLAN NOT SAFE: {exc}", file=sys.stderr)
            return 2
        print(f"\nAGREED PREVIEW: {len(regular)} normal moves; {len(protected)} later-sales unchanged; "
              f"{EXPECTED_SHORTFALL_QTY} inventory units unresolved after invoice restore.")
        if not args.apply:
            db.session.rollback()
            print("DRY RUN ONLY. No changes applied. Obtain a NEW backup and stop live billing before applying.")
            return 0
        try:
            summary = apply_agreed_repair(plans, args.actor)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            print(f"\nFAILED; all repair changes rolled back: {exc}", file=sys.stderr)
            return 1
        print(f"\nAPPLIED: {summary['moved']} allocations moved; {summary['preserved']} later-sale allocations "
              f"untouched; {summary['unresolved_qty']} stock units remain in an OPEN audit discrepancy; "
              "all four original invoices restored.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
