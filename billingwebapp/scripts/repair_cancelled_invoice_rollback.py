#!/usr/bin/env python3
"""Repair cancelled invoices whose restored stock was consumed again.

Default mode is a dry run. Use on production only after taking a database backup:

    python billingwebapp/scripts/repair_cancelled_invoice_rollback.py \
        --invoices INV-2026-1622 INV-2026-1621 INV-2026-1620 INV-2026-1619

Apply mode is intentionally conservative. It can move later sales to another
available batch only when the sale allocation can be represented safely in
invoice item rows.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
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


TARGET_INVOICES = ("INV-2026-1622", "INV-2026-1621", "INV-2026-1620", "INV-2026-1619")


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
    return Invoice.query.filter(db.func.upper(Invoice.invoice_no) == invoice_no.upper()).first()


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
    today = datetime.utcnow().date()
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
        rows.append((purchase_item, candidate_med))
    return rows


def build_plan(invoice_numbers, auto_replace=False):
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
                    if auto_replace:
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
    active_allocs = SalesAllocation.query.filter_by(invoice_item_id=later_item.id).all()
    active_qty = sum(to_int(a.qty) - to_int(a.returned_qty) for a in active_allocs)
    if move_qty == to_int(later_item.qty) and active_qty == move_qty:
        return "replace"
    if move_qty < to_int(later_item.qty):
        return "split"
    return "blocked"


def clone_split_item(item, qty, new_batch, new_expiry, new_cost_rate):
    original_qty = to_int(item.qty)
    if qty <= 0 or qty >= original_qty:
        return None
    ratio = qty / float(original_qty)
    old_remaining_qty = original_qty - qty

    def proportional(value):
        return app_module.round_currency(app_module.to_float_safe(value, 0) * ratio)

    new_item = InvoiceItem(
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
    item.cost_amount = app_module.round_currency(app_module.to_float_safe(getattr(item, "cost_amount", 0), 0) - new_item.cost_amount)
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
        db.session.add(SalesAllocation(
            invoice_item_id=new_item.id,
            purchase_item_id=new_pi.id,
            qty=move.qty,
            cost_rate=app_module.to_float_safe(new_pi.purchase_rate, 0),
            returned_qty=0,
        ))

    db.session.add(StockHistory(
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


def apply_plan(plans, actor):
    for plan in plans:
        if plan.blocked:
            raise RuntimeError(f"{plan.invoice_no} is blocked: {'; '.join(plan.blocked)}")
        for move in plan.moves:
            if not move.new_purchase_item_id or move.blocked_reason:
                raise RuntimeError(f"{plan.invoice_no} move blocked: {move.blocked_reason or 'missing replacement lot'}")
            apply_move(move, actor)
        invoice = Invoice.query.get(plan.invoice_id)
        error = app_module.restore_cancelled_invoice_record(
            invoice,
            actor_username=actor,
            reason="Production rollback repair",
        )
        if error:
            raise RuntimeError(f"{plan.invoice_no} restore failed after moves: {error}")


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
            blocked = f" BLOCKED: {move.blocked_reason}" if move.blocked_reason else ""
            print(
                f"  later {move.later_invoice_no}: move {move.qty} x {move.medicine_name} "
                f"from batch {move.old_batch}{target}{blocked}"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoices", nargs="+", default=list(TARGET_INVOICES))
    parser.add_argument("--auto-replace", action="store_true", help="Pick replacement lots automatically.")
    parser.add_argument("--apply", action="store_true", help="Apply the repair. Default is dry-run.")
    parser.add_argument("--actor", default="system-repair")
    args = parser.parse_args()

    with app.app_context():
        plans = build_plan(args.invoices, auto_replace=args.auto_replace)
        print_plan(plans)
        if not args.apply:
            print("\nDRY RUN ONLY. Re-run with --auto-replace --apply after backup if the plan is correct.")
            return 0
        try:
            apply_plan(plans, args.actor)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            print(f"\nFAILED: {exc}", file=sys.stderr)
            return 1
        print("\nAPPLIED successfully.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
