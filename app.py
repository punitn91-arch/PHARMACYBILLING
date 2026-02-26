# app.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from models import db, User, Medicine, StockHistory, Invoice, InvoiceItem, Return, ReturnItem, HoldBill, Appointment, Vendor, VendorPurchase, VendorPurchaseItem, SalesAllocation, VendorNote, VendorNoteItem, VendorNoteAllocation, VendorLedgerEntry
from datetime import datetime, time, timedelta, date
from functools import wraps
import webbrowser
import threading
import os
from decimal import Decimal, ROUND_HALF_UP
from werkzeug.utils import secure_filename
from sqlalchemy import text, or_, inspect

def open_browser():
    webbrowser.open("http://127.0.0.1:5000")

IS_PROD = bool(
    os.environ.get("RAILWAY_ENVIRONMENT")
    or os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    or os.environ.get("RENDER")
    or os.environ.get("FLY_APP_NAME")
)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true" and not IS_PROD:
    threading.Timer(1, open_browser).start()


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# ---------------- UPLOADS ----------------
UPLOAD_FOLDER = os.path.join(app.root_path, "static", "uploads", "vendors")
ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg"}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def to_int(val):
    return int(val) if val not in (None, "", " ") else 0

def to_float(val):
    return float(val) if val not in (None, "", " ") else 0.0

def to_decimal(val, default="0"):
    if val in (None, "", " "):
        return Decimal(default)
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal(default)

def quantize_decimal(val, places="0.0001"):
    return to_decimal(val).quantize(Decimal(places), rounding=ROUND_HALF_UP)

def decimal_str(val):
    if val is None:
        return "0"
    try:
        return format(val, "f")
    except Exception:
        return str(val)

def parse_pack_qty(val):
    if val in (None, "", " "):
        return None
    v = str(val).strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None

def to_int_safe(val, default=0):
    try:
        return int(str(val).strip())
    except (TypeError, ValueError):
        return default

def to_float_safe(val, default=0.0):
    try:
        return float(str(val).strip())
    except (TypeError, ValueError):
        return default

def find_medicine_for_hold(name, batch=""):
    if not name:
        return None
    if batch:
        med = Medicine.query.filter_by(name=name, batch=batch).first()
        if med:
            return med
    return Medicine.query.filter_by(name=name).first()

def build_hold_items_from_form(form):
    items = []

    meds_list = form.getlist("medicine_name")
    qty_list = form.getlist("qty[]")
    if not qty_list:
        qty_list = form.getlist("qty")

    batch_overrides = form.getlist("batch_override[]")
    if not batch_overrides:
        batch_overrides = form.getlist("batch_override")

    expiry_list = form.getlist("line_expiry[]")
    qoh_list = form.getlist("line_qoh[]")
    mrp_list = form.getlist("line_mrp[]")
    discount_list = form.getlist("line_discount_percent[]")
    net_list = form.getlist("line_net[]")

    row_count = max(
        len(meds_list),
        len(qty_list),
        len(batch_overrides),
        len(expiry_list),
        len(mrp_list),
        len(discount_list),
        len(net_list)
    )

    for idx in range(row_count):
        name = (meds_list[idx] if idx < len(meds_list) else "").strip().upper()
        if not name:
            continue

        qty_val = to_int_safe(qty_list[idx] if idx < len(qty_list) else 0, 0)
        if qty_val <= 0:
            continue

        batch = (batch_overrides[idx] if idx < len(batch_overrides) else "").strip()
        expiry = (expiry_list[idx] if idx < len(expiry_list) else "").strip()
        qoh_raw = qoh_list[idx] if idx < len(qoh_list) else ""
        mrp_raw = mrp_list[idx] if idx < len(mrp_list) else ""
        discount_raw = discount_list[idx] if idx < len(discount_list) else ""
        net_raw = net_list[idx] if idx < len(net_list) else ""

        qoh = to_float_safe(qoh_raw, 0)
        mrp = to_float_safe(mrp_raw, 0)
        discount_percent = to_float_safe(discount_raw, 0)
        net_amount = to_float_safe(net_raw, 0)

        med = find_medicine_for_hold(name, batch)
        if med:
            if not batch:
                batch = med.batch or ""
            if not expiry:
                expiry = med.expiry or ""
            if qoh_raw in (None, "", " "):
                qoh = to_float_safe(med.qty, 0)
            if mrp_raw in (None, "", " "):
                mrp = to_float_safe(med.mrp, 0)
            if discount_raw in (None, "", " "):
                discount_percent = to_float_safe(med.discount_percent, 0)

        line_amount = qty_val * mrp
        if net_amount <= 0:
            net_amount = line_amount - (line_amount * discount_percent / 100)
        discount_amount = line_amount - net_amount
        if discount_amount < 0:
            discount_amount = 0

        items.append({
            "name": name,
            "batch": batch,
            "expiry": expiry,
            "qoh": round(qoh, 2),
            "qty": qty_val,
            "mrp": round(mrp, 2),
            "discount_percent": round(discount_percent, 2),
            "discount_amount": round(discount_amount, 2),
            "net_amount": round(net_amount, 2),
            "amount": round(line_amount, 2)
        })

    return items

def build_hold_totals_from_form(form, items):
    subtotal_raw = form.get("subtotal")
    discount_raw = form.get("discount")
    cgst_raw = form.get("cgst")
    sgst_raw = form.get("sgst")
    net_total_raw = form.get("net_total")
    rounded_raw = form.get("rounded_amount")

    subtotal = to_float_safe(subtotal_raw, 0.0) if subtotal_raw not in (None, "") else round(sum(i["net_amount"] for i in items), 2)
    discount = to_float_safe(discount_raw, 0.0) if discount_raw not in (None, "") else round(sum(i["discount_amount"] for i in items), 2)
    cgst = to_float_safe(cgst_raw, round(subtotal * 0.025, 2)) if cgst_raw not in (None, "") else round(subtotal * 0.025, 2)
    sgst = to_float_safe(sgst_raw, round(subtotal * 0.025, 2)) if sgst_raw not in (None, "") else round(subtotal * 0.025, 2)
    net_total = to_float_safe(net_total_raw, subtotal) if net_total_raw not in (None, "") else subtotal
    rounded_amount = to_float_safe(rounded_raw, round(net_total, 2)) if rounded_raw not in (None, "") else round(net_total, 2)

    return {
        "subtotal": round(subtotal, 2),
        "discount": round(discount, 2),
        "cgst": round(cgst, 2),
        "sgst": round(sgst, 2),
        "net_total": round(net_total, 2),
        "rounded_amount": round(rounded_amount, 2)
    }

def normalize_hold_bill_data(hold_bill):
    payload = hold_bill.data if isinstance(hold_bill.data, (dict, list)) else {}
    raw_items = []
    totals = {}
    header = {}

    if isinstance(payload, dict):
        header = payload.get("header") if isinstance(payload.get("header"), dict) else {}
        raw_items = payload.get("items") if isinstance(payload.get("items"), list) else []
        if not raw_items and isinstance(payload.get("cart"), list):
            raw_items = payload.get("cart")
        totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    elif isinstance(payload, list):
        raw_items = payload

    items = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        name = (raw.get("name") or raw.get("medicine_name") or "").strip().upper()
        if not name:
            continue
        qty_val = to_int_safe(raw.get("qty"), 0)
        if qty_val <= 0:
            continue
        batch = (raw.get("batch") or "").strip()
        expiry = (raw.get("expiry") or "").strip()
        qoh_source = raw.get("qoh", raw.get("stock", ""))
        mrp_source = raw.get("mrp", raw.get("price", ""))
        qoh = to_float_safe(qoh_source, 0)
        mrp = to_float_safe(mrp_source, 0)
        discount_percent = to_float_safe(raw.get("discount_percent", raw.get("discount", 0)), 0)
        net_amount = to_float_safe(raw.get("net_amount", raw.get("net", raw.get("amount", 0))), 0)

        med = find_medicine_for_hold(name, batch)
        if med:
            if not batch:
                batch = med.batch or ""
            if not expiry:
                expiry = med.expiry or ""
            if qoh_source in (None, "", " "):
                qoh = to_float_safe(med.qty, 0)
            if mrp_source in (None, "", " "):
                mrp = to_float_safe(med.mrp, 0)

        line_amount = qty_val * mrp
        if net_amount <= 0:
            net_amount = line_amount - (line_amount * discount_percent / 100)
        discount_amount = line_amount - net_amount
        if discount_amount < 0:
            discount_amount = 0

        items.append({
            "name": name,
            "batch": batch,
            "expiry": expiry,
            "qoh": round(qoh, 2),
            "qty": qty_val,
            "mrp": round(mrp, 2),
            "discount_percent": round(discount_percent, 2),
            "discount_amount": round(discount_amount, 2),
            "net_amount": round(net_amount, 2),
            "amount": round(line_amount, 2)
        })

    subtotal = to_float_safe(totals.get("subtotal"), round(sum(i["net_amount"] for i in items), 2))
    discount = to_float_safe(totals.get("discount"), round(sum(i["discount_amount"] for i in items), 2))
    cgst = to_float_safe(totals.get("cgst"), round(subtotal * 0.025, 2))
    sgst = to_float_safe(totals.get("sgst"), round(subtotal * 0.025, 2))
    net_total = to_float_safe(totals.get("net_total"), subtotal)
    rounded_amount = to_float_safe(totals.get("rounded_amount"), round(net_total, 2))

    return {
        "hold_bill_id": hold_bill.id,
        "header": {
            "customer": (header.get("customer") or hold_bill.customer or "").strip(),
            "mobile": (header.get("mobile") or hold_bill.mobile or "").strip(),
            "doctor": (header.get("doctor") or hold_bill.doctor or "").strip(),
            "gender": (header.get("gender") or hold_bill.gender or "").strip(),
            "sale_type": (header.get("sale_type") or "sale").strip().lower() or "sale",
            "payment_mode": (header.get("payment_mode") or "CASH").strip().upper() or "CASH"
        },
        "items": items,
        "totals": {
            "subtotal": round(subtotal, 2),
            "discount": round(discount, 2),
            "cgst": round(cgst, 2),
            "sgst": round(sgst, 2),
            "net_total": round(net_total, 2),
            "rounded_amount": round(rounded_amount, 2)
        }
    }

LOW_STOCK_LIMIT = 5
LOW_STOCK_TARGET = 10
LOW_STOCK_MIN_ORDER = 5

def get_low_stock_items(limit=LOW_STOCK_LIMIT, target_stock=LOW_STOCK_TARGET, min_order_qty=LOW_STOCK_MIN_ORDER):
    # Aggregate stock by medicine name across all batches.
    medicines = Medicine.query.order_by(
        db.func.lower(Medicine.name).asc(),
        Medicine.expiry.asc(),
        Medicine.batch.asc(),
        Medicine.id.asc()
    ).all()

    grouped = {}
    for med in medicines:
        name = (med.name or "").strip()
        if not name:
            continue
        key = name.lower()
        entry = grouped.get(key)
        if not entry:
            entry = {
                "name": name,
                "batch": (med.batch or "").strip() or "-",
                "expiry": (med.expiry or "").strip(),
                "stock": 0,
                "qty": 0,
                "batch_count": 0,
                "mrp": to_float(med.mrp)
            }
            grouped[key] = entry

        qty = to_int(med.qty)
        entry["stock"] += qty
        entry["qty"] = entry["stock"]
        entry["batch_count"] += 1

        expiry = (med.expiry or "").strip()
        if expiry and (not entry["expiry"] or expiry < entry["expiry"]):
            entry["expiry"] = expiry

        current_mrp = to_float(med.mrp)
        if entry["mrp"] is not None and entry["mrp"] != current_mrp:
            entry["mrp"] = None

    low_items = []
    for entry in grouped.values():
        if entry["stock"] > limit:
            continue
        if entry["batch_count"] > 1:
            entry["batch"] = f"{entry['batch_count']} batches"
        entry["suggested"] = max(target_stock - entry["stock"], min_order_qty)
        low_items.append(entry)

    low_items.sort(key=lambda item: item["name"].lower())
    return low_items

def generate_vendor_note_no(note_type):
    prefix = "VN-DN-" if note_type == "DEBIT" else "VN-CN-"
    last = VendorNote.query.filter_by(note_type=note_type).order_by(VendorNote.id.desc()).first()
    next_num = 1
    if last and last.note_no and last.note_no.startswith(prefix):
        try:
            next_num = int(last.note_no.replace(prefix, "")) + 1
        except Exception:
            next_num = last.id + 1
    return f"{prefix}{next_num:06d}"

def compute_vendor_note_totals(items):
    subtotal = Decimal("0")
    gst_total = Decimal("0")
    prepared = []
    for raw in items:
        qty = int(raw.get("qty") or 0)
        free_qty = int(raw.get("free_qty") or 0)
        rate = quantize_decimal(raw.get("purchase_rate") or 0, "0.0001")
        gst_percent = quantize_decimal(raw.get("gst_percent") or 0, "0.01")
        disc_percent = quantize_decimal(raw.get("disc_percent") or 0, "0.01")
        base = Decimal(qty) * rate
        disc_amt = base * disc_percent / Decimal("100")
        taxable = base - disc_amt
        gst_amt = taxable * gst_percent / Decimal("100")
        line_total = quantize_decimal(taxable + gst_amt, "0.0001")
        subtotal += taxable
        gst_total += gst_amt
        prepared.append({
            "qty": qty,
            "free_qty": free_qty,
            "purchase_rate": rate,
            "gst_percent": gst_percent,
            "disc_percent": disc_percent,
            "line_total": line_total
        })
    subtotal = quantize_decimal(subtotal, "0.0001")
    gst_total = quantize_decimal(gst_total, "0.0001")
    return subtotal, gst_total, prepared

def vendor_note_to_dict(note, include_items=False, include_ledger=False):
    data = {
        "id": note.id,
        "note_no": note.note_no,
        "note_type": note.note_type,
        "vendor_id": note.vendor_id,
        "reference_purchase_id": note.reference_purchase_id,
        "supplier_bill_no": note.supplier_bill_no,
        "note_date": note.note_date.isoformat() if note.note_date else None,
        "status": note.status,
        "reason_code": note.reason_code,
        "reason_text": note.reason_text,
        "subtotal": decimal_str(note.subtotal),
        "gst_total": decimal_str(note.gst_total),
        "round_off": decimal_str(note.round_off),
        "grand_total": decimal_str(note.grand_total),
        "remarks": note.remarks,
        "created_by": note.created_by,
        "created_at": note.created_at.isoformat() if note.created_at else None,
        "posted_at": note.posted_at.isoformat() if note.posted_at else None,
        "cancelled_at": note.cancelled_at.isoformat() if note.cancelled_at else None,
        "cancel_reason": note.cancel_reason
    }
    if include_items:
        data["items"] = [
            {
                "id": it.id,
                "medicine_id": it.medicine_id,
                "batch_no": it.batch_no,
                "expiry": it.expiry,
                "qty": it.qty,
                "free_qty": it.free_qty,
                "purchase_rate": decimal_str(it.purchase_rate),
                "mrp": decimal_str(it.mrp),
                "gst_percent": decimal_str(it.gst_percent),
                "disc_percent": decimal_str(it.disc_percent),
                "line_total": decimal_str(it.line_total),
                "hsn": it.hsn
            }
            for it in VendorNoteItem.query.filter_by(note_id=note.id).order_by(VendorNoteItem.id.asc()).all()
        ]
    if include_ledger:
        data["ledger"] = [
            {
                "id": l.id,
                "txn_date": l.txn_date.isoformat() if l.txn_date else None,
                "txn_type": l.txn_type,
                "debit": decimal_str(l.debit),
                "credit": decimal_str(l.credit),
                "notes": l.notes
            }
            for l in VendorLedgerEntry.query.filter_by(ref_table="vendor_notes", ref_id=note.id).order_by(VendorLedgerEntry.id.asc()).all()
        ]
    return data

def get_purchase_items_for_return(med, batch_no=None, reference_purchase_id=None):
    query = VendorPurchaseItem.query
    if reference_purchase_id:
        query = query.filter(VendorPurchaseItem.purchase_id == reference_purchase_id)
    query = query.filter(
        or_(
            VendorPurchaseItem.medicine_id == med.id,
            (VendorPurchaseItem.medicine_id.is_(None) &
             (VendorPurchaseItem.medicine_name == med.name) &
             (VendorPurchaseItem.batch == med.batch))
        )
    )
    if batch_no:
        query = query.filter(VendorPurchaseItem.batch == batch_no)
    return query.order_by(VendorPurchaseItem.created_at.asc(), VendorPurchaseItem.id.asc()).all()

def adjust_vendor_outstanding(vendor_id, delta):
    vendor = Vendor.query.get(vendor_id) if vendor_id else None
    if not vendor:
        return
    balance = to_decimal(vendor.outstanding_balance or 0) + to_decimal(delta or 0)
    if balance < Decimal("0"):
        balance = Decimal("0")
    vendor.outstanding_balance = float(quantize_decimal(balance, "0.01"))
    if vendor.outstanding_balance <= 0:
        vendor.payment_status = "Paid"
    elif (vendor.payment_status or "").strip().lower() == "paid":
        vendor.payment_status = "Unpaid"
    db.session.add(vendor)

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None

def parse_time_value(val):
    if not val:
        return None
    raw = str(val).strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return None

def generate_appointment_no():
    prefix = "APT-"
    last = Appointment.query.order_by(Appointment.id.desc()).first()
    next_num = 1
    if last and last.appointment_no and last.appointment_no.startswith(prefix):
        try:
            next_num = int(last.appointment_no.replace(prefix, "")) + 1
        except Exception:
            next_num = last.id + 1
    return f"{prefix}{next_num:06d}"

def get_doctor_suggestions():
    names = set()
    invoice_names = db.session.query(Invoice.doctor).filter(
        Invoice.doctor.isnot(None),
        Invoice.doctor != ""
    ).all()
    appt_names = db.session.query(Appointment.doctor_name).filter(
        Appointment.doctor_name.isnot(None),
        Appointment.doctor_name != ""
    ).all()
    for (name,) in invoice_names + appt_names:
        nm = (name or "").strip()
        if nm:
            names.add(nm)
    return sorted(names, key=lambda x: x.lower())

APPOINTMENT_STATUSES = ("BOOKED", "CHECKED_IN", "COMPLETED", "CANCELLED")
APPOINTMENT_PAYMENT_MODES = ("CASH", "ONLINE", "UPI", "CARD")

def find_medicine_by_name_batch(name, batch):
    n = (name or "").strip()
    b = (batch or "").strip()
    if not n or not b:
        return None
    return Medicine.query.filter(
        db.func.lower(Medicine.name) == n.lower(),
        db.func.lower(Medicine.batch) == b.lower()
    ).first()

def normalize_expiry(val):
    if not val:
        return ""
    v = str(val).strip()
    # MM/YYYY
    if len(v) == 7 and v[2] == "/":
        mm = v[:2]
        yy = v[3:]
        return f"{yy}-{mm}-01"
    # YYYY-MM
    if len(v) == 7 and v[4] == "-":
        return f"{v}-01"
    # YYYY-MM-DD
    if len(v) == 10 and v[4] == "-" and v[7] == "-":
        return v
    return v

def parse_expiry_date(val):
    v = normalize_expiry(val)
    if not v:
        return None
    try:
        return datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        return None

def get_batch_candidates(name):
    today = date.today()
    meds = Medicine.query.filter(
        Medicine.name == name,
        Medicine.qty > 0
    ).all()
    candidates = []
    for med in meds:
        exp_dt = parse_expiry_date(med.expiry)
        if exp_dt and exp_dt < today:
            continue
        purchase_dt = db.session.query(db.func.min(VendorPurchaseItem.created_at)).filter(
            or_(
                VendorPurchaseItem.medicine_id == med.id,
                (VendorPurchaseItem.medicine_id.is_(None) &
                 (VendorPurchaseItem.medicine_name == med.name) &
                 (VendorPurchaseItem.batch == med.batch))
            )
        ).scalar()
        if not purchase_dt:
            purchase_dt = med.created_at
        candidates.append((med, exp_dt or date.max, purchase_dt or datetime.max))
    candidates.sort(key=lambda x: (x[1], x[2], x[0].id))
    return [c[0] for c in candidates]

def get_purchase_items_for_med(med):
    return VendorPurchaseItem.query.filter(
        or_(
            VendorPurchaseItem.medicine_id == med.id,
            (VendorPurchaseItem.medicine_id.is_(None) &
             (VendorPurchaseItem.medicine_name == med.name) &
             (VendorPurchaseItem.batch == med.batch))
        )
    ).order_by(VendorPurchaseItem.created_at.asc(), VendorPurchaseItem.id.asc()).all()

def fifo_consume(med, qty):
    remaining = qty
    allocations = []
    purchase_items = get_purchase_items_for_med(med)
    for pi in purchase_items:
        available = to_int(pi.remaining_qty)
        if available <= 0:
            continue
        take = min(available, remaining)
        if take <= 0:
            continue
        pi.remaining_qty = available - take
        allocations.append({
            "purchase_item": pi,
            "qty": take,
            "cost_rate": to_float(pi.purchase_rate)
        })
        remaining -= take
        if remaining == 0:
            break
    if remaining > 0:
        allocations.append({
            "purchase_item": None,
            "qty": remaining,
            "cost_rate": to_float(med.mrp)
        })
    return allocations

def fifo_return(invoice_item_id, qty, fallback_rate):
    remaining = qty
    cost_total = 0.0
    allocations = SalesAllocation.query.filter_by(invoice_item_id=invoice_item_id).order_by(SalesAllocation.id.asc()).all()
    for alloc in allocations:
        available = to_int(alloc.qty) - to_int(alloc.returned_qty)
        if available <= 0:
            continue
        take = min(available, remaining)
        if take <= 0:
            continue
        if alloc.purchase_item_id:
            pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
            if pi:
                pi.remaining_qty = to_int(pi.remaining_qty) + take
        alloc.returned_qty = to_int(alloc.returned_qty) + take
        cost_total += take * to_float(alloc.cost_rate)
        remaining -= take
        if remaining == 0:
            break
    if remaining > 0:
        cost_total += remaining * to_float(fallback_rate)
    return cost_total

def fifo_cancel_return(invoice_item_id, qty, fallback_rate):
    remaining = qty
    cost_total = 0.0
    allocations = SalesAllocation.query.filter_by(invoice_item_id=invoice_item_id).order_by(SalesAllocation.id.desc()).all()
    for alloc in allocations:
        available = to_int(alloc.returned_qty)
        if available <= 0:
            continue
        take = min(available, remaining)
        if take <= 0:
            continue
        if alloc.purchase_item_id:
            pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
            if pi:
                new_remaining = to_int(pi.remaining_qty) - take
                if new_remaining < 0:
                    new_remaining = 0
                pi.remaining_qty = new_remaining
        alloc.returned_qty = to_int(alloc.returned_qty) - take
        cost_total += take * to_float(alloc.cost_rate)
        remaining -= take
        if remaining == 0:
            break
    if remaining > 0:
        cost_total += remaining * to_float(fallback_rate)
    return cost_total

# ---------------- DATABASE ----------------
os.makedirs(app.instance_path, exist_ok=True)
default_db = "sqlite:///" + os.path.join(app.instance_path, "pharmacy.db")
db_url = os.environ.get("DATABASE_URL", default_db)
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

# ---------------- INIT ----------------
with app.app_context():
    db.create_all()
    def ensure_column(table, column, col_def):
        try:
            insp = inspect(db.engine)
            existing = {c["name"] for c in insp.get_columns(table)}
        except Exception:
            # Fallback for SQLite/older engines
            cols = db.session.execute(text(f'PRAGMA table_info("{table}")')).fetchall()
            existing = {c[1] for c in cols}
        if column not in existing:
            db.session.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {col_def}'))
            db.session.commit()

    # Safe schema upgrades for return system (no data loss)
    try:
        # User permissions
        ensure_column("user", "can_view_medicine", "INTEGER")
        ensure_column("user", "can_add_medicine", "INTEGER")
        ensure_column("user", "can_edit_medicine", "INTEGER")
        ensure_column("user", "can_delete_medicine", "INTEGER")
        ensure_column("user", "can_edit_invoice", "INTEGER")
        ensure_column("user", "can_delete_invoice", "INTEGER")
        ensure_column("user", "can_invoice_action", "INTEGER")
        ensure_column("user", "can_view_stock_history", "INTEGER")
        ensure_column("user", "can_view_reports", "INTEGER")
        ensure_column("user", "can_manage_users", "INTEGER")

        # Medicine extra fields
        ensure_column("medicine", "composition", "TEXT")
        ensure_column("medicine", "company", "TEXT")
        ensure_column("medicine", "pack_type", "TEXT")
        ensure_column("medicine", "pack_qty", "INTEGER")

        # Vendor extra fields
        ensure_column("vendor", "shop_name", "TEXT")
        ensure_column("vendor", "area", "TEXT")
        ensure_column("vendor", "city", "TEXT")
        ensure_column("vendor", "state", "TEXT")
        ensure_column("vendor", "pincode", "TEXT")
        ensure_column("vendor", "account_holder_name", "TEXT")
        ensure_column("vendor_purchase", "invoice_no", "TEXT")
        ensure_column("vendor_purchase", "paid_amount", "REAL")

        ensure_column("return_bill", "return_no", "TEXT")
        ensure_column("return_bill", "payment_mode", "TEXT")
        ensure_column("return_bill", "cgst", "REAL")
        ensure_column("return_bill", "sgst", "REAL")
        ensure_column("return_bill", "is_cancelled", "INTEGER")
        ensure_column("return_bill", "cancelled_by", "TEXT")
        ensure_column("return_bill", "cancelled_at", "TEXT")
        ensure_column("return_item", "medicine_id", "INTEGER")
        ensure_column("return_item", "purchase_rate", "REAL")
        ensure_column("return_item", "selling_rate", "REAL")
        ensure_column("return_item", "gst_percent", "REAL")
        ensure_column("return_item", "reason", "TEXT")
        ensure_column("invoice_item", "cost_price", "REAL")
        ensure_column("invoice_item", "cost_amount", "REAL")
        ensure_column("return_item", "cost_price", "REAL")
        ensure_column("return_item", "cost_amount", "REAL")
        ensure_column("vendor_purchase_item", "remaining_qty", "INTEGER")
        ensure_column("vendor_purchase_item", "pack_type", "TEXT")
        ensure_column("vendor_purchase_item", "pack_qty", "INTEGER")
        ensure_column("stock_history", "ref_table", "TEXT")
        ensure_column("stock_history", "ref_id", "INTEGER")
        ensure_column("appointment", "payment_mode", "TEXT")
        ensure_column("appointment", "doctor_discount", "REAL")
        ensure_column("appointment", "consultation_fee", "REAL")
        db.session.execute(text(
            'UPDATE "appointment" '
            "SET \"payment_mode\" = 'CASH' "
            'WHERE "payment_mode" IS NULL OR "payment_mode" = \'\''
        ))
        db.session.execute(text(
            'UPDATE "appointment" '
            'SET "doctor_discount" = 0 '
            'WHERE "doctor_discount" IS NULL'
        ))
        db.session.execute(text(
            'UPDATE "appointment" '
            'SET "consultation_fee" = 0 '
            'WHERE "consultation_fee" IS NULL'
        ))
        db.session.commit()

        db.session.execute(text(
            'CREATE TABLE IF NOT EXISTS "sales_allocation" ('
            'id INTEGER PRIMARY KEY, '
            'invoice_item_id INTEGER NOT NULL, '
            'purchase_item_id INTEGER, '
            'qty INTEGER, '
            'cost_rate REAL, '
            'returned_qty INTEGER DEFAULT 0, '
            'created_at TEXT'
            ')'
        ))
        db.session.commit()
        ensure_column("sales_allocation", "returned_qty", "INTEGER")
        missing_remaining = db.session.execute(text(
            'SELECT COUNT(*) FROM "vendor_purchase_item" WHERE "remaining_qty" IS NULL'
        )).scalar()
        if missing_remaining:
            db.session.execute(text('UPDATE "vendor_purchase_item" SET "remaining_qty" = 0'))
            db.session.commit()
            medicines = Medicine.query.all()
            for med in medicines:
                stock_left = to_int(med.qty)
                purchase_items = VendorPurchaseItem.query.filter(
                    or_(
                        VendorPurchaseItem.medicine_id == med.id,
                        (VendorPurchaseItem.medicine_id.is_(None) &
                         (VendorPurchaseItem.medicine_name == med.name) &
                         (VendorPurchaseItem.batch == med.batch))
                    )
                ).order_by(VendorPurchaseItem.created_at.desc(), VendorPurchaseItem.id.desc()).all()
                for pi in purchase_items:
                    lot_qty = to_int(pi.qty) + to_int(pi.free_qty)
                    if stock_left <= 0:
                        pi.remaining_qty = 0
                        continue
                    take = lot_qty if stock_left >= lot_qty else stock_left
                    pi.remaining_qty = take
                    stock_left -= take
            db.session.commit()
        db.session.execute(text(
            'UPDATE "sales_allocation" '
            'SET "returned_qty" = 0 '
            'WHERE "returned_qty" IS NULL'
        ))
        db.session.commit()
    except Exception:
        # If schema upgrade fails, app should still run
        db.session.rollback()
        pass
    if not User.query.filter_by(username="admin").first():
        admin = User(username="admin", role="admin")
        admin.set_password("admin123")
        db.session.add(admin)
        db.session.commit()

# ---------------- AUTH DECORATOR ----------------
def login_required(f):
    @wraps(f)
    def w(*args, **kwargs):
        if not session.get("user_id"):
            return redirect("/login")
        return f(*args, **kwargs)
    return w

# ---------------- LOGIN ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            flash("Username and password are required", "danger")
            return render_template("login.html")
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            session["user_id"] = user.id
            session["username"] = user.username
            session["role"] = user.role
            session["can_view_medicine"] = user.can_view_medicine
            session["can_edit_invoice"] = user.can_edit_invoice
            session["can_delete_invoice"] = user.can_delete_invoice
            session["can_view_stock_history"] = user.can_view_stock_history
            session["can_view_reports"] = user.can_view_reports
            session["can_manage_users"] = user.can_manage_users
            # ✅ PERMISSIONS SESSION ME DAALO
            session["can_invoice_action"] = user.can_invoice_action
            session["can_add_medicine"] = user.can_add_medicine
            session["can_edit_medicine"] = user.can_edit_medicine
            session["can_delete_medicine"] = user.can_delete_medicine
            return redirect("/")
        flash("Invalid credentials", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")
# ---------------- ADD USER (ADMIN ONLY) ----------------
def admin_required(f):
    @wraps(f)
    def w(*args, **kwargs):
        if session.get("role") != "admin":
            flash("Access denied", "danger")
            return redirect("/")
        return f(*args, **kwargs)
    return w

def vendor_note_access_required(api=False):
    def decorator(f):
        @wraps(f)
        def w(*args, **kwargs):
            user = User.query.get(session.get("user_id"))
            allowed = bool(user and (user.role == "admin" or user.can_invoice_action))
            if allowed:
                return f(*args, **kwargs)
            if api:
                return jsonify({"error": "Access denied"}), 403
            flash("Access denied", "danger")
            return redirect("/")
        return w
    return decorator
# ---------------- DASHBOARD (DAILY LIVE SALE) ----------------
@app.route("/")
@login_required
def index():
    from datetime import date, timedelta

    # ---------- TODAY SALE ----------
    today = date.today()
    today_invoices = Invoice.query.filter(
        db.func.date(Invoice.created_at) == today
    ).all()

    today_sale = sum(i.total for i in today_invoices)
    bills_today = len(today_invoices)

    # ---------- LOW STOCK ----------
    low_stock_count = len(get_low_stock_items(limit=LOW_STOCK_LIMIT))

    # ---------- EXPIRING SOON (30 days) ----------
    exp_limit = today + timedelta(days=30)
    expiring_soon = Medicine.query.filter(
        Medicine.expiry <= exp_limit.strftime("%Y-%m-%d")
    ).count()

    # ---------- INVENTORY VALUE (AT PURCHASE RATE) ----------
    inventory_value = (
        db.session.query(
            db.func.coalesce(
                db.func.sum(
                    db.func.coalesce(VendorPurchaseItem.remaining_qty, 0) *
                    db.func.coalesce(VendorPurchaseItem.purchase_rate, 0)
                ),
                0
            )
        ).scalar()
        or 0
    )

    # ---------- GST COLLECTED (ONLY DISPLAY) ----------
    gst_collected = sum(
        (i.cgst + i.sgst) for i in today_invoices
    )

    return render_template(
        "index.html",
        today_sale=today_sale,
        bills_today=bills_today,
        low_stock=low_stock_count,
        expiring_soon=expiring_soon,
        inventory_value=inventory_value,
        gst_collected=gst_collected
    )
@app.route("/low-stock")
@login_required
def low_stock():
    medicines = get_low_stock_items(limit=LOW_STOCK_LIMIT)
    return render_template("low_stock.html", medicines=medicines)
@app.route("/order-list")
@login_required
def order_list():
    medicines = get_low_stock_items(limit=LOW_STOCK_LIMIT)

    order_items = []
    for m in medicines:
        order_items.append({
            "name": m["name"],
            "batch": m["batch"],
            "stock": m["stock"],
            "suggested": m["suggested"]
        })

    return render_template("order_list.html", items=order_items)

@app.route("/expiring-soon")
@login_required
def expiring_soon():
    from datetime import date, timedelta
    limit = (date.today() + timedelta(days=30)).strftime("%Y-%m-%d")

    medicines = Medicine.query.filter(
        Medicine.expiry <= limit
    ).order_by(Medicine.expiry).all()

    return render_template("expiring.html", medicines=medicines)


# ---------------- MEDICINES ----------------
@app.route("/medicines")
@login_required
def medicines():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (
        user.can_view_medicine or user.can_add_medicine or user.can_edit_medicine or user.can_delete_medicine
    ):
        flash("Access denied", "danger")
        return redirect("/")
    medicines = Medicine.query.order_by(
        db.func.lower(Medicine.name).asc(),
        Medicine.batch.asc(),
        Medicine.id.asc()
    ).all()
    return render_template("medicines.html", medicines=medicines)

@app.route("/medicines/add", methods=["GET", "POST"])
@login_required
def add_medicine():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_add_medicine:
        flash("Access denied", "danger")
        return redirect("/medicines")
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        batch = (request.form.get("batch") or "").strip()
        expiry = (request.form.get("expiry") or "").strip()
        if not name or not batch or not expiry:
            flash("Name, batch and expiry are required", "danger")
            return redirect("/medicines/add")
        qty = to_int(request.form.get("qty"))
        pack_type = (request.form.get("pack_type") or "").strip()
        pack_qty_raw = (request.form.get("pack_qty") or "").strip()
        pack_qty = parse_pack_qty(pack_qty_raw)
        if pack_qty_raw and (pack_qty is None or pack_qty < 1):
            flash("Pack quantity must be at least 1", "danger")
            return redirect("/medicines/add")

        med = Medicine(
            name=name,
            batch=batch,
            mrp=to_float(request.form.get("mrp")),
            qty=qty,
            expiry=expiry,
            pack_type=pack_type,
            pack_qty=pack_qty
        )

        db.session.add(med)
        
        db.session.commit()

        history = StockHistory(
            medicine_id=med.id,
            medicine_name=med.name,
            batch=med.batch,
            action="ADD",
            stock_before=0,
            qty_change=qty,
            stock_after=qty,
            user=session.get("username"),
            remark="New medicine added"
        )

        db.session.add(history)
        db.session.commit()

        flash("Medicine added successfully", "success")
        return redirect("/medicines")

    return render_template("add_medicine.html")

@app.route("/medicines/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_medicine(id):
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_edit_medicine:
        flash("Access denied", "danger")
        return redirect("/medicines")
    med = Medicine.query.get_or_404(id)
    old_qty = med.qty

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        batch = (request.form.get("batch") or "").strip()
        expiry = (request.form.get("expiry") or "").strip()
        if not name or not batch or not expiry:
            flash("Name, batch and expiry are required", "danger")
            return redirect(request.url)
        med.name = name
        med.batch = batch
        med.expiry = expiry
        med.mrp = to_float(request.form.get("mrp"))
        med.discount_percent = to_int(request.form.get("discount_percent"))
        med.qty = to_int(request.form.get("qty"))
        pack_type = (request.form.get("pack_type") or "").strip()
        pack_qty_raw = (request.form.get("pack_qty") or "").strip()
        pack_qty = parse_pack_qty(pack_qty_raw)
        if pack_qty_raw and (pack_qty is None or pack_qty < 1):
            flash("Pack quantity must be at least 1", "danger")
            return redirect(request.url)
        if pack_type:
            med.pack_type = pack_type
        if pack_qty_raw:
            med.pack_qty = pack_qty

        change = med.qty - old_qty

        if change != 0:
            history = StockHistory(
                medicine_id=med.id,
                medicine_name=med.name,
                batch=med.batch,
                action="EDIT",
                stock_before=old_qty,
                qty_change=change,
                stock_after=med.qty,
                user=session.get("username"),
                remark="Medicine edited"
            )
            db.session.add(history)

        db.session.commit()
        flash("Medicine updated successfully", "success")
        return redirect("/medicines")

    return render_template("edit_medicine.html", med=med)

@app.route("/medicines/delete/<int:id>")
@login_required
def delete_medicine(id):
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_delete_medicine:
        flash("Access denied", "danger")
        return redirect("/medicines")
    med = Medicine.query.get_or_404(id)

    # 🔴 STOCK HISTORY ENTRY (DELETE)
    history = StockHistory(
        medicine_name=med.name,
        batch=med.batch,
        action="DELETE",
        stock_before=med.qty,
        qty_change=-med.qty,
        stock_after=0,
        user=session.get("username"),
        remark="Medicine deleted from inventory"
    )
    db.session.add(history)

    # 🔴 DELETE MEDICINE
    db.session.delete(med)
    db.session.commit()

    flash("Medicine deleted successfully", "danger")
    return redirect("/medicines")


# ---------------- BILLING ----------------
@app.route("/billing", methods=["GET", "POST"])
@login_required
def billing():
    meds = Medicine.query.order_by(Medicine.name).all()
    medicine_names = sorted({m.name for m in meds})
    medicine_data = []
    for m in meds:
        medicine_data.append({
            "name": (m.name or "").strip().upper(),
            "batch": m.batch or "",
            "expiry": m.expiry or "",
            "stock": m.qty or 0,
            "mrp": float(m.mrp or 0),
            "discount": m.discount_percent or 0,
            "created_at": m.created_at.strftime("%Y-%m-%d") if m.created_at else ""
        })
    
    cart = []
    posted_hold_bill_id = to_int_safe(request.form.get("hold_bill_id"), 0) if request.method == "POST" else 0

    def redirect_to_billing_with_context():
        if posted_hold_bill_id > 0:
            return redirect(url_for("billing", hold_bill_id=posted_hold_bill_id))
        return redirect("/billing")

    if request.method == "POST":
        subtotal = 0
        total_discount = 0

        meds_list = request.form.getlist("medicine_name")
        batch_overrides = request.form.getlist("batch_override[]")
        if not batch_overrides:
            batch_overrides = request.form.getlist("batch_override")
        qty_list = request.form.getlist("qty")
        if not qty_list:
            qty_list = request.form.getlist("qty[]")

        for idx, (name, qty) in enumerate(zip(meds_list, qty_list)):
            name = (name or "").strip().upper()
            if not name:
                continue
            qty = to_int_safe(qty, 0)
            if qty <= 0:
                continue
            batch_override = (batch_overrides[idx] or "").strip() if idx < len(batch_overrides) else ""
            allocations = []

            if batch_override:
                med = Medicine.query.filter_by(name=name, batch=batch_override).first()
                if not med:
                    flash(f"Batch not found for {name}", "danger")
                    return redirect_to_billing_with_context()
                exp_dt = parse_expiry_date(med.expiry)
                if exp_dt and exp_dt < date.today():
                    flash(f"Batch {med.batch} of {med.name} is expired", "danger")
                    return redirect_to_billing_with_context()
                if qty > med.qty:
                    flash(f"Not enough stock for {med.name} ({med.batch})", "danger")
                    return redirect_to_billing_with_context()
                allocations.append((med, qty))
            else:
                candidates = get_batch_candidates(name)
                total_available = sum(m.qty for m in candidates)
                if total_available <= 0:
                    flash(f"No stock available for {name}", "danger")
                    return redirect_to_billing_with_context()
                if qty > total_available:
                    flash(f"Not enough stock for {name}. Available: {total_available}", "danger")
                    return redirect_to_billing_with_context()
                remaining = qty
                for med in candidates:
                    take = min(med.qty, remaining)
                    if take <= 0:
                        continue
                    allocations.append((med, take))
                    remaining -= take
                    if remaining == 0:
                        break

            for med, take_qty in allocations:
                discp = med.discount_percent or 0
                amount = take_qty * med.mrp
                disc_amt = amount * discp / 100
                net_amt = amount - disc_amt
                subtotal += net_amt
                total_discount += disc_amt

                old_stock = med.qty
                med.qty -= take_qty

                history = StockHistory(
                    medicine_id=med.id,
                    medicine_name=med.name,
                    batch=med.batch,
                    action="SALE",
                    stock_before=old_stock,
                    qty_change=-take_qty,
                    stock_after=med.qty,
                    user=session.get("username"),
                    remark="Invoice sale"
                )
                db.session.add(history)

                fifo_alloc = fifo_consume(med, take_qty)
                cost_amount = sum(a["qty"] * a["cost_rate"] for a in fifo_alloc)
                cost_price = round(cost_amount / take_qty, 4) if take_qty else 0

                cart.append({
                    "name": med.name,
                    "qty": take_qty,
                    "price": med.mrp,
                    "amount": amount,
                    "batch": med.batch,
                    "expiry": med.expiry,
                    "discount_percent": discp,
                    "discount_amount": disc_amt,
                    "net_amount": net_amt,
                    "cost_price": cost_price,
                    "cost_amount": cost_amount,
                    "allocations": fifo_alloc
                })

        cgst = round(subtotal * 0.025, 2)
        sgst = round(subtotal * 0.025, 2)
        total = round(subtotal, 2)
        rounded_total = round(subtotal)

        last = Invoice.query.order_by(Invoice.id.desc()).first()
        inv_no = f"INV-{datetime.now().year}-{1000 + ((last.id + 1) if last else 1)}"
        customer = (request.form.get("customer") or "").strip()
        mobile = (request.form.get("mobile") or "").strip()
        inv = Invoice(
            invoice_no=inv_no,
            customer=customer,
            mobile=mobile,
            doctor=request.form.get("doctor", ""),
            gender=request.form.get("gender", ""),
            subtotal=subtotal,
            discount=total_discount,
            cgst=cgst,
            sgst=sgst,
            total=total,
            payment_mode=request.form.get("payment_mode", "CASH"),
            created_by=session.get("username")
        )

        db.session.add(inv)
        db.session.flush()

        for item in cart:
            inv_item = InvoiceItem(
                invoice_id=inv.id,
                name=item["name"],
                qty=item["qty"],
                price=item["price"],
                amount=item["amount"],
                batch=item["batch"],
                expiry=item["expiry"],
                discount_percent=item["discount_percent"],
                discount_amount=item["discount_amount"],
                net_amount=item["net_amount"],
                cost_price=item["cost_price"],
                cost_amount=item["cost_amount"]
            )
            db.session.add(inv_item)
            db.session.flush()
            for alloc in item["allocations"]:
                db.session.add(SalesAllocation(
                    invoice_item_id=inv_item.id,
                    purchase_item_id=alloc["purchase_item"].id if alloc["purchase_item"] else None,
                    qty=alloc["qty"],
                    cost_rate=alloc["cost_rate"],
                    returned_qty=0
                ))

        if posted_hold_bill_id > 0:
            held_bill = HoldBill.query.get(posted_hold_bill_id)
            if held_bill:
                db.session.delete(held_bill)

        db.session.commit()

        return render_template(
            "invoice.html",
            inv=inv,
            cart=cart,
            customer=inv.customer,
            mobile=inv.mobile,
            doctor=inv.doctor,
            gender=inv.gender,
            invoice_no=inv.invoice_no,
            subtotal=subtotal,
            discount=total_discount,
            cgst=cgst,
            sgst=sgst,
            total=total,
            rounded_total=rounded_total,
            date=datetime.now().strftime("%d-%m-%Y")
        )

    restored_hold_bill = None
    restore_hold_bill_id = to_int_safe(request.args.get("hold_bill_id"), 0)
    if restore_hold_bill_id > 0:
        hold_bill = HoldBill.query.get(restore_hold_bill_id)
        if not hold_bill:
            flash("Held bill not found. It may have been deleted.", "warning")
        else:
            restored_hold_bill = normalize_hold_bill_data(hold_bill)

    return render_template(
        "billing.html",
        medicines=meds,
        medicine_names=medicine_names,
        medicine_data=medicine_data,
        restored_hold_bill=restored_hold_bill
    )


# ---------------- RETURN MEDICINE ----------------
@app.route("/return-medicine", methods=["GET", "POST"])
@login_required
def return_medicine():
    invoice = None
    items_payload = []
    medicines = Medicine.query.order_by(Medicine.name).all()

    invoice_no = request.args.get("invoice_no", "").strip()
    search_customer = request.args.get("customer", "").strip()
    search_from = request.args.get("from", "").strip()
    search_to = request.args.get("to", "").strip()
    search_results = []

    if request.method == "POST":
        mode = request.form.get("mode", "invoice")

        # ---------------- MANUAL RETURN ----------------
        if mode == "manual":
            customer = request.form.get("manual_customer", "").strip()
            mobile = request.form.get("manual_mobile", "").strip()
            payment_mode = request.form.get("manual_payment_mode", "CASH")
            original_invoice_no = request.form.get("original_invoice_no", "").strip()

            ids = request.form.getlist("manual_medicine_id")
            qtys = request.form.getlist("manual_qty")
            sold_qtys = request.form.getlist("manual_sold_qty")
            selling_rates = request.form.getlist("manual_selling_rate")
            purchase_rates = request.form.getlist("manual_purchase_rate")
            gst_percents = request.form.getlist("manual_gst")
            reasons = request.form.getlist("manual_reason")

            total_refund = 0
            total_cgst = 0
            total_sgst = 0
            line_items = []

            for idx, mid in enumerate(ids):
                if not mid:
                    continue
                med = Medicine.query.get(int(mid))
                if not med:
                    continue
                qty = int(qtys[idx] or 0)
                if qty <= 0:
                    continue

                sold_qty = int(sold_qtys[idx] or 0)
                if sold_qty and qty > sold_qty:
                    flash(f"Return qty cannot exceed sold qty for {med.name} ({med.batch})", "danger")
                    return redirect("/return-medicine")

                selling_rate = float(selling_rates[idx] or med.mrp or 0)
                purchase_rate = float(purchase_rates[idx] or 0)
                gst_percent = float(gst_percents[idx] or 0)
                reason = (reasons[idx] or "").strip()

                amount = qty * selling_rate
                tax_amt = amount * gst_percent / 100
                cgst_amt = tax_amt / 2
                sgst_amt = tax_amt / 2
                net_amt = amount

                total_refund += amount + tax_amt
                total_cgst += cgst_amt
                total_sgst += sgst_amt

                line_items.append({
                    "med": med,
                    "qty": qty,
                    "selling_rate": selling_rate,
                    "purchase_rate": purchase_rate,
                    "gst_percent": gst_percent,
                    "reason": reason,
                    "amount": amount,
                    "net_amount": net_amt
                })

            if not line_items:
                flash("Please select at least one medicine to return", "danger")
                return redirect("/return-medicine")

            ret = Return(
                invoice_id=0,
                invoice_no=original_invoice_no,
                customer=customer,
                mobile=mobile,
                total_refund=0,
                cgst=round(total_cgst, 2),
                sgst=round(total_sgst, 2),
                payment_mode=payment_mode,
                created_by=session.get("username")
            )
            db.session.add(ret)
            db.session.flush()
            ret.return_no = f"RB-{ret.id:06d}"

            for item in line_items:
                med = item["med"]
                old_stock = med.qty
                med.qty += item["qty"]

                history = StockHistory(
                    medicine_id=med.id,
                    medicine_name=med.name,
                    batch=med.batch,
                    action="RETURN",
                    stock_before=old_stock,
                    qty_change=item["qty"],
                    stock_after=med.qty,
                    user=session.get("username"),
                    remark=f"Return Bill {ret.return_no}"
                )
                db.session.add(history)

                db.session.add(ReturnItem(
                    return_id=ret.id,
                    invoice_item_id=0,
                    medicine_id=med.id,
                    medicine_name=med.name,
                    batch=med.batch,
                    expiry=med.expiry,
                    qty=item["qty"],
                    price=item["selling_rate"],
                    amount=item["amount"],
                    purchase_rate=item["purchase_rate"],
                    selling_rate=item["selling_rate"],
                    gst_percent=item["gst_percent"],
                    reason=item["reason"],
                    discount_percent=0,
                    discount_amount=0,
                    net_amount=item["net_amount"],
                    cost_price=item["purchase_rate"] if item["purchase_rate"] else item["selling_rate"],
                    cost_amount=(item["purchase_rate"] if item["purchase_rate"] else item["selling_rate"]) * item["qty"]
                ))

            ret.total_refund = round(total_refund, 2)
            db.session.commit()
            flash("Medicine returned successfully. Stock updated & Return Bill generated.", "success")
            return redirect(f"/return-invoice/{ret.id}")

        # ---------------- INVOICE RETURN ----------------
        invoice_no = request.form.get("invoice_no", "").strip()
        if not invoice_no:
            flash("Please enter invoice number", "danger")
            return redirect("/return-medicine")

        invoice = Invoice.query.filter_by(invoice_no=invoice_no).first()
        if not invoice:
            flash("Invoice not found", "danger")
            return redirect(f"/return-medicine?invoice_no={invoice_no}")

        items = InvoiceItem.query.filter_by(invoice_id=invoice.id).all()
        returned_map = dict(
            db.session.query(ReturnItem.invoice_item_id, db.func.sum(ReturnItem.qty))
            .join(Return, Return.id == ReturnItem.return_id)
            .filter(
                Return.invoice_id == invoice.id,
                Return.is_cancelled == False
            )
            .group_by(ReturnItem.invoice_item_id)
            .all()
        )

        return_requests = []
        for it in items:
            already_returned = int(returned_map.get(it.id) or 0)
            remaining = it.qty - already_returned
            req_qty = int(request.form.get(f"return_qty_{it.id}", 0) or 0)
            reason = request.form.get(f"reason_{it.id}", "").strip()
            if req_qty < 0 or req_qty > remaining:
                flash(f"Invalid return qty for {it.name}", "danger")
                return redirect(f"/return-medicine?invoice_no={invoice_no}")
            if req_qty > 0:
                return_requests.append((it, req_qty, remaining, reason))

        if not return_requests:
            flash("Please enter return quantity", "danger")
            return redirect(f"/return-medicine?invoice_no={invoice_no}")

        gst_rate = 0
        if invoice.subtotal and (invoice.cgst or invoice.sgst):
            gst_rate = ((invoice.cgst + invoice.sgst) / invoice.subtotal) * 100

        ret = Return(
            invoice_id=invoice.id,
            invoice_no=invoice.invoice_no,
            customer=invoice.customer,
            mobile=invoice.mobile,
            payment_mode=request.form.get("payment_mode", "CASH"),
            created_by=session.get("username")
        )
        db.session.add(ret)
        db.session.flush()
        ret.return_no = f"RB-{ret.id:06d}"

        subtotal_return = 0
        total_cgst = 0
        total_sgst = 0

        for it, req_qty, _, reason in return_requests:
            med = Medicine.query.filter_by(name=it.name, batch=it.batch).first()
            if not med:
                med = Medicine(
                    name=it.name,
                    batch=it.batch,
                    expiry=it.expiry,
                    mrp=it.price or 0,
                    qty=0,
                    discount_percent=int(it.discount_percent or 0)
                )
                db.session.add(med)
                db.session.flush()

            old_stock = med.qty
            med.qty += req_qty

            history = StockHistory(
                medicine_id=med.id,
                medicine_name=med.name,
                batch=med.batch,
                action="RETURN",
                stock_before=old_stock,
                qty_change=req_qty,
                stock_after=med.qty,
                user=session.get("username"),
                remark=f"Return Bill {ret.return_no} (Inv {invoice.invoice_no})"
            )
            db.session.add(history)

            amount = req_qty * (it.price or 0)
            discp = it.discount_percent or 0
            disc_amt = amount * discp / 100
            net_amt = amount - disc_amt

            tax_amt = net_amt * gst_rate / 100
            cgst_amt = tax_amt / 2
            sgst_amt = tax_amt / 2

            subtotal_return += net_amt
            total_cgst += cgst_amt
            total_sgst += sgst_amt

            fallback_rate = it.cost_price or it.price or 0
            cost_total = fifo_return(it.id, req_qty, fallback_rate)
            cost_price = round(cost_total / req_qty, 4) if req_qty else 0

            db.session.add(ReturnItem(
                return_id=ret.id,
                invoice_item_id=it.id,
                medicine_id=med.id,
                medicine_name=it.name,
                batch=it.batch,
                expiry=it.expiry,
                qty=req_qty,
                price=it.price,
                amount=amount,
                purchase_rate=0,
                selling_rate=it.price or 0,
                gst_percent=gst_rate,
                reason=reason,
                discount_percent=discp,
                discount_amount=disc_amt,
                net_amount=net_amt,
                cost_price=cost_price,
                cost_amount=cost_total
            ))

        ret.cgst = round(total_cgst, 2)
        ret.sgst = round(total_sgst, 2)
        ret.total_refund = round(subtotal_return + total_cgst + total_sgst, 2)
        db.session.commit()
        flash("Medicine returned successfully. Stock updated & Return Bill generated.", "success")
        return redirect(f"/return-invoice/{ret.id}")

    # ---------------- SEARCH (GET) ----------------
    if invoice_no:
        invoice = Invoice.query.filter_by(invoice_no=invoice_no).first()
        if invoice:
            items = InvoiceItem.query.filter_by(invoice_id=invoice.id).all()
            returned_map = dict(
                db.session.query(ReturnItem.invoice_item_id, db.func.sum(ReturnItem.qty))
                .join(Return, Return.id == ReturnItem.return_id)
                .filter(
                    Return.invoice_id == invoice.id,
                    Return.is_cancelled == False
                )
                .group_by(ReturnItem.invoice_item_id)
                .all()
            )
            for it in items:
                already_returned = int(returned_map.get(it.id) or 0)
                remaining = it.qty - already_returned
                items_payload.append({
                    "item": it,
                    "returned": already_returned,
                    "remaining": remaining
                })
        else:
            flash("Invoice not found", "danger")

    if (search_customer or search_from or search_to) and not invoice_no:
        q = Invoice.query
        if search_customer:
            q = q.filter(Invoice.customer.ilike(f"%{search_customer}%"))
        if search_from:
            from_dt = datetime.strptime(search_from, "%Y-%m-%d")
            q = q.filter(Invoice.created_at >= from_dt)
        if search_to:
            to_dt = datetime.strptime(search_to, "%Y-%m-%d") + timedelta(days=1)
            q = q.filter(Invoice.created_at < to_dt)
        search_results = q.order_by(Invoice.id.desc()).limit(50).all()

    return render_template(
        "return_medicine.html",
        invoice=invoice,
        items=items_payload,
        invoice_no=invoice_no,
        medicines=medicines,
        search_results=search_results,
        search_customer=search_customer,
        search_from=search_from,
        search_to=search_to
    )


# ---------------- RETURN INVOICE ----------------
@app.route("/return-invoice/<int:id>")
@login_required
def return_invoice(id):
    ret = Return.query.get_or_404(id)
    items = ReturnItem.query.filter_by(return_id=id).all()
    return_no = ret.return_no or f"RB-{ret.id:06d}"
    subtotal = sum((i.net_amount or 0) for i in items)
    total_refund = ret.total_refund or round(subtotal + (ret.cgst or 0) + (ret.sgst or 0), 2)

    return render_template(
        "return_invoice.html",
        ret=ret,
        items=items,
        return_no=return_no,
        subtotal=subtotal,
        total_refund=total_refund,
        date=ret.created_at.strftime("%d-%m-%Y")
    )

# ---------------- HOLD BILL ----------------
@app.route("/billing/hold", methods=["POST"])
@login_required
def hold_bill():
    items = build_hold_items_from_form(request.form)
    hold_bill_id = to_int_safe(request.form.get("hold_bill_id"), 0)

    customer = (request.form.get("customer") or "").strip()
    mobile = (request.form.get("mobile") or "").strip()
    doctor = (request.form.get("doctor") or "").strip()
    gender = (request.form.get("gender") or "").strip()
    sale_type = (request.form.get("sale_type") or "sale").strip().lower() or "sale"
    payment_mode = (request.form.get("payment_mode") or "CASH").strip().upper() or "CASH"

    if not customer:
        flash("Please enter patient name before holding bill", "danger")
        if hold_bill_id > 0:
            return redirect(url_for("billing", hold_bill_id=hold_bill_id))
        return redirect("/billing")

    payload = {
        "header": {
            "customer": customer,
            "mobile": mobile,
            "doctor": doctor,
            "gender": gender,
            "sale_type": sale_type,
            "payment_mode": payment_mode
        },
        "items": items,
        "totals": build_hold_totals_from_form(request.form, items)
    }

    hold_bill = HoldBill.query.get(hold_bill_id) if hold_bill_id > 0 else None
    if hold_bill:
        hold_bill.customer = customer
        hold_bill.mobile = mobile
        hold_bill.doctor = doctor
        hold_bill.gender = gender
        hold_bill.data = payload
        flash_msg = "Pending bill updated"
    else:
        hold_bill = HoldBill(
            customer=customer,
            mobile=mobile,
            doctor=doctor,
            gender=gender,
            data=payload
        )
        db.session.add(hold_bill)
        flash_msg = "Bill saved to Pending Bills"

    db.session.commit()

    flash(flash_msg, "info")
    return redirect("/pending-bills")

# ---------------- PENDING BILLS ----------------
@app.route("/pending-bills")
@login_required
def pending_bills():
    bills = HoldBill.query.order_by(HoldBill.id.desc()).all()
    return render_template("pending_bills.html", bills=bills)

@app.route("/restore-bill/<int:id>")
@login_required
def restore_bill(id):
    hb = HoldBill.query.get(id)
    if not hb:
        flash("Held bill not found.", "warning")
        return redirect("/pending-bills")
    return redirect(url_for("billing", hold_bill_id=hb.id))

@app.route("/delete-hold/<int:id>")
@login_required
def delete_hold(id):
    db.session.delete(HoldBill.query.get_or_404(id))
    db.session.commit()
    return redirect("/pending-bills")

# ---------------- CANCEL RETURN BILL ----------------
@app.route("/return-bill/delete/<int:id>")
@login_required
def delete_return_bill(id):
    ret = Return.query.get_or_404(id)
    if ret.is_cancelled:
        flash("Return bill already cancelled.", "info")
        return redirect("/return-bills")

    items = ReturnItem.query.filter_by(return_id=id).all()
    for it in items:
        med = None
        if it.medicine_id:
            med = Medicine.query.get(it.medicine_id)
        if not med:
            med = Medicine.query.filter_by(name=it.medicine_name, batch=it.batch).first()
        if not med:
            flash(f"Medicine not found for {it.medicine_name} ({it.batch}).", "danger")
            return redirect("/return-bills")
        if med.qty < it.qty:
            flash(f"Cannot cancel: stock for {med.name} ({med.batch}) is less than return qty.", "danger")
            return redirect("/return-bills")

    for it in items:
        med = Medicine.query.get(it.medicine_id) if it.medicine_id else Medicine.query.filter_by(name=it.medicine_name, batch=it.batch).first()
        if not med:
            continue
        if it.invoice_item_id:
            fallback_rate = it.cost_price or it.selling_rate or it.price or 0
            fifo_cancel_return(it.invoice_item_id, it.qty, fallback_rate)
        old_stock = med.qty
        med.qty -= it.qty
        history = StockHistory(
            medicine_id=med.id,
            medicine_name=med.name,
            batch=med.batch,
            action="RETURN_CANCEL",
            stock_before=old_stock,
            qty_change=-it.qty,
            stock_after=med.qty,
            user=session.get("username"),
            remark=f"Return Bill cancelled {ret.return_no or f'RB-{ret.id:06d}'}"
        )
        db.session.add(history)

    ret.is_cancelled = True
    ret.cancelled_by = session.get("username")
    ret.cancelled_at = datetime.now()
    db.session.commit()
    flash("Return bill cancelled. Stock reversed and record kept for audit.", "warning")
    return redirect("/return-bills")

# ---------------- RETURN BILLS LIST ----------------
@app.route("/return-bills")
@login_required
def return_bills():
    returns = Return.query.order_by(Return.id.desc()).all()
    return render_template("return_bills.html", returns=returns)

# ---------------- INVOICES LIST ----------------
@app.route("/invoices")
@login_required
def invoices():
    from_str = request.args.get("from", "").strip()
    to_str = request.args.get("to", "").strip()
    query = Invoice.query
    if from_str:
        try:
            from_dt = datetime.strptime(from_str, "%Y-%m-%d")
            query = query.filter(Invoice.created_at >= from_dt)
        except ValueError:
            pass
    if to_str:
        try:
            to_dt = datetime.strptime(to_str, "%Y-%m-%d") + timedelta(days=1)
            query = query.filter(Invoice.created_at < to_dt)
        except ValueError:
            pass
    return render_template(
        "invoices.html",
        invoices=query.order_by(Invoice.id.desc()).all()
    )

# ---------------- PART-3: VIEW / PRINT INVOICE ----------------
@app.route("/invoice/<int:id>")
@login_required
def view_invoice(id):
    inv = Invoice.query.get_or_404(id)
    items = InvoiceItem.query.filter_by(invoice_id=id).all()
    rounded_total = round(inv.subtotal or 0)

    return render_template(
        "invoice.html",
        inv=inv,
        cart=items,
        customer=inv.customer,
        mobile=inv.mobile,
        doctor=inv.doctor,
        gender=inv.gender,
        invoice_no=inv.invoice_no,
        subtotal=inv.subtotal,
        discount=inv.discount,
        cgst=inv.cgst,
        sgst=inv.sgst,
        total=inv.total,
        rounded_total=rounded_total,
        date=inv.created_at.strftime("%d-%m-%Y")
    )
@app.route("/invoice/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_invoice(id):
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (user.can_edit_invoice or user.can_invoice_action):
        flash("Access denied", "danger")
        return redirect("/invoices")
    flash("Invoice editing is disabled to protect batch-wise stock. Use Return Bill for changes.", "warning")
    return redirect("/invoices")
    invoice = Invoice.query.get_or_404(id)
    items = InvoiceItem.query.filter_by(invoice_id=id).all()
    medicines = Medicine.query.order_by(Medicine.name).all()

    if request.method == "POST":
        has_returns = Return.query.filter(Return.invoice_id == invoice.id, Return.is_cancelled == False).first()
        if has_returns:
            flash("Cannot edit invoice with returns. Cancel returns first.", "danger")
            return redirect("/invoices")

        # 1) Old stock wapas add + FIFO restore
        for it in items:
            med = Medicine.query.filter_by(name=it.name, batch=it.batch).first()
            if med:
                med.qty += it.qty
                history = StockHistory(
                    medicine_id=med.id,
                    medicine_name=med.name,
                    batch=med.batch,
                    action="RETURN",
                    stock_before=med.qty - it.qty,
                    qty_change=it.qty,
                    stock_after=med.qty,
                    user=session.get("username"),
                    remark="Invoice edited (stock returned)"
                )
                db.session.add(history)

            allocations = SalesAllocation.query.filter_by(invoice_item_id=it.id).all()
            for alloc in allocations:
                sold_qty = to_int(alloc.qty) - to_int(alloc.returned_qty)
                if sold_qty <= 0:
                    continue
                if alloc.purchase_item_id:
                    pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
                    if pi:
                        pi.remaining_qty = to_int(pi.remaining_qty) + sold_qty
            SalesAllocation.query.filter_by(invoice_item_id=it.id).delete()

        # 2) Old invoice items delete
        InvoiceItem.query.filter_by(invoice_id=id).delete()

        subtotal = 0
        discount_total = 0

        meds = request.form.getlist("medicine")
        qtys = request.form.getlist("qty")
        discs = request.form.getlist("discount")

        for mid, qty, disc in zip(meds, qtys, discs):
            if not mid:
                continue
            med = Medicine.query.get(int(mid))
            qty = int(qty or 0)
            disc = float(disc or 0)

            if qty > med.qty:
                flash(f"Not enough stock for {med.name}", "danger")
                return redirect(f"/invoice/edit/{invoice.id}")

            amount = qty * med.mrp
            disc_amt = amount * disc / 100
            net = amount - disc_amt

            subtotal += net
            discount_total += disc_amt

            old_stock = med.qty
            med.qty -= qty

            history = StockHistory(
                medicine_id=med.id,
                medicine_name=med.name,
                batch=med.batch,
                action="SALE",
                stock_before=old_stock,
                qty_change=-qty,
                stock_after=med.qty,
                user=session.get("username"),
                remark="Invoice edited (sale)"
            )
            db.session.add(history)

            allocations = fifo_consume(med, qty)
            cost_amount = sum(a["qty"] * a["cost_rate"] for a in allocations)
            cost_price = round(cost_amount / qty, 4) if qty else 0

            inv_item = InvoiceItem(
                invoice_id=invoice.id,
                name=med.name,
                qty=qty,
                price=med.mrp,
                batch=med.batch,
                expiry=med.expiry,
                discount_percent=disc,
                discount_amount=disc_amt,
                net_amount=net,
                cost_price=cost_price,
                cost_amount=cost_amount
            )
            db.session.add(inv_item)
            db.session.flush()
            for alloc in allocations:
                db.session.add(SalesAllocation(
                    invoice_item_id=inv_item.id,
                    purchase_item_id=alloc["purchase_item"].id if alloc["purchase_item"] else None,
                    qty=alloc["qty"],
                    cost_rate=alloc["cost_rate"],
                    returned_qty=0
                ))

        invoice.subtotal = subtotal
        invoice.discount = discount_total
        invoice.cgst = round(subtotal * 0.025, 2)
        invoice.sgst = round(subtotal * 0.025, 2)
        invoice.total = subtotal   # GST add nahi ho raha

        db.session.commit()
        flash("Invoice updated successfully", "success")
        return redirect(f"/invoice/{invoice.id}")


    return render_template(
        "edit_invoice.html",
        invoice=invoice,
        items=items,
        medicines=medicines
    )
@app.route("/delete-invoice/<int:id>")
@login_required
def delete_invoice(id):
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (user.can_delete_invoice or user.can_invoice_action):
        flash("Access denied", "danger")
        return redirect("/invoices")
    inv = Invoice.query.get_or_404(id)
    has_returns = Return.query.filter(Return.invoice_id == inv.id, Return.is_cancelled == False).first()
    if has_returns:
        flash("Cannot delete invoice with returns. Cancel returns first.", "danger")
        return redirect("/invoices")
    items = InvoiceItem.query.filter_by(invoice_id=id).all()
    for it in items:
        med = Medicine.query.filter_by(name=it.name, batch=it.batch).first()
        if med:
            old_stock = med.qty
            med.qty += it.qty
            history = StockHistory(
                medicine_id=med.id,
                medicine_name=med.name,
                batch=med.batch,
                action="RETURN",
                stock_before=old_stock,
                qty_change=it.qty,
                stock_after=med.qty,
                user=session.get("username"),
                remark="Invoice deleted (stock returned)"
            )
            db.session.add(history)

        allocations = SalesAllocation.query.filter_by(invoice_item_id=it.id).all()
        for alloc in allocations:
            sold_qty = to_int(alloc.qty) - to_int(alloc.returned_qty)
            if sold_qty <= 0:
                continue
            if alloc.purchase_item_id:
                pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
                if pi:
                    pi.remaining_qty = to_int(pi.remaining_qty) + sold_qty
        SalesAllocation.query.filter_by(invoice_item_id=it.id).delete()

    InvoiceItem.query.filter_by(invoice_id=id).delete()
    db.session.delete(inv)
    db.session.commit()
    flash("Invoice deleted and stock restored.", "info")
    return redirect("/invoices")
@app.route("/company")
@login_required
def company():
    return "<h2>Company Master – Coming Soon</h2>"

@app.route("/category")
@login_required
def category():
    return "<h2>Category Master – Coming Soon</h2>"

@app.route("/salt")
@login_required
def salt():
    return "<h2>Salt Master – Coming Soon</h2>"

@app.route("/vendor")
@login_required
def vendor():
    vendors = Vendor.query.order_by(Vendor.name).all()
    return render_template("vendor.html", vendors=vendors)

@app.route("/vendor-notes")
@login_required
@vendor_note_access_required()
def vendor_notes():
    vendors = Vendor.query.order_by(Vendor.name.asc()).all()
    medicines = Medicine.query.order_by(
        db.func.lower(Medicine.name).asc(),
        Medicine.batch.asc(),
        Medicine.id.asc()
    ).all()
    purchases = VendorPurchase.query.order_by(
        VendorPurchase.purchase_date.desc(),
        VendorPurchase.id.desc()
    ).limit(300).all()
    notes = VendorNote.query.order_by(
        VendorNote.note_date.desc(),
        VendorNote.id.desc()
    ).limit(200).all()
    vendor_map = {v.id: v.name for v in vendors}
    return render_template(
        "vendor_notes.html",
        vendors=vendors,
        medicines=medicines,
        purchases=purchases,
        notes=notes,
        vendor_map=vendor_map,
        today=date.today().isoformat()
    )

@app.route("/vendor/reports")
@login_required
def vendor_reports():
    vendors = Vendor.query.order_by(Vendor.name).all()
    purchase_summary = []
    for v in vendors:
        total = db.session.query(db.func.sum(VendorPurchase.total_amount)).filter_by(vendor_id=v.id).scalar() or 0
        count = db.session.query(db.func.count(VendorPurchase.id)).filter_by(vendor_id=v.id).scalar() or 0
        mrp_value = (
            db.session.query(
                db.func.coalesce(
                    db.func.sum(
                        (db.func.coalesce(VendorPurchaseItem.qty, 0) + db.func.coalesce(VendorPurchaseItem.free_qty, 0))
                        * db.func.coalesce(VendorPurchaseItem.mrp, 0)
                    ),
                    0
                )
            )
            .filter(
                VendorPurchaseItem.vendor_id == v.id,
                db.func.coalesce(VendorPurchaseItem.mrp, 0) > 0
            )
            .scalar()
            or 0
        )
        cost_value = (
            db.session.query(
                db.func.coalesce(
                    db.func.sum(db.func.coalesce(VendorPurchaseItem.total_value, 0)),
                    0
                )
            )
            .filter(
                VendorPurchaseItem.vendor_id == v.id,
                db.func.coalesce(VendorPurchaseItem.mrp, 0) > 0
            )
            .scalar()
            or 0
        )
        margin_amount = mrp_value - cost_value
        margin_percent = (margin_amount / mrp_value * 100) if mrp_value else 0
        purchase_summary.append({
            "vendor": v,
            "total": round(total, 2),
            "count": count,
            "outstanding": round(v.outstanding_balance or 0, 2),
            "margin_amount": round(margin_amount, 2),
            "margin_percent": round(margin_percent, 2)
        })
    return render_template("vendor_reports.html", purchase_summary=purchase_summary)

@app.route("/vendor/medicine-history")
@login_required
def vendor_medicine_history():
    query_name = (request.args.get("name") or "").strip()
    items_query = VendorPurchaseItem.query
    if query_name:
        items_query = items_query.filter(VendorPurchaseItem.medicine_name.ilike(f"%{query_name}%"))
    items = items_query.order_by(VendorPurchaseItem.created_at.desc()).limit(200).all()

    last_rates = []
    seen = set()
    for it in items:
        key = (it.vendor_id, it.medicine_name, it.batch)
        if key in seen:
            continue
        seen.add(key)
        vendor_obj = Vendor.query.get(it.vendor_id)
        last_rates.append({
            "vendor": vendor_obj.name if vendor_obj else "-",
            "medicine": it.medicine_name,
            "batch": it.batch,
            "expiry": it.expiry,
            "rate": it.purchase_rate,
            "date": it.created_at
        })

    return render_template("vendor_medicine_history.html", items=last_rates, query_name=query_name)

@app.route("/vendor/add", methods=["GET", "POST"])
@login_required
def add_vendor():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Vendor name is required", "danger")
            return redirect("/vendor/add")
        existing = Vendor.query.filter(db.func.lower(Vendor.name) == name.lower()).first()
        if existing:
            flash("Vendor name already exists", "danger")
            return redirect("/vendor/add")

        last_purchase_dt = parse_date(request.form.get("last_purchase_date"))

        attachment_ref = request.form.get("attachment_ref", "")
        file = request.files.get("attachment_file")
        if file and file.filename and allowed_file(file.filename):
            safe_name = secure_filename(file.filename)
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{ts}_{safe_name}"
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            attachment_ref = filename

        v = Vendor(
            name=name,
            mobile=request.form.get("mobile", ""),
            email=request.form.get("email", ""),
            gst_no=request.form.get("gst_no", ""),
            shop_name=request.form.get("shop_name", ""),
            area=request.form.get("area", ""),
            city=request.form.get("city", ""),
            state=request.form.get("state", ""),
            pincode=request.form.get("pincode", ""),
            address=request.form.get("address", ""),
            vendor_type=request.form.get("vendor_type", ""),
            credit_days=to_int(request.form.get("credit_days")),
            credit_limit=to_float(request.form.get("credit_limit")),
            bank_name=request.form.get("bank_name", ""),
            account_holder_name=request.form.get("account_holder_name", ""),
            account_no=request.form.get("account_no", ""),
            ifsc=request.form.get("ifsc", ""),
            upi=request.form.get("upi", ""),
            categories=request.form.get("categories", ""),
            salts=request.form.get("salts", ""),
            last_purchase_date=last_purchase_dt,
            total_purchases=to_float(request.form.get("total_purchases")),
            outstanding_balance=to_float(request.form.get("outstanding_balance")),
            payment_status=request.form.get("payment_status", ""),
            rate_history=request.form.get("rate_history", ""),
            default_payment_mode=request.form.get("default_payment_mode", ""),
            notes=request.form.get("notes", ""),
            attachment_ref=attachment_ref,
            is_active=True if request.form.get("is_active") else False
        )

        db.session.add(v)
        db.session.commit()
        flash("Vendor added successfully", "success")
        return redirect("/vendor")

    return render_template("vendor_form.html", vendor=None)

@app.route("/vendor/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_vendor(id):
    v = Vendor.query.get_or_404(id)

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Vendor name is required", "danger")
            return redirect(f"/vendor/edit/{id}")
        existing = Vendor.query.filter(db.func.lower(Vendor.name) == name.lower(), Vendor.id != v.id).first()
        if existing:
            flash("Vendor name already exists", "danger")
            return redirect(f"/vendor/edit/{id}")

        last_purchase_dt = parse_date(request.form.get("last_purchase_date"))

        attachment_ref = request.form.get("attachment_ref", "")
        file = request.files.get("attachment_file")
        if file and file.filename and allowed_file(file.filename):
            safe_name = secure_filename(file.filename)
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{ts}_{safe_name}"
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            attachment_ref = filename
        elif not attachment_ref:
            attachment_ref = v.attachment_ref or ""

        v.name = name
        v.mobile = request.form.get("mobile", "")
        v.email = request.form.get("email", "")
        v.gst_no = request.form.get("gst_no", "")
        v.shop_name = request.form.get("shop_name", "")
        v.area = request.form.get("area", "")
        v.city = request.form.get("city", "")
        v.state = request.form.get("state", "")
        v.pincode = request.form.get("pincode", "")
        v.address = request.form.get("address", "")
        v.vendor_type = request.form.get("vendor_type", "")
        v.credit_days = to_int(request.form.get("credit_days"))
        v.credit_limit = to_float(request.form.get("credit_limit"))
        v.bank_name = request.form.get("bank_name", "")
        v.account_holder_name = request.form.get("account_holder_name", "")
        v.account_no = request.form.get("account_no", "")
        v.ifsc = request.form.get("ifsc", "")
        v.upi = request.form.get("upi", "")
        v.categories = request.form.get("categories", "")
        v.salts = request.form.get("salts", "")
        v.last_purchase_date = last_purchase_dt
        v.total_purchases = to_float(request.form.get("total_purchases"))
        v.outstanding_balance = to_float(request.form.get("outstanding_balance"))
        v.payment_status = request.form.get("payment_status", "")
        v.rate_history = request.form.get("rate_history", "")
        v.default_payment_mode = request.form.get("default_payment_mode", "")
        v.notes = request.form.get("notes", "")
        v.attachment_ref = attachment_ref
        v.is_active = True if request.form.get("is_active") else False

        db.session.commit()
        flash("Vendor updated successfully", "success")
        return redirect("/vendor")

    medicines = Medicine.query.order_by(Medicine.name).all()
    purchase_items = VendorPurchaseItem.query.filter_by(vendor_id=v.id).order_by(VendorPurchaseItem.created_at.desc()).limit(50).all()
    today = date.today()
    for item in purchase_items:
        item.is_expired = False
        if item.expiry:
            try:
                exp_date = datetime.strptime(normalize_expiry(item.expiry), "%Y-%m-%d").date()
                if exp_date < today:
                    item.is_expired = True
            except ValueError:
                item.is_expired = False
    last_rates = []
    seen = set()
    for item in purchase_items:
        key = (item.medicine_name, item.batch)
        if key in seen:
            continue
        seen.add(key)
        last_rates.append(item)
    purchase_bills = VendorPurchase.query.filter_by(vendor_id=v.id).order_by(
        VendorPurchase.purchase_date.desc(),
        VendorPurchase.id.desc()
    ).all()

    return render_template(
        "vendor_form.html",
        vendor=v,
        medicines=medicines,
        purchase_items=purchase_items,
        last_rates=last_rates,
        purchase_bills=purchase_bills
    )

@app.route("/vendor/<int:id>/purchase", methods=["POST"])
@login_required
def add_vendor_purchase(id):
    vendor = Vendor.query.get_or_404(id)

    invoice_no = (request.form.get("invoice_no") or "").strip()
    if not invoice_no:
        flash("Bill/Invoice number is required", "danger")
        return redirect(f"/vendor/edit/{vendor.id}")
    duplicate = VendorPurchase.query.filter(
        VendorPurchase.vendor_id == vendor.id,
        db.func.lower(VendorPurchase.invoice_no) == invoice_no.lower()
    ).first()
    if duplicate:
        flash("This bill number already exists for this vendor", "danger")
        return redirect(f"/vendor/edit/{vendor.id}")

    purchase_date_val = request.form.get("purchase_date")
    purchase_date = datetime.strptime(purchase_date_val, "%Y-%m-%d") if purchase_date_val else datetime.now()
    payment_mode = request.form.get("payment_mode") or vendor.default_payment_mode or "CASH"
    payment_status = request.form.get("payment_status") or vendor.payment_status or "Unpaid"
    paid_amount = to_float(request.form.get("paid_amount"))

    names = request.form.getlist("medicine_name")
    compositions = request.form.getlist("composition")
    companies = request.form.getlist("company")
    distributors = request.form.getlist("distributor_name")
    pack_types = request.form.getlist("pack_type")
    pack_qtys = request.form.getlist("pack_qty")
    batches = request.form.getlist("batch")
    expiries = request.form.getlist("expiry")
    qtys = request.form.getlist("qty")
    free_qtys = request.form.getlist("free_qty")
    purchase_rates = request.form.getlist("purchase_rate")
    mrps = request.form.getlist("mrp")
    gst_percents = request.form.getlist("gst_percent")
    discount_percents = request.form.getlist("discount_percent")

    line_items = []
    subtotal = 0
    gst_total = 0
    discount_total = 0
    total_amount = 0

    for idx, name in enumerate(names):
        name = (name or "").strip()
        batch = (batches[idx] or "").strip() if idx < len(batches) else ""
        expiry_raw = (expiries[idx] or "").strip() if idx < len(expiries) else ""
        expiry = normalize_expiry(expiry_raw)
        qty = to_int(qtys[idx]) if idx < len(qtys) else 0
        free_qty = to_int(free_qtys[idx]) if idx < len(free_qtys) else 0
        purchase_rate = to_float(purchase_rates[idx]) if idx < len(purchase_rates) else 0
        mrp = to_float(mrps[idx]) if idx < len(mrps) else 0
        gst_percent = to_float(gst_percents[idx]) if idx < len(gst_percents) else 0
        discount_percent = to_float(discount_percents[idx]) if idx < len(discount_percents) else 0
        composition = (compositions[idx] or "").strip() if idx < len(compositions) else ""
        company = (companies[idx] or "").strip() if idx < len(companies) else ""
        distributor_name = (distributors[idx] or "").strip() if idx < len(distributors) else ""
        pack_type = (pack_types[idx] or "").strip() if idx < len(pack_types) else ""
        pack_qty_raw = (pack_qtys[idx] or "").strip() if idx < len(pack_qtys) else ""
        pack_qty = parse_pack_qty(pack_qty_raw)

        if not name:
            continue
        if not batch or not expiry:
            flash(f"Batch and expiry required for {name}", "danger")
            return redirect(f"/vendor/edit/{vendor.id}")
        if qty <= 0:
            continue
        if not pack_type:
            flash(f"Pack type is required for {name}", "danger")
            return redirect(f"/vendor/edit/{vendor.id}")
        if not pack_qty_raw or pack_qty is None or pack_qty < 1:
            flash(f"Pack quantity must be at least 1 for {name}", "danger")
            return redirect(f"/vendor/edit/{vendor.id}")

        base_amount = qty * purchase_rate
        discount_amt = base_amount * discount_percent / 100
        taxable = base_amount - discount_amt
        gst_amt = taxable * gst_percent / 100
        total_value = taxable + gst_amt

        subtotal += taxable
        gst_total += gst_amt
        discount_total += discount_amt
        total_amount += total_value

        line_items.append({
            "name": name,
            "composition": composition,
            "company": company,
            "distributor_name": distributor_name,
            "pack_type": pack_type,
            "pack_qty": pack_qty,
            "batch": batch,
            "expiry": expiry,
            "qty": qty,
            "free_qty": free_qty,
            "purchase_rate": purchase_rate,
            "mrp": mrp,
            "gst_percent": gst_percent,
            "discount_percent": discount_percent,
            "total_value": total_value
        })

    if not line_items:
        flash("Please add at least one medicine to purchase", "danger")
        return redirect(f"/vendor/edit/{vendor.id}")

    purchase = VendorPurchase(
        vendor_id=vendor.id,
        purchase_date=purchase_date,
        invoice_no=invoice_no,
        payment_mode=payment_mode,
        payment_status=payment_status,
        paid_amount=paid_amount,
        subtotal=subtotal,
        gst_total=gst_total,
        discount_total=discount_total,
        total_amount=total_amount,
        created_by=session.get("username")
    )
    db.session.add(purchase)
    db.session.flush()
    purchase.purchase_no = f"PB-{purchase.id:06d}"

    for item in line_items:
        med = find_medicine_by_name_batch(item["name"], item["batch"])
        if not med:
            med = Medicine(
                name=item["name"],
                composition=item["composition"],
                company=item["company"],
                pack_type=item["pack_type"],
                pack_qty=item["pack_qty"],
                batch=item["batch"],
                expiry=item["expiry"],
                mrp=item["mrp"] or 0,
                qty=0,
                discount_percent=int(item["discount_percent"] or 0)
            )
            db.session.add(med)
            db.session.flush()
        else:
            if item["expiry"]:
                med.expiry = item["expiry"]
            if item["mrp"]:
                med.mrp = item["mrp"]
            if item["composition"]:
                med.composition = item["composition"]
            if item["company"]:
                med.company = item["company"]
            if item["pack_type"]:
                med.pack_type = item["pack_type"]
            if item.get("pack_qty") is not None:
                med.pack_qty = item["pack_qty"]

        old_stock = med.qty
        med.qty += item["qty"] + item["free_qty"]

        history = StockHistory(
            medicine_id=med.id,
            medicine_name=med.name,
            batch=med.batch,
            action="PURCHASE",
            stock_before=old_stock,
            qty_change=item["qty"] + item["free_qty"],
            stock_after=med.qty,
            user=session.get("username"),
            remark=f"Purchase {purchase.purchase_no} from {vendor.name}"
        )
        db.session.add(history)

        db.session.add(VendorPurchaseItem(
            purchase_id=purchase.id,
            vendor_id=vendor.id,
            medicine_id=med.id,
            medicine_name=item["name"],
            composition=item["composition"],
            company=item["company"],
            distributor_name=item["distributor_name"],
            pack_type=item["pack_type"],
            pack_qty=item["pack_qty"],
            batch=item["batch"],
            expiry=item["expiry"],
            qty=item["qty"],
            free_qty=item["free_qty"],
            remaining_qty=item["qty"] + item["free_qty"],
            purchase_rate=item["purchase_rate"],
            mrp=item["mrp"],
            gst_percent=item["gst_percent"],
            discount_percent=item["discount_percent"],
            total_value=item["total_value"]
        ))

    vendor.last_purchase_date = purchase_date.date() if purchase_date else vendor.last_purchase_date
    vendor.total_purchases = (vendor.total_purchases or 0) + total_amount
    vendor.payment_status = payment_status
    if payment_status.lower() in ("unpaid", "partial"):
        balance_add = total_amount - paid_amount if payment_status.lower() == "partial" else total_amount
        if balance_add < 0:
            balance_add = 0
        vendor.outstanding_balance = (vendor.outstanding_balance or 0) + balance_add

    db.session.commit()
    flash("Purchase saved. Stock updated.", "success")
    return redirect(f"/vendor/edit/{vendor.id}")

@app.route("/vendor/purchase/<int:purchase_id>")
@login_required
def view_vendor_purchase(purchase_id):
    purchase = VendorPurchase.query.get_or_404(purchase_id)
    vendor = Vendor.query.get(purchase.vendor_id)
    items = VendorPurchaseItem.query.filter_by(purchase_id=purchase.id).order_by(VendorPurchaseItem.id.asc()).all()
    mode = (request.args.get("mode") or "view").lower()
    return render_template(
        "vendor_purchase_view.html",
        vendor=vendor,
        purchase=purchase,
        items=items,
        mode=mode
    )

@app.route("/vendor/purchase/delete/<int:purchase_id>")
@login_required
def delete_vendor_purchase(purchase_id):
    purchase = VendorPurchase.query.get_or_404(purchase_id)
    vendor = Vendor.query.get(purchase.vendor_id)
    items = VendorPurchaseItem.query.filter_by(purchase_id=purchase.id).all()
    bill_no = purchase.invoice_no if purchase.invoice_no else (purchase.purchase_no or f"PB-{purchase.id:06d}")

    linked_note = VendorNote.query.filter_by(reference_purchase_id=purchase.id).first()
    if linked_note:
        flash("Cannot delete this bill because vendor note entries are linked to it.", "danger")
        return redirect(f"/vendor/edit/{purchase.vendor_id}")

    item_ids = [it.id for it in items]
    if item_ids:
        linked_alloc = SalesAllocation.query.filter(SalesAllocation.purchase_item_id.in_(item_ids)).first()
        if linked_alloc:
            flash("Cannot delete this bill because invoice/return transactions are linked.", "danger")
            return redirect(f"/vendor/edit/{purchase.vendor_id}")

    for it in items:
        total_qty = to_int(it.qty) + to_int(it.free_qty)
        if total_qty <= 0:
            continue
        med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
        if not med:
            med = find_medicine_by_name_batch(it.medicine_name, it.batch)
        if not med:
            flash(f"Cannot delete bill. Medicine missing: {it.medicine_name} ({it.batch})", "danger")
            return redirect(f"/vendor/edit/{purchase.vendor_id}")
        if to_int(med.qty) < total_qty:
            flash(
                f"Cannot delete bill. Stock already used for {med.name} ({med.batch}). "
                "Return/cancel related sales first.",
                "danger"
            )
            return redirect(f"/vendor/edit/{purchase.vendor_id}")

    purchase_total = to_float(purchase.total_amount)
    status = (purchase.payment_status or "").strip().lower()
    raw_paid_amount = getattr(purchase, "paid_amount", None)
    paid_amount = to_float(raw_paid_amount)
    if paid_amount < 0:
        paid_amount = 0
    outstanding_reduction = purchase_total
    legacy_partial_unknown = False
    if status == "partial":
        if raw_paid_amount is None:
            legacy_partial_unknown = True
            outstanding_reduction = 0
        else:
            outstanding_reduction = purchase_total - paid_amount
    if outstanding_reduction < 0:
        outstanding_reduction = 0

    purchase_no = purchase.purchase_no or f"PB-{purchase.id:06d}"

    for it in items:
        total_qty = to_int(it.qty) + to_int(it.free_qty)
        if total_qty <= 0:
            continue
        med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
        if not med:
            med = find_medicine_by_name_batch(it.medicine_name, it.batch)
        if not med:
            continue
        old_stock = to_int(med.qty)
        med.qty = old_stock - total_qty
        db.session.add(StockHistory(
            medicine_id=med.id,
            medicine_name=med.name,
            batch=med.batch,
            action="PURCHASE_DELETE",
            stock_before=old_stock,
            qty_change=-total_qty,
            stock_after=med.qty,
            user=session.get("username"),
            remark=f"Purchase bill deleted {bill_no}",
            ref_table="vendor_purchase",
            ref_id=purchase.id
        ))

    if vendor:
        vendor.total_purchases = max((vendor.total_purchases or 0) - purchase_total, 0)
        if status == "unpaid":
            vendor.outstanding_balance = max((vendor.outstanding_balance or 0) - outstanding_reduction, 0)
        elif status == "partial" and not legacy_partial_unknown:
            vendor.outstanding_balance = max((vendor.outstanding_balance or 0) - outstanding_reduction, 0)
        latest_purchase = VendorPurchase.query.filter(
            VendorPurchase.vendor_id == vendor.id,
            VendorPurchase.id != purchase.id
        ).order_by(VendorPurchase.purchase_date.desc(), VendorPurchase.id.desc()).first()
        vendor.last_purchase_date = latest_purchase.purchase_date.date() if latest_purchase and latest_purchase.purchase_date else None
        if (vendor.outstanding_balance or 0) <= 0:
            vendor.payment_status = "Paid"
        elif latest_purchase and latest_purchase.payment_status:
            vendor.payment_status = latest_purchase.payment_status

    StockHistory.query.filter(
        StockHistory.action == "PURCHASE",
        StockHistory.remark.like(f"Purchase {purchase_no}%")
    ).delete(synchronize_session=False)
    if item_ids:
        StockHistory.query.filter(
            StockHistory.action == "PURCHASE_EDIT",
            StockHistory.remark.in_([f"Purchase item {item_id} edited" for item_id in item_ids])
        ).delete(synchronize_session=False)

    VendorLedgerEntry.query.filter_by(ref_table="vendor_purchase", ref_id=purchase.id).delete(synchronize_session=False)
    VendorPurchaseItem.query.filter_by(purchase_id=purchase.id).delete(synchronize_session=False)
    db.session.delete(purchase)

    db.session.commit()
    if legacy_partial_unknown:
        flash(
            f"Bill {bill_no} deleted. Outstanding balance for old partial bill was not auto-adjusted (paid amount missing).",
            "warning"
        )
    flash(f"Bill {bill_no} deleted. Linked purchase entries and stock updates removed.", "warning")
    return redirect(f"/vendor/edit/{purchase.vendor_id}")

@app.route("/vendor/purchase-item/edit/<int:item_id>", methods=["GET", "POST"])
@login_required
def edit_vendor_purchase_item(item_id):
    item = VendorPurchaseItem.query.get_or_404(item_id)
    purchase = VendorPurchase.query.get(item.purchase_id)
    vendor = Vendor.query.get(item.vendor_id)

    med = Medicine.query.get(item.medicine_id) if item.medicine_id else None
    if not med:
        med = find_medicine_by_name_batch(item.medicine_name, item.batch)

    old_total_qty = to_int(item.qty) + to_int(item.free_qty)
    sold_qty = old_total_qty - to_int(item.remaining_qty)
    if sold_qty < 0:
        sold_qty = 0

    if request.method == "POST":
        name = (request.form.get("medicine_name") or "").strip()
        composition = (request.form.get("composition") or "").strip()
        company = (request.form.get("company") or "").strip()
        distributor_name = (request.form.get("distributor_name") or "").strip()
        pack_type = (request.form.get("pack_type") or "").strip()
        pack_qty_raw = (request.form.get("pack_qty") or "").strip()
        pack_qty = parse_pack_qty(pack_qty_raw)
        batch = (request.form.get("batch") or "").strip()
        expiry_raw = (request.form.get("expiry") or "").strip()
        expiry = normalize_expiry(expiry_raw)
        qty = to_int(request.form.get("qty"))
        free_qty = to_int(request.form.get("free_qty"))
        purchase_rate = to_float(request.form.get("purchase_rate"))
        mrp = to_float(request.form.get("mrp"))
        gst_percent = to_float(request.form.get("gst_percent"))
        discount_percent = to_float(request.form.get("discount_percent"))

        if not name:
            flash("Medicine name is required", "danger")
            return redirect(request.url)
        if not batch or not expiry:
            flash("Batch and expiry are required", "danger")
            return redirect(request.url)
        if qty <= 0:
            flash("Quantity must be greater than 0", "danger")
            return redirect(request.url)
        if pack_qty_raw and (pack_qty is None or pack_qty < 1):
            flash("Pack quantity must be at least 1", "danger")
            return redirect(request.url)

        new_total_qty = qty + free_qty
        if new_total_qty < sold_qty:
            flash(f"Cannot reduce below sold quantity ({sold_qty})", "danger")
            return redirect(request.url)

        old_base = to_float(item.qty) * to_float(item.purchase_rate)
        old_discount = old_base * to_float(item.discount_percent) / 100
        old_taxable = old_base - old_discount
        old_gst = old_taxable * to_float(item.gst_percent) / 100
        old_total = old_taxable + old_gst

        new_base = qty * purchase_rate
        new_discount = new_base * discount_percent / 100
        new_taxable = new_base - new_discount
        new_gst = new_taxable * gst_percent / 100
        new_total = new_taxable + new_gst

        diff_total_qty = new_total_qty - old_total_qty
        old_med_qty = med.qty if med else None

        if med:
            if med.qty + diff_total_qty < 0:
                flash("Not enough stock to reduce this purchase quantity", "danger")
                return redirect(request.url)
            med.qty = med.qty + diff_total_qty
            med.name = name
            med.batch = batch
            med.expiry = expiry
            med.mrp = mrp
            med.composition = composition
            med.company = company
            med.pack_type = pack_type
            med.discount_percent = int(discount_percent or 0)
            if pack_qty_raw:
                med.pack_qty = pack_qty

        item.medicine_name = name
        item.composition = composition
        item.company = company
        item.distributor_name = distributor_name
        item.pack_type = pack_type
        if pack_qty_raw:
            item.pack_qty = pack_qty
        item.batch = batch
        item.expiry = expiry
        item.qty = qty
        item.free_qty = free_qty
        item.remaining_qty = new_total_qty - sold_qty
        item.purchase_rate = purchase_rate
        item.mrp = mrp
        item.gst_percent = gst_percent
        item.discount_percent = discount_percent
        item.total_value = new_total

        if purchase:
            purchase.subtotal = max((purchase.subtotal or 0) - old_taxable + new_taxable, 0)
            purchase.gst_total = max((purchase.gst_total or 0) - old_gst + new_gst, 0)
            purchase.discount_total = max((purchase.discount_total or 0) - old_discount + new_discount, 0)
            purchase.total_amount = max((purchase.total_amount or 0) - old_total + new_total, 0)

        if vendor:
            delta_total = new_total - old_total
            vendor.total_purchases = (vendor.total_purchases or 0) + delta_total
            if purchase and (purchase.payment_status or "").lower() in ("unpaid", "partial"):
                vendor.outstanding_balance = (vendor.outstanding_balance or 0) + delta_total

        if diff_total_qty != 0 and med:
            history = StockHistory(
                medicine_id=med.id,
                medicine_name=name,
                batch=batch,
                action="PURCHASE_EDIT",
                stock_before=old_med_qty,
                qty_change=diff_total_qty,
                stock_after=med.qty,
                user=session.get("username"),
                remark=f"Purchase item {item.id} edited"
            )
            db.session.add(history)

        db.session.commit()
        flash("Purchase item updated successfully", "success")
        return redirect(f"/vendor/edit/{item.vendor_id}")

    return render_template(
        "edit_purchase_item.html",
        item=item,
        vendor=vendor,
        purchase=purchase,
        sold_qty=sold_qty
    )

# ---------------- VENDOR NOTES (DEBIT/CREDIT) ----------------
@app.route("/api/vendor-notes", methods=["POST"])
@login_required
@vendor_note_access_required(api=True)
def api_create_vendor_note():
    data = request.get_json(silent=True) or {}
    note_type = (data.get("note_type") or "").strip().upper()
    if note_type not in ("DEBIT", "CREDIT"):
        return jsonify({"error": "Invalid note_type"}), 400

    vendor_id = data.get("vendor_id")
    vendor = Vendor.query.get(vendor_id) if vendor_id else None
    if not vendor:
        return jsonify({"error": "Invalid vendor_id"}), 400

    note_date = parse_date(data.get("note_date"))
    if not note_date:
        return jsonify({"error": "note_date is required (YYYY-MM-DD)"}), 400

    reference_purchase_id = data.get("reference_purchase_id")
    if reference_purchase_id:
        purchase = VendorPurchase.query.get(reference_purchase_id)
        if not purchase:
            return jsonify({"error": "Invalid reference_purchase_id"}), 400
        if purchase.vendor_id != vendor.id:
            return jsonify({"error": "reference_purchase_id does not belong to vendor_id"}), 400

    note = VendorNote(
        note_no=generate_vendor_note_no(note_type),
        note_type=note_type,
        vendor_id=vendor.id,
        reference_purchase_id=reference_purchase_id,
        supplier_bill_no=(data.get("supplier_bill_no") or "").strip() or None,
        note_date=note_date,
        status="DRAFT",
        reason_code=(data.get("reason_code") or "").strip() or None,
        reason_text=(data.get("reason_text") or "").strip() or None,
        remarks=(data.get("remarks") or "").strip() or None,
        created_by=session.get("username"),
        created_at=datetime.utcnow()
    )

    mode = (data.get("mode") or "").strip().upper()
    if note_type == "CREDIT" and mode == "AMOUNT_ONLY":
        amount = data.get("grand_total") if "grand_total" in data else data.get("amount")
        amount_dec = quantize_decimal(amount or 0, "0.0001")
        note.subtotal = amount_dec
        note.gst_total = quantize_decimal(0, "0.0001")
        note.round_off = quantize_decimal(data.get("round_off") or 0, "0.0001")
        note.grand_total = quantize_decimal(note.subtotal + note.round_off, "0.0001")

    db.session.add(note)
    db.session.commit()
    return jsonify(vendor_note_to_dict(note)), 201


@app.route("/api/vendor-notes/<int:note_id>/items", methods=["PUT"])
@login_required
@vendor_note_access_required(api=True)
def api_update_vendor_note_items(note_id):
    note = VendorNote.query.get_or_404(note_id)
    if note.status != "DRAFT":
        return jsonify({"error": "Only DRAFT notes can be edited"}), 400

    data = request.get_json(silent=True) or {}
    items = data.get("items") or []

    if note.note_type == "DEBIT" and not items:
        return jsonify({"error": "DEBIT note requires items"}), 400

    if items:
        for idx, raw in enumerate(items):
            med_id = raw.get("medicine_id")
            if not med_id:
                return jsonify({"error": f"medicine_id missing at item {idx+1}"}), 400
            if note.note_type == "DEBIT" and not (raw.get("batch_no") or "").strip():
                return jsonify({"error": f"batch_no required at item {idx+1}"}), 400
            qty = int(raw.get("qty") or 0)
            if qty <= 0:
                return jsonify({"error": f"qty must be > 0 at item {idx+1}"}), 400

        subtotal, gst_total, prepared = compute_vendor_note_totals(items)
        with db.session.begin():
            VendorNoteItem.query.filter_by(note_id=note.id).delete()
            for idx, raw in enumerate(items):
                prepared_item = prepared[idx]
                item = VendorNoteItem(
                    note_id=note.id,
                    medicine_id=raw.get("medicine_id"),
                    batch_no=(raw.get("batch_no") or "").strip() or None,
                    expiry=(raw.get("expiry") or "").strip() or None,
                    qty=prepared_item["qty"],
                    free_qty=prepared_item["free_qty"],
                    purchase_rate=prepared_item["purchase_rate"],
                    mrp=quantize_decimal(raw.get("mrp") or 0, "0.0001"),
                    gst_percent=prepared_item["gst_percent"],
                    disc_percent=prepared_item["disc_percent"],
                    line_total=prepared_item["line_total"],
                    hsn=(raw.get("hsn") or "").strip() or None
                )
                db.session.add(item)

            note.subtotal = subtotal
            note.gst_total = gst_total
            if "round_off" in data:
                note.round_off = quantize_decimal(data.get("round_off") or 0, "0.0001")
            note.grand_total = quantize_decimal(note.subtotal + note.gst_total + to_decimal(note.round_off), "0.0001")
            db.session.add(note)
    else:
        # Amount-only credit note
        amount = data.get("grand_total") if "grand_total" in data else data.get("amount")
        amount_dec = quantize_decimal(amount or 0, "0.0001")
        with db.session.begin():
            VendorNoteItem.query.filter_by(note_id=note.id).delete()
            note.subtotal = amount_dec
            note.gst_total = quantize_decimal(0, "0.0001")
            if "round_off" in data:
                note.round_off = quantize_decimal(data.get("round_off") or 0, "0.0001")
            note.grand_total = quantize_decimal(note.subtotal + to_decimal(note.round_off), "0.0001")
            db.session.add(note)

    db.session.commit()
    return jsonify(vendor_note_to_dict(note, include_items=True)), 200


@app.route("/api/vendor-notes/<int:note_id>/post", methods=["POST"])
@login_required
@vendor_note_access_required(api=True)
def api_post_vendor_note(note_id):
    note = VendorNote.query.get_or_404(note_id)
    if note.status != "DRAFT":
        return jsonify({"error": "Only DRAFT notes can be posted"}), 400
    if note.reference_purchase_id:
        purchase = VendorPurchase.query.get(note.reference_purchase_id)
        if not purchase:
            return jsonify({"error": "Invalid reference_purchase_id"}), 400
        if purchase.vendor_id != note.vendor_id:
            return jsonify({"error": "reference_purchase_id does not belong to note vendor"}), 400

    items = VendorNoteItem.query.filter_by(note_id=note.id).all()

    if note.note_type == "DEBIT":
        if not items:
            return jsonify({"error": "DEBIT note requires items"}), 400
        adjustments = []
        allocations = []
        for it in items:
            if not it.batch_no:
                return jsonify({"error": "batch_no required for DEBIT note"}), 400
            total_qty = to_int(it.qty) + to_int(it.free_qty)
            if total_qty <= 0:
                return jsonify({"error": "qty must be > 0"}), 400
            med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
            if not med:
                return jsonify({"error": "Invalid medicine_id in items"}), 400
            if to_int(med.qty) < total_qty:
                return jsonify({"error": f"Insufficient stock for {med.name} ({med.batch})"}), 400

            purchase_items = get_purchase_items_for_return(
                med,
                batch_no=it.batch_no,
                reference_purchase_id=note.reference_purchase_id
            )
            available = sum(to_int(p.remaining_qty) for p in purchase_items)
            if available < total_qty:
                return jsonify({"error": f"Return qty exceeds available purchase stock for {med.name} ({med.batch})"}), 400

            remaining = total_qty
            for pi in purchase_items:
                if remaining <= 0:
                    break
                avail = to_int(pi.remaining_qty)
                if avail <= 0:
                    continue
                take = avail if avail <= remaining else remaining
                allocations.append((it, pi, take))
                remaining -= take
            adjustments.append((med, total_qty))

        with db.session.begin():
            for med, total_qty in adjustments:
                old_qty = med.qty
                med.qty = to_int(med.qty) - total_qty
                history = StockHistory(
                    medicine_id=med.id,
                    medicine_name=med.name,
                    batch=med.batch,
                    action="V_DEBIT_NOTE",
                    stock_before=old_qty,
                    qty_change=-total_qty,
                    stock_after=med.qty,
                    user=session.get("username"),
                    remark=f"Vendor debit note {note.note_no}",
                    ref_table="vendor_notes",
                    ref_id=note.id
                )
                db.session.add(history)

            for it, pi, take in allocations:
                pi.remaining_qty = to_int(pi.remaining_qty) - take
                db.session.add(VendorNoteAllocation(
                    note_id=note.id,
                    note_item_id=it.id,
                    purchase_item_id=pi.id,
                    qty=take
                ))

            db.session.add(VendorLedgerEntry(
                vendor_id=note.vendor_id,
                txn_date=datetime.utcnow(),
                txn_type="DEBIT_NOTE",
                ref_table="vendor_notes",
                ref_id=note.id,
                debit=quantize_decimal(note.grand_total or 0, "0.0001"),
                credit=quantize_decimal(0, "0.0001"),
                notes=f"Vendor debit note {note.note_no}"
            ))
            adjust_vendor_outstanding(note.vendor_id, -to_decimal(note.grand_total or 0))

            note.status = "POSTED"
            note.posted_at = datetime.utcnow()
            db.session.add(note)

    elif note.note_type == "CREDIT":
        if to_decimal(note.grand_total) <= 0:
            return jsonify({"error": "grand_total must be > 0"}), 400
        with db.session.begin():
            db.session.add(VendorLedgerEntry(
                vendor_id=note.vendor_id,
                txn_date=datetime.utcnow(),
                txn_type="CREDIT_NOTE",
                ref_table="vendor_notes",
                ref_id=note.id,
                debit=quantize_decimal(0, "0.0001"),
                credit=quantize_decimal(note.grand_total or 0, "0.0001"),
                notes=f"Vendor credit note {note.note_no}"
            ))
            adjust_vendor_outstanding(note.vendor_id, to_decimal(note.grand_total or 0))

            note.status = "POSTED"
            note.posted_at = datetime.utcnow()
            db.session.add(note)

    db.session.commit()
    return jsonify(vendor_note_to_dict(note, include_items=True, include_ledger=True)), 200


@app.route("/api/vendor-notes/<int:note_id>/cancel", methods=["POST"])
@login_required
@vendor_note_access_required(api=True)
def api_cancel_vendor_note(note_id):
    note = VendorNote.query.get_or_404(note_id)
    if note.status != "POSTED":
        return jsonify({"error": "Only POSTED notes can be cancelled"}), 400

    data = request.get_json(silent=True) or {}
    cancel_reason = (data.get("cancel_reason") or "").strip() or None

    items = VendorNoteItem.query.filter_by(note_id=note.id).all()
    allocations = VendorNoteAllocation.query.filter_by(note_id=note.id).all()

    with db.session.begin():
        reversal_debit = quantize_decimal(0, "0.0001")
        reversal_credit = quantize_decimal(0, "0.0001")
        if note.note_type == "DEBIT":
            for it in items:
                total_qty = to_int(it.qty) + to_int(it.free_qty)
                if total_qty <= 0:
                    continue
                med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
                if not med:
                    continue
                old_qty = med.qty
                med.qty = to_int(med.qty) + total_qty
                history = StockHistory(
                    medicine_id=med.id,
                    medicine_name=med.name,
                    batch=med.batch,
                    action="V_NOTE_REV",
                    stock_before=old_qty,
                    qty_change=total_qty,
                    stock_after=med.qty,
                    user=session.get("username"),
                    remark=f"Debit note cancel {note.note_no}",
                    ref_table="vendor_notes",
                    ref_id=note.id
                )
                db.session.add(history)

            if allocations:
                for alloc in allocations:
                    pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
                    if not pi:
                        continue
                    pi.remaining_qty = to_int(pi.remaining_qty) + to_int(alloc.qty)
            else:
                for it in items:
                    total_qty = to_int(it.qty) + to_int(it.free_qty)
                    if total_qty <= 0:
                        continue
                    med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
                    if not med:
                        continue
                    purchase_items = get_purchase_items_for_return(
                        med,
                        batch_no=it.batch_no,
                        reference_purchase_id=note.reference_purchase_id
                    )
                    remaining = total_qty
                    for pi in purchase_items:
                        if remaining <= 0:
                            break
                        take = remaining
                        pi.remaining_qty = to_int(pi.remaining_qty) + take
                        remaining -= take
            reversal_credit = quantize_decimal(note.grand_total or 0, "0.0001")
            adjust_vendor_outstanding(note.vendor_id, to_decimal(note.grand_total or 0))
        elif note.note_type == "CREDIT":
            reversal_debit = quantize_decimal(note.grand_total or 0, "0.0001")
            adjust_vendor_outstanding(note.vendor_id, -to_decimal(note.grand_total or 0))

        db.session.add(VendorLedgerEntry(
            vendor_id=note.vendor_id,
            txn_date=datetime.utcnow(),
            txn_type="REVERSAL",
            ref_table="vendor_notes",
            ref_id=note.id,
            debit=reversal_debit,
            credit=reversal_credit,
            notes=f"Reversal of {note.note_no}"
        ))

        note.status = "CANCELLED"
        note.cancelled_at = datetime.utcnow()
        note.cancel_reason = cancel_reason
        db.session.add(note)

    db.session.commit()
    return jsonify(vendor_note_to_dict(note, include_items=True, include_ledger=True)), 200


@app.route("/api/vendor-notes", methods=["GET"])
@login_required
@vendor_note_access_required(api=True)
def api_list_vendor_notes():
    vendor_id = request.args.get("vendor_id")
    note_type = (request.args.get("type") or "").strip().upper()
    status = (request.args.get("status") or "").strip().upper()
    date_from = parse_date(request.args.get("date_from"))
    date_to = parse_date(request.args.get("date_to"))
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1)
    per_page = int(request.args.get("per_page") or 50)

    query = VendorNote.query
    if vendor_id:
        query = query.filter(VendorNote.vendor_id == int(vendor_id))
    if note_type:
        query = query.filter(VendorNote.note_type == note_type)
    if status:
        query = query.filter(VendorNote.status == status)
    if date_from:
        query = query.filter(VendorNote.note_date >= date_from)
    if date_to:
        query = query.filter(VendorNote.note_date <= date_to)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            VendorNote.note_no.ilike(like),
            VendorNote.supplier_bill_no.ilike(like)
        ))

    total = query.count()
    notes = query.order_by(VendorNote.note_date.desc(), VendorNote.id.desc()).offset((page - 1) * per_page).limit(per_page).all()
    return jsonify({
        "page": page,
        "per_page": per_page,
        "total": total,
        "data": [vendor_note_to_dict(n) for n in notes]
    }), 200


@app.route("/api/vendor-notes/<int:note_id>", methods=["GET"])
@login_required
@vendor_note_access_required(api=True)
def api_view_vendor_note(note_id):
    note = VendorNote.query.get_or_404(note_id)
    return jsonify(vendor_note_to_dict(note, include_items=True, include_ledger=True)), 200

@app.route("/vendor/delete/<int:id>")
@login_required
def delete_vendor(id):
    v = Vendor.query.get_or_404(id)
    has_purchases = VendorPurchase.query.filter_by(vendor_id=v.id).first()
    if has_purchases:
        flash("Vendor cannot be deleted because purchase history exists.", "danger")
        return redirect("/vendor")
    db.session.delete(v)
    db.session.commit()
    flash("Vendor deleted successfully", "danger")
    return redirect("/vendor")

@app.route("/customer")
@login_required
def customer():
    return "<h2>Customer Master – Coming Soon</h2>"

@app.route("/appointments")
@login_required
def appointments():
    selected_date = parse_date(request.args.get("date")) or date.today()
    selected_status = (request.args.get("status") or "ALL").strip().upper()
    q = (request.args.get("q") or "").strip()

    query = Appointment.query.filter(Appointment.appointment_date == selected_date)
    if selected_status and selected_status != "ALL":
        query = query.filter(Appointment.status == selected_status)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            Appointment.appointment_no.ilike(like),
            Appointment.patient_name.ilike(like),
            Appointment.mobile.ilike(like),
            Appointment.doctor_name.ilike(like)
        ))

    appointments = query.order_by(Appointment.appointment_time.asc(), Appointment.id.asc()).all()

    status_counts = {status: 0 for status in APPOINTMENT_STATUSES}
    daily_appointments = Appointment.query.filter(
        Appointment.appointment_date == selected_date
    ).all()
    for item in daily_appointments:
        if item.status in status_counts:
            status_counts[item.status] += 1

    return render_template(
        "appointments.html",
        appointments=appointments,
        selected_date=selected_date.isoformat(),
        selected_status=selected_status,
        q=q,
        status_counts=status_counts,
        all_statuses=APPOINTMENT_STATUSES
    )

@app.route("/appointments/add", methods=["GET", "POST"])
@login_required
def add_appointment():
    doctor_names = get_doctor_suggestions()
    form_data = {
        "patient_name": "",
        "mobile": "",
        "doctor_name": "",
        "appointment_date": date.today().isoformat(),
        "appointment_time": "",
        "payment_mode": "CASH",
        "doctor_discount": "0",
        "consultation_fee": "0",
        "reason": "",
        "notes": ""
    }

    if request.method == "POST":
        form_data["patient_name"] = (request.form.get("patient_name") or "").strip()
        form_data["mobile"] = (request.form.get("mobile") or "").strip()
        form_data["doctor_name"] = (request.form.get("doctor_name") or "").strip()
        form_data["appointment_date"] = (request.form.get("appointment_date") or "").strip()
        form_data["appointment_time"] = (request.form.get("appointment_time") or "").strip()
        form_data["payment_mode"] = (request.form.get("payment_mode") or "CASH").strip().upper()
        form_data["doctor_discount"] = (request.form.get("doctor_discount") or "0").strip()
        form_data["consultation_fee"] = (request.form.get("consultation_fee") or "0").strip()
        form_data["reason"] = (request.form.get("reason") or "").strip()
        form_data["notes"] = (request.form.get("notes") or "").strip()

        appt_date = parse_date(form_data["appointment_date"])
        appt_time = parse_time_value(form_data["appointment_time"])

        if not form_data["patient_name"] or not form_data["doctor_name"] or not appt_date or not appt_time:
            flash("Patient, doctor, appointment date and time are required.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )

        if form_data["mobile"] and not form_data["mobile"].isdigit():
            flash("Mobile number should contain digits only.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )

        if form_data["payment_mode"] not in APPOINTMENT_PAYMENT_MODES:
            flash("Invalid payment mode selected.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )

        try:
            doctor_discount = float(form_data["doctor_discount"] or 0)
        except ValueError:
            flash("Doctor discount amount should be a valid number.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )
        if doctor_discount < 0:
            flash("Doctor discount amount cannot be negative.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )

        try:
            consultation_fee = float(form_data["consultation_fee"] or 0)
        except ValueError:
            flash("Consultation fee should be a valid number.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )
        if consultation_fee < 0:
            flash("Consultation fee cannot be negative.", "danger")
            return render_template(
                "add_appointment.html",
                doctor_names=doctor_names,
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES
            )

        appointment = Appointment(
            appointment_no=generate_appointment_no(),
            patient_name=form_data["patient_name"],
            mobile=form_data["mobile"],
            doctor_name=form_data["doctor_name"],
            appointment_date=appt_date,
            appointment_time=appt_time,
            payment_mode=form_data["payment_mode"],
            doctor_discount=doctor_discount,
            consultation_fee=consultation_fee,
            status="BOOKED",
            reason=form_data["reason"],
            notes=form_data["notes"],
            created_by=session.get("username")
        )
        db.session.add(appointment)
        db.session.commit()

        flash("Appointment booked successfully.", "success")
        return redirect(url_for("appointments", date=appt_date.isoformat()))

    return render_template(
        "add_appointment.html",
        doctor_names=doctor_names,
        form_data=form_data,
        payment_modes=APPOINTMENT_PAYMENT_MODES
    )

@app.route("/appointments/<int:id>/status", methods=["POST"])
@login_required
def update_appointment_status(id):
    appointment = Appointment.query.get_or_404(id)
    new_status = (request.form.get("status") or "").strip().upper()

    if new_status not in APPOINTMENT_STATUSES:
        flash("Invalid appointment status.", "danger")
        return redirect(request.referrer or url_for("appointments"))

    now = datetime.utcnow()
    appointment.status = new_status
    if new_status == "CHECKED_IN":
        appointment.checked_in_at = appointment.checked_in_at or now
    elif new_status == "COMPLETED":
        appointment.completed_at = now
        appointment.checked_in_at = appointment.checked_in_at or now
    elif new_status == "CANCELLED":
        appointment.cancelled_at = now

    db.session.commit()
    flash("Appointment status updated.", "success")
    return redirect(
        request.referrer or url_for(
            "appointments",
            date=(appointment.appointment_date.isoformat() if appointment.appointment_date else date.today().isoformat())
        )
    )

@app.route("/appointments/delete/<int:id>", methods=["POST"])
@login_required
def delete_appointment(id):
    appointment = Appointment.query.get_or_404(id)
    selected_date = appointment.appointment_date.isoformat() if appointment.appointment_date else date.today().isoformat()
    db.session.delete(appointment)
    db.session.commit()
    flash("Appointment deleted successfully.", "success")
    return redirect(request.referrer or url_for("appointments", date=selected_date))

@app.route("/reports", methods=["GET", "POST"])
@login_required
def reports():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_reports:
        flash("Access denied", "danger")
        return redirect("/")
    invoices = []
    total = 0
    profit_summary = None
    medicine_summary = []
    fast_movers = []
    medicine_totals = None
    report_filters = {
        "month": "",
        "year": "",
        "from_date": "",
        "to_date": "",
        "patient": "",
        "mobile": "",
        "medicine_query": "",
        "top_n": "10"
    }

    report_type = request.form.get("report_type")
    if request.method == "POST":
        for key in report_filters.keys():
            report_filters[key] = (request.form.get(key) or "").strip()
        if not report_filters["top_n"]:
            report_filters["top_n"] = "10"

        # DAILY
        if report_type == "daily":
            today = datetime.now().date()
            start = datetime.combine(today, time.min)
            end = datetime.combine(today, time.max)

            invoices = Invoice.query.filter(
                Invoice.created_at >= start,
                Invoice.created_at <= end
            ).all()

        # MONTHLY
        elif report_type == "monthly":
            month = int(request.form.get("month"))
            year = int(request.form.get("year"))

            invoices = Invoice.query.filter(
                db.extract("month", Invoice.created_at) == month,
                db.extract("year", Invoice.created_at) == year
            ).all()

        # CUSTOM DATE
        elif report_type == "custom":
            from_date = request.form.get("from_date")
            to_date = request.form.get("to_date")

            if from_date and to_date:
                from_dt = datetime.strptime(from_date, "%Y-%m-%d")
                to_dt = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
                invoices = Invoice.query.filter(
                    Invoice.created_at >= from_dt,
                    Invoice.created_at < to_dt
                ).all()

        # PATIENT WISE
        elif report_type == "patient":
            patient = request.form.get("patient").upper()
            invoices = Invoice.query.filter(
                Invoice.customer.like(f"%{patient}%")
            ).all()

        # MOBILE WISE
        elif report_type == "mobile":
            mobile = request.form.get("mobile")
            invoices = Invoice.query.filter_by(mobile=mobile).all()

        # PROFIT / LOSS (FIFO)
        elif report_type == "profit":
            from_date = request.form.get("from_date")
            to_date = request.form.get("to_date")
            if not from_date or not to_date:
                flash("Please select from and to dates", "danger")
            else:
                start = datetime.strptime(from_date, "%Y-%m-%d")
                end = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
                sales_total = db.session.query(db.func.coalesce(db.func.sum(Invoice.subtotal), 0)).filter(
                    Invoice.created_at >= start,
                    Invoice.created_at < end
                ).scalar() or 0
                returns_total = db.session.query(db.func.coalesce(db.func.sum(ReturnItem.net_amount), 0)).join(
                    Return, ReturnItem.return_id == Return.id
                ).filter(
                    Return.created_at >= start,
                    Return.created_at < end,
                    Return.is_cancelled == False
                ).scalar() or 0
                cogs = db.session.query(db.func.coalesce(db.func.sum(InvoiceItem.cost_amount), 0)).join(
                    Invoice, InvoiceItem.invoice_id == Invoice.id
                ).filter(
                    Invoice.created_at >= start,
                    Invoice.created_at < end
                ).scalar() or 0
                return_cogs = db.session.query(db.func.coalesce(db.func.sum(ReturnItem.cost_amount), 0)).join(
                    Return, ReturnItem.return_id == Return.id
                ).filter(
                    Return.created_at >= start,
                    Return.created_at < end,
                    Return.is_cancelled == False
                ).scalar() or 0

                net_sales = sales_total - returns_total
                net_cogs = cogs - return_cogs
                gross_profit = net_sales - net_cogs
                gross_profit_percentage = (gross_profit / net_sales * 100) if net_sales else 0

                profit_summary = {
                    "from_date": from_date,
                    "to_date": to_date,
                    "sales_total": round(sales_total, 2),
                    "returns_total": round(returns_total, 2),
                    "net_sales": round(net_sales, 2),
                    "cogs": round(cogs, 2),
                    "return_cogs": round(return_cogs, 2),
                    "net_cogs": round(net_cogs, 2),
                    "gross_profit": round(gross_profit, 2),
                    "gross_profit_percentage": round(gross_profit_percentage, 2)
                }

        # MEDICINE SUMMARY / FAST MOVERS
        elif report_type == "medicine":
            from_date = report_filters["from_date"]
            to_date = report_filters["to_date"]
            q = report_filters["medicine_query"].lower()
            top_n = to_int_safe(report_filters["top_n"], 10)
            if top_n < 1:
                top_n = 10
            if top_n > 100:
                top_n = 100
            report_filters["top_n"] = str(top_n)

            start = None
            end = None
            if from_date:
                try:
                    start = datetime.strptime(from_date, "%Y-%m-%d")
                except ValueError:
                    flash("Invalid from date", "danger")
            if to_date:
                try:
                    end = datetime.strptime(to_date, "%Y-%m-%d")
                except ValueError:
                    flash("Invalid to date", "danger")

            if start and end and end < start:
                flash("To date must be greater than or equal to from date", "danger")
            else:
                end_exclusive = (end + timedelta(days=1)) if end else None
                period_days = 0
                if start and end:
                    period_days = (end.date() - start.date()).days + 1
                elif start and not end:
                    period_days = (datetime.now().date() - start.date()).days + 1
                elif end and not start:
                    period_days = 1

                rows = {}
                med_name_by_id = {m.id: (m.name or "").strip() for m in Medicine.query.all()}

                def get_row(name):
                    key = (name or "").strip().upper()
                    if not key:
                        return None
                    if key not in rows:
                        rows[key] = {
                            "medicine": key,
                            "purchase_qty": 0,
                            "free_qty": 0,
                            "inward_qty": 0,
                            "sold_qty": 0,
                            "return_qty": 0,
                            "net_sold_qty": 0,
                            "current_stock": 0,
                            "purchase_value": 0.0,
                            "sales_value": 0.0,
                            "avg_daily_sale": 0.0
                        }
                    return rows[key]

                for med in Medicine.query.all():
                    row = get_row(med.name)
                    if not row:
                        continue
                    row["current_stock"] += to_int(med.qty)

                purchase_query = VendorPurchaseItem.query
                if start:
                    purchase_query = purchase_query.filter(VendorPurchaseItem.created_at >= start)
                if end_exclusive:
                    purchase_query = purchase_query.filter(VendorPurchaseItem.created_at < end_exclusive)
                for item in purchase_query.all():
                    med_name = (item.medicine_name or med_name_by_id.get(item.medicine_id) or "").strip()
                    row = get_row(med_name)
                    if not row:
                        continue
                    qty = to_int(item.qty)
                    free_qty = to_int(item.free_qty)
                    row["purchase_qty"] += qty
                    row["free_qty"] += free_qty
                    row["inward_qty"] += qty + free_qty
                    row["purchase_value"] += to_float(item.total_value)

                sales_query = InvoiceItem.query.join(Invoice, InvoiceItem.invoice_id == Invoice.id)
                if start:
                    sales_query = sales_query.filter(Invoice.created_at >= start)
                if end_exclusive:
                    sales_query = sales_query.filter(Invoice.created_at < end_exclusive)
                for item in sales_query.all():
                    row = get_row(item.name)
                    if not row:
                        continue
                    row["sold_qty"] += to_int(item.qty)
                    sales_value = item.net_amount if item.net_amount not in (None, 0) else item.amount
                    row["sales_value"] += to_float(sales_value)

                return_query = ReturnItem.query.join(Return, ReturnItem.return_id == Return.id).filter(
                    Return.is_cancelled == False
                )
                if start:
                    return_query = return_query.filter(Return.created_at >= start)
                if end_exclusive:
                    return_query = return_query.filter(Return.created_at < end_exclusive)
                for item in return_query.all():
                    row = get_row(item.medicine_name)
                    if not row:
                        continue
                    row["return_qty"] += to_int(item.qty)

                for row in rows.values():
                    row["net_sold_qty"] = row["sold_qty"] - row["return_qty"]
                    if period_days > 0:
                        row["avg_daily_sale"] = round(row["net_sold_qty"] / period_days, 2)
                    else:
                        row["avg_daily_sale"] = round(float(row["net_sold_qty"]), 2)
                    row["purchase_value"] = round(row["purchase_value"], 2)
                    row["sales_value"] = round(row["sales_value"], 2)

                medicine_summary = list(rows.values())
                if q:
                    medicine_summary = [
                        r for r in medicine_summary
                        if q in (r["medicine"] or "").lower()
                    ]
                medicine_summary.sort(key=lambda x: (x["medicine"] or "").lower())

                movers = [r for r in medicine_summary if r["net_sold_qty"] > 0]
                movers.sort(
                    key=lambda x: (x["avg_daily_sale"], x["net_sold_qty"], x["sales_value"]),
                    reverse=True
                )
                fast_movers = movers[:top_n]

                medicine_totals = {
                    "count": len(medicine_summary),
                    "purchase_qty": sum(r["purchase_qty"] for r in medicine_summary),
                    "free_qty": sum(r["free_qty"] for r in medicine_summary),
                    "inward_qty": sum(r["inward_qty"] for r in medicine_summary),
                    "sold_qty": sum(r["sold_qty"] for r in medicine_summary),
                    "return_qty": sum(r["return_qty"] for r in medicine_summary),
                    "net_sold_qty": sum(r["net_sold_qty"] for r in medicine_summary),
                    "current_stock": sum(r["current_stock"] for r in medicine_summary),
                    "purchase_value": round(sum(r["purchase_value"] for r in medicine_summary), 2),
                    "sales_value": round(sum(r["sales_value"] for r in medicine_summary), 2),
                    "period_days": period_days
                }

        total = sum(i.total for i in invoices)

    return render_template(
        "reports.html",
        invoices=invoices,
        total=round(total, 2),
        report_type=report_type,
        profit_summary=profit_summary,
        medicine_summary=medicine_summary,
        fast_movers=fast_movers,
        medicine_totals=medicine_totals,
        report_filters=report_filters
    )


@app.route("/reports/export")
@login_required
def export_excel():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_reports:
        flash("Access denied", "danger")
        return redirect("/")
    import pandas as pd
    from flask import send_file

    invoices = Invoice.query.all()

    data = [{
        "Invoice No": i.invoice_no,
        "Patient Name": i.customer,
        "Mobile": i.mobile,
        "Date": i.created_at.strftime("%d-%m-%Y"),
        "Total (ex GST)": i.subtotal,
        "Payment Mode": i.payment_mode,
        "User": i.created_by
    } for i in invoices]

    df = pd.DataFrame(data)

    file_path = "reports.xlsx"
    df.to_excel(file_path, index=False)

    return send_file(
        file_path,
        as_attachment=True,
        download_name="Pharmacy_Reports.xlsx"
    )
@app.route("/users")
@login_required
@admin_required
def users():
    users = User.query.all()
    return render_template("users.html", users=users)
@app.route("/users/edit/<int:user_id>", methods=["GET", "POST"])
@login_required
@admin_required
def edit_user(user_id):
    user = User.query.get_or_404(user_id)

    if request.method == "POST":
        user.role = request.form.get("role")

        user.can_view_medicine = True if request.form.get("can_view_medicine") else False
        user.can_add_medicine = True if request.form.get("can_add_medicine") else False
        user.can_edit_medicine = True if request.form.get("can_edit_medicine") else False
        user.can_delete_medicine = True if request.form.get("can_delete_medicine") else False
        user.can_edit_invoice = True if request.form.get("can_edit_invoice") else False
        user.can_delete_invoice = True if request.form.get("can_delete_invoice") else False
        user.can_invoice_action = True if request.form.get("can_invoice_action") else False
        if user.can_edit_invoice or user.can_delete_invoice:
            user.can_invoice_action = True
        user.can_view_stock_history = True if request.form.get("can_view_stock_history") else False
        user.can_view_reports = True if request.form.get("can_view_reports") else False
        user.can_manage_users = True if request.form.get("can_manage_users") else False

        db.session.commit()
        flash("User updated successfully")
        return redirect("/users")

    return render_template("edit_user.html", user=user)
@app.route("/users/add", methods=["GET", "POST"])
@login_required
@admin_required
def add_user():
    if request.method == "POST":
        can_edit_invoice = bool(request.form.get("can_edit_invoice"))
        can_delete_invoice = bool(request.form.get("can_delete_invoice"))
        username = (request.form.get("username") or "").strip()
        role = (request.form.get("role") or "user").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            flash("Username and password are required", "danger")
            return redirect("/users/add")
        user = User(
            username=username,
            role=role,
            can_view_medicine=bool(request.form.get("can_view_medicine")),
            can_add_medicine=bool(request.form.get("can_add_medicine")),
            can_edit_medicine=bool(request.form.get("can_edit_medicine")),
            can_delete_medicine=bool(request.form.get("can_delete_medicine")),
            can_edit_invoice=can_edit_invoice,
            can_delete_invoice=can_delete_invoice,
            can_invoice_action=bool(request.form.get("can_invoice_action")) or can_edit_invoice or can_delete_invoice,
            can_view_stock_history=bool(request.form.get("can_view_stock_history")),
            can_view_reports=bool(request.form.get("can_view_reports")),
            can_manage_users=bool(request.form.get("can_manage_users"))
            )
        user.set_password(password)
        
        db.session.add(user)
        db.session.commit()
        flash("User created")
        return redirect("/users")

    return render_template("add_user.html")
@app.route("/users/change-password/<int:user_id>", methods=["GET","POST"])
@login_required
@admin_required
def change_user_password(user_id):
    user = User.query.get_or_404(user_id)

    if request.method == "POST":
        new_password = (request.form.get("new_password") or request.form.get("password") or "").strip()
        confirm_password = (request.form.get("confirm_password") or "").strip()
        if not new_password:
            flash("Password is required", "danger")
            return redirect(request.url)
        if confirm_password and new_password != confirm_password:
            flash("New password and confirm password do not match", "danger")
            return redirect(request.url)
        user.set_password(new_password)
        db.session.commit()
        flash("Password updated")
        return redirect("/users")

    return render_template("change_password.html", user=user)
@app.route("/users/delete/<int:user_id>")
@login_required
@admin_required
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    if user.username == "admin":
        flash("Admin user cannot be deleted", "danger")
        return redirect("/users")

    db.session.delete(user)
    db.session.commit()
    flash("User deleted successfully", "success")
    return redirect("/users")
    # -------- PERMISSION DECORATORS --------

def permission_required(permission):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            user = User.query.get(session.get("user_id"))
            if not user:
                flash("Access denied", "danger")
                return redirect("/")
            if user.role == "admin":
                return f(*args, **kwargs)
            if not getattr(user, permission, False):
                flash("Access denied", "danger")
                return redirect("/")
            return f(*args, **kwargs)
        return wrapper
    return decorator
  
@app.route("/medicines/export")
@login_required
def export_medicines():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (
        user.can_view_medicine or user.can_add_medicine or user.can_edit_medicine or user.can_delete_medicine
    ):
        flash("Access denied", "danger")
        return redirect("/medicines")
    import pandas as pd
    from flask import send_file

    medicines = Medicine.query.order_by(Medicine.name).all()

    data = []
    for m in medicines:
        pack_display = ""
        if m.pack_type and m.pack_qty:
            pack_display = f"{m.pack_type} of {m.pack_qty}"
        elif m.pack_type:
            pack_display = m.pack_type
        elif m.pack_qty:
            pack_display = str(m.pack_qty)
        data.append({
            "Medicine Name": m.name,
            "Batch": m.batch,
            "Expiry (MM/YYYY)": f"{m.expiry[5:7]}/{m.expiry[0:4]}",
            "MRP": m.mrp,
            "Stock": m.qty,
            "Pack Type": m.pack_type,
            "Pack Qty": m.pack_qty,
            "Pack": pack_display
        })

    df = pd.DataFrame(data)

    file_path = "medicine_list.xlsx"
    df.to_excel(file_path, index=False)

    return send_file(file_path, as_attachment=True)
@app.route("/export-low-stock")
@login_required
def export_low_stock():
    import pandas as pd
    from flask import send_file

    meds = get_low_stock_items(limit=LOW_STOCK_LIMIT)

    data = []
    for m in meds:
        mrp_value = m["mrp"] if m["mrp"] is not None else "Multiple"
        data.append({
            "Medicine Name": m["name"],
            "Batch": m["batch"],
            "Current Stock": m["stock"],
            "MRP": mrp_value,
            "Expiry": m["expiry"],
            "Suggested Order Qty": m["suggested"]
        })

    df = pd.DataFrame(data)

    file_path = "low_stock_medicines.xlsx"
    df.to_excel(file_path, index=False)

    return send_file(file_path, as_attachment=True)

from datetime import datetime, time

@app.route("/stock-history")
@login_required
def stock_history():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_stock_history:
        flash("Access denied", "danger")
        return redirect("/")

    search = request.args.get("search")
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")

    query = StockHistory.query

    # 🔍 SEARCH
    if search:
        query = query.filter(
            StockHistory.medicine_name.ilike(f"%{search}%") |
            StockHistory.user.ilike(f"%{search}%") |
            StockHistory.action.ilike(f"%{search}%")
        )

    # 📅 FROM DATE
    if from_date:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        query = query.filter(StockHistory.created_at >= from_dt)

    # 📅 TO DATE (IMPORTANT FIX)
    if to_date:
        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
        to_dt = datetime.combine(to_dt.date(), time(23, 59, 59))
        query = query.filter(StockHistory.created_at <= to_dt)

    history = query.order_by(StockHistory.created_at.desc()).all()

    return render_template("stock_history.html", history=history)

@app.route("/stock-history/export")
@login_required
def export_stock_history():
    user = User.query.get(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_stock_history:
        flash("Access denied", "danger")
        return redirect("/")
    import pandas as pd
    from flask import send_file

    history = StockHistory.query.order_by(StockHistory.created_at.desc()).all()

    data = []
    for h in history:
        data.append({
            "Date": h.created_at.strftime("%d-%m-%Y"),
            "Medicine": h.medicine_name,
            "Batch": h.batch,
            "Type": h.action,
            "Old Stock": h.stock_before,
            "Change": h.qty_change,
            "New Stock": h.stock_after,
            "User": h.user,
            "Remark": h.remark
        })

    df = pd.DataFrame(data)
    file = "stock_history.xlsx"
    df.to_excel(file, index=False)

    return send_file(file, as_attachment=True)
    
@app.route("/stock-history/delete/<int:id>", methods=["POST"])
@login_required
@admin_required
def delete_stock_history(id):
    h = StockHistory.query.get_or_404(id)
    db.session.delete(h)
    db.session.commit()
    flash("Stock history deleted successfully", "success")
    return redirect("/stock-history")



# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
