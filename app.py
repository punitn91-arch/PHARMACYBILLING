# app.py
from flask import Flask, render_template, request, redirect,url_for, flash, session
from models import db, User, Medicine, StockHistory, Invoice, InvoiceItem, Return, ReturnItem, HoldBill, Vendor, VendorPurchase, VendorPurchaseItem, SalesAllocation
from datetime import datetime, time, timedelta, date
from functools import wraps
import webbrowser
import threading
import os
from werkzeug.utils import secure_filename
from sqlalchemy import text, or_

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

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None

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

        # Vendor extra fields
        ensure_column("vendor", "shop_name", "TEXT")
        ensure_column("vendor", "area", "TEXT")
        ensure_column("vendor", "city", "TEXT")
        ensure_column("vendor", "state", "TEXT")
        ensure_column("vendor", "pincode", "TEXT")
        ensure_column("vendor", "account_holder_name", "TEXT")
        ensure_column("vendor_purchase", "invoice_no", "TEXT")

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
        user = User.query.filter_by(username=request.form["username"]).first()
        if user and user.check_password(request.form["password"]):
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
    low_stock_count = Medicine.query.filter(Medicine.qty <= 5).count()

    # ---------- EXPIRING SOON (30 days) ----------
    exp_limit = today + timedelta(days=30)
    expiring_soon = Medicine.query.filter(
        Medicine.expiry <= exp_limit.strftime("%Y-%m-%d")
    ).count()

    # ---------- INVENTORY VALUE ----------
    inventory_value = sum(
        m.qty * m.mrp for m in Medicine.query.all()
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
    medicines = Medicine.query.filter(Medicine.qty <= 5).order_by(Medicine.qty).all()
    return render_template("low_stock.html", medicines=medicines)
@app.route("/order-list")
@login_required
def order_list():
    medicines = Medicine.query.filter(Medicine.qty <= 5).all()

    order_items = []
    for m in medicines:
        order_items.append({
            "name": m.name,
            "batch": m.batch,
            "stock": m.qty,
            "suggested": max(10 - m.qty, 5)  # auto order logic
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
        qty = int(request.form["qty"])

        med = Medicine(
            name=request.form["name"],
            batch=request.form["batch"],
            mrp=float(request.form["mrp"]),
            qty=qty,
            expiry=request.form["expiry"]
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
        med.name = request.form["name"]
        med.batch = request.form["batch"]
        med.expiry = request.form["expiry"]
        med.mrp = float(request.form["mrp"])
        med.discount_percent = int(request.form.get("discount_percent", 0))
        med.qty = int(request.form["qty"])

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
    
    cart = []

    if request.method == "POST":
        subtotal = 0
        total_discount = 0

        meds_list = request.form.getlist("medicine_name")
        batch_overrides = request.form.getlist("batch_override")
        qty_list = request.form.getlist("qty")
        if not qty_list:
            qty_list = request.form.getlist("qty[]")

        for idx, (name, qty) in enumerate(zip(meds_list, qty_list)):
            name = (name or "").strip().upper()
            if not name:
                continue
            qty = int(qty or 0)
            if qty <= 0:
                continue
            batch_override = (batch_overrides[idx] or "").strip() if idx < len(batch_overrides) else ""
            allocations = []

            if batch_override:
                med = Medicine.query.filter_by(name=name, batch=batch_override).first()
                if not med:
                    flash(f"Batch not found for {name}", "danger")
                    return redirect("/billing")
                exp_dt = parse_expiry_date(med.expiry)
                if exp_dt and exp_dt < date.today():
                    flash(f"Batch {med.batch} of {med.name} is expired", "danger")
                    return redirect("/billing")
                if qty > med.qty:
                    flash(f"Not enough stock for {med.name} ({med.batch})", "danger")
                    return redirect("/billing")
                allocations.append((med, qty))
            else:
                candidates = get_batch_candidates(name)
                total_available = sum(m.qty for m in candidates)
                if total_available <= 0:
                    flash(f"No stock available for {name}", "danger")
                    return redirect("/billing")
                if qty > total_available:
                    flash(f"Not enough stock for {name}. Available: {total_available}", "danger")
                    return redirect("/billing")
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

        last = Invoice.query.order_by(Invoice.id.desc()).first()
        inv_no = f"INV-{datetime.now().year}-{1000 + ((last.id + 1) if last else 1)}"
        inv = Invoice(
            invoice_no=inv_no,
            customer=request.form["customer"],
            mobile=request.form["mobile"],
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
            date=datetime.now().strftime("%d-%m-%Y")
        )
    return render_template("billing.html", medicines=meds, medicine_names=medicine_names)


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
            .filter(Return.invoice_id == invoice.id)
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
                .filter(Return.invoice_id == invoice.id)
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
    cart = []
    meds_list = request.form.getlist("medicine_name")
    batch_overrides = request.form.getlist("batch_override")
    qty_list = request.form.getlist("qty")
    if not qty_list:
        qty_list = request.form.getlist("qty[]")

    for idx, (name, qty) in enumerate(zip(meds_list, qty_list)):
        name = (name or "").strip().upper()
        if not name:
            continue
        qty_val = int(qty or 0)
        if qty_val <= 0:
            continue
        batch_override = (batch_overrides[idx] or "").strip() if idx < len(batch_overrides) else ""
        med = None
        if batch_override:
            med = Medicine.query.filter_by(name=name, batch=batch_override).first()
        if not med:
            med = Medicine.query.filter_by(name=name).first()
        cart.append({
            "name": name,
            "qty": qty_val,
            "mrp": med.mrp if med else 0,
            "batch": batch_override or (med.batch if med else ""),
            "expiry": med.expiry if med else "",
            "amount": qty_val * (med.mrp if med else 0)
        })

    db.session.add(HoldBill(
        customer=request.form["customer"],
        mobile=request.form["mobile"],
        doctor=request.form.get("doctor", ""),
        gender=request.form.get("gender", ""),
        data=cart
    ))
    db.session.commit()

    flash("Bill saved to Pending Bills", "info")
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
    hb = HoldBill.query.get_or_404(id)
    session["hold_bill"] = {
        "customer": hb.customer,
        "mobile": hb.mobile,
        "doctor": hb.doctor,
        "gender": hb.gender,
        "cart": hb.data
    }
    db.session.delete(hb)
    db.session.commit()
    return redirect("/billing")

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

@app.route("/vendor/reports")
@login_required
def vendor_reports():
    vendors = Vendor.query.order_by(Vendor.name).all()
    purchase_summary = []
    for v in vendors:
        total = db.session.query(db.func.sum(VendorPurchase.total_amount)).filter_by(vendor_id=v.id).scalar() or 0
        count = db.session.query(db.func.count(VendorPurchase.id)).filter_by(vendor_id=v.id).scalar() or 0
        purchase_summary.append({
            "vendor": v,
            "total": round(total, 2),
            "count": count,
            "outstanding": round(v.outstanding_balance or 0, 2)
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

        if not name:
            continue
        if not batch or not expiry:
            flash(f"Batch and expiry required for {name}", "danger")
            return redirect(f"/vendor/edit/{vendor.id}")
        if qty <= 0:
            continue

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
        subtotal=round(subtotal, 2),
        gst_total=round(gst_total, 2),
        discount_total=round(discount_total, 2),
        total_amount=round(total_amount, 2),
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

        item.medicine_name = name
        item.composition = composition
        item.company = company
        item.distributor_name = distributor_name
        item.pack_type = pack_type
        item.batch = batch
        item.expiry = expiry
        item.qty = qty
        item.free_qty = free_qty
        item.remaining_qty = new_total_qty - sold_qty
        item.purchase_rate = purchase_rate
        item.mrp = mrp
        item.gst_percent = gst_percent
        item.discount_percent = discount_percent
        item.total_value = round(new_total, 2)

        if purchase:
            purchase.subtotal = round(max((purchase.subtotal or 0) - old_taxable + new_taxable, 0), 2)
            purchase.gst_total = round(max((purchase.gst_total or 0) - old_gst + new_gst, 0), 2)
            purchase.discount_total = round(max((purchase.discount_total or 0) - old_discount + new_discount, 0), 2)
            purchase.total_amount = round(max((purchase.total_amount or 0) - old_total + new_total, 0), 2)

        if vendor:
            delta_total = new_total - old_total
            vendor.total_purchases = round((vendor.total_purchases or 0) + delta_total, 2)
            if purchase and (purchase.payment_status or "").lower() in ("unpaid", "partial"):
                vendor.outstanding_balance = round((vendor.outstanding_balance or 0) + delta_total, 2)

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

    report_type = request.form.get("report_type")

    if request.method == "POST":

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
                sales_total = db.session.query(db.func.coalesce(db.func.sum(Invoice.total), 0)).filter(
                    Invoice.created_at >= start,
                    Invoice.created_at < end
                ).scalar() or 0
                returns_total = db.session.query(db.func.coalesce(db.func.sum(Return.total_refund), 0)).filter(
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

                profit_summary = {
                    "from_date": from_date,
                    "to_date": to_date,
                    "sales_total": round(sales_total, 2),
                    "returns_total": round(returns_total, 2),
                    "net_sales": round(net_sales, 2),
                    "cogs": round(cogs, 2),
                    "return_cogs": round(return_cogs, 2),
                    "net_cogs": round(net_cogs, 2),
                    "gross_profit": round(gross_profit, 2)
                }

        total = sum(i.total for i in invoices)

    return render_template(
        "reports.html",
        invoices=invoices,
        total=round(total, 2),
        report_type=report_type,
        profit_summary=profit_summary
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
        "Total": i.total,
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
        user = User(
            username=request.form["username"],
            role=request.form["role"],
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
        user.set_password(request.form["password"])
        
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
        user.set_password(request.form["password"])
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
        data.append({
            "Medicine Name": m.name,
            "Batch": m.batch,
            "Expiry (MM/YYYY)": f"{m.expiry[5:7]}/{m.expiry[0:4]}",
            "MRP": m.mrp,
            "Stock": m.qty
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

    LOW_STOCK_LIMIT = 5   # 🔴 yaha se control hoga

    meds = Medicine.query.filter(Medicine.qty <= LOW_STOCK_LIMIT).all()

    data = []
    for m in meds:
        data.append({
            "Medicine Name": m.name,
            "Batch": m.batch,
            "Current Stock": m.qty,
            "MRP": m.mrp,
            "Expiry": m.expiry,
            "Suggested Order Qty": max(10 - m.qty, 0)
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

