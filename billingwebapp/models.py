from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import validates
from werkzeug.security import check_password_hash, generate_password_hash

db = SQLAlchemy()

# ================= USER =================
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)

    password_hash = db.Column(db.String(255), nullable=True)  # ⚠️ IMPORTANT

    role = db.Column(db.String(20), default="staff")
    session_version = db.Column(db.Integer, default=0)

    can_view_medicine = db.Column(db.Boolean, default=False)
    can_add_medicine = db.Column(db.Boolean, default=False)
    can_edit_medicine = db.Column(db.Boolean, default=False)
    can_delete_medicine = db.Column(db.Boolean, default=False)
    can_edit_invoice = db.Column(db.Boolean, default=False)
    can_delete_invoice = db.Column(db.Boolean, default=False)
    can_invoice_action = db.Column(db.Boolean, default=False)
    can_view_stock_history = db.Column(db.Boolean, default=False)
    can_view_reports = db.Column(db.Boolean, default=False)
    can_manage_users = db.Column(db.Boolean, default=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Use PBKDF2 explicitly so hashes work reliably across Python/OpenSSL builds.
    def set_password(self, password):
        self.password_hash = generate_password_hash(password, method="pbkdf2:sha256")

    def check_password(self, password):
        if not self.password_hash:
            return False
        try:
            return check_password_hash(self.password_hash, password)
        except (AttributeError, ValueError):
            # Handles environments where stored hash algorithm isn't supported (e.g. scrypt).
            return False


# ================= MEDICINE =================
class Medicine(db.Model):
    __tablename__ = "medicine"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    composition = db.Column(db.String(255))
    company = db.Column(db.String(150))
    pack_type = db.Column(db.String(50))
    pack_qty = db.Column(db.Integer)
    batch = db.Column(db.String(50), nullable=False)
    expiry = db.Column(db.String(10), nullable=False)
    mrp = db.Column(db.Float, nullable=False)
    qty = db.Column(db.Integer, default=0)
    discount_percent = db.Column(db.Integer, default=0)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ================= INVOICE =================
class Invoice(db.Model):
    __tablename__ = "invoice"

    id = db.Column(db.Integer, primary_key=True)
    invoice_no = db.Column(db.String(50), unique=True)
    customer = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    doctor = db.Column(db.String(100))
    gender = db.Column(db.String(10))

    subtotal = db.Column(db.Float, default=0)
    discount = db.Column(db.Float, default=0)
    cgst = db.Column(db.Float, default=0)
    sgst = db.Column(db.Float, default=0)
    total = db.Column(db.Float, default=0)
    payment_mode = db.Column(db.String(20), default="CASH")


    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class InvoiceItem(db.Model):
    __tablename__ = "invoice_item"

    id = db.Column(db.Integer, primary_key=True)
    invoice_id = db.Column(db.Integer, nullable=False)

    name = db.Column(db.String(150))
    qty = db.Column(db.Integer)
    price = db.Column(db.Float)
    amount = db.Column(db.Float)

    batch = db.Column(db.String(50))
    expiry = db.Column(db.String(10))

    discount_percent = db.Column(db.Float, default=0)
    discount_amount = db.Column(db.Float, default=0)
    net_amount = db.Column(db.Float, default=0)
    cost_price = db.Column(db.Float, default=0)
    cost_amount = db.Column(db.Float, default=0)

# ================= RETURN =================
class Return(db.Model):
    __tablename__ = "return_bill"

    id = db.Column(db.Integer, primary_key=True)
    return_no = db.Column(db.String(30), unique=True)
    invoice_id = db.Column(db.Integer, nullable=False)
    invoice_no = db.Column(db.String(50))
    customer = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    total_refund = db.Column(db.Float, default=0)
    cgst = db.Column(db.Float, default=0)
    sgst = db.Column(db.Float, default=0)
    payment_mode = db.Column(db.String(20), default="CASH")
    is_cancelled = db.Column(db.Boolean, default=False)
    cancelled_by = db.Column(db.String(50))
    cancelled_at = db.Column(db.DateTime)

    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ReturnItem(db.Model):
    __tablename__ = "return_item"

    id = db.Column(db.Integer, primary_key=True)
    return_id = db.Column(db.Integer, nullable=False)
    invoice_item_id = db.Column(db.Integer, nullable=False)

    medicine_id = db.Column(db.Integer)
    medicine_name = db.Column(db.String(150))
    batch = db.Column(db.String(50))
    expiry = db.Column(db.String(10))

    qty = db.Column(db.Integer)
    price = db.Column(db.Float)
    amount = db.Column(db.Float)
    purchase_rate = db.Column(db.Float, default=0)
    selling_rate = db.Column(db.Float, default=0)
    gst_percent = db.Column(db.Float, default=0)
    reason = db.Column(db.String(255))
    discount_percent = db.Column(db.Float, default=0)
    discount_amount = db.Column(db.Float, default=0)
    net_amount = db.Column(db.Float, default=0)
    cost_price = db.Column(db.Float, default=0)
    cost_amount = db.Column(db.Float, default=0)

# ================= HOLD BILL =================
class HoldBill(db.Model):
    __tablename__ = "hold_bill"

    id = db.Column(db.Integer, primary_key=True)
    customer = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    doctor = db.Column(db.String(100))
    gender = db.Column(db.String(10))

    data = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Patient(db.Model):
    __tablename__ = "patient"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, index=True)
    mobile = db.Column(db.String(20), unique=True, index=True)
    age = db.Column(db.Integer)
    gender = db.Column(db.String(10))
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @validates("age")
    def _sanitize_age(self, _key, value):
        if value in (None, "", " "):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None


# ================= APPOINTMENT =================
class Appointment(db.Model):
    __tablename__ = "appointment"

    id = db.Column(db.Integer, primary_key=True)
    appointment_no = db.Column(db.String(30), unique=True, index=True)
    token_no = db.Column(db.Integer, index=True)
    patient_id = db.Column(db.Integer, index=True)

    patient_name = db.Column(db.String(120), nullable=False)
    mobile = db.Column(db.String(20))
    age = db.Column(db.Integer)
    gender = db.Column(db.String(10))
    doctor_name = db.Column(db.String(120), nullable=False)

    appointment_date = db.Column(db.Date, nullable=False, index=True)
    appointment_time = db.Column(db.Time, nullable=False)
    payment_mode = db.Column(db.String(20), default="CASH")
    payment_status = db.Column(db.String(20), default="UNPAID")
    doctor_discount = db.Column(db.Float, default=0)
    consultation_fee = db.Column(db.Float, default=0)

    status = db.Column(db.String(20), default="BOOKED", index=True)
    reason = db.Column(db.String(255))
    notes = db.Column(db.Text)
    symptoms = db.Column(db.Text)
    previous_visit_notes = db.Column(db.Text)

    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    checked_in_at = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)


# ================= CONSULTATION =================
class Consultation(db.Model):
    __tablename__ = "consultation"

    id = db.Column(db.Integer, primary_key=True)
    appointment_id = db.Column(db.Integer, unique=True, index=True)
    patient_id = db.Column(db.Integer, index=True)

    patient_name = db.Column(db.String(120), nullable=False)
    mobile = db.Column(db.String(20))
    age = db.Column(db.Integer)
    gender = db.Column(db.String(10))

    appointment_date = db.Column(db.Date)
    appointment_time = db.Column(db.Time)

    complaints = db.Column(db.Text)
    diagnosis = db.Column(db.Text)
    advice = db.Column(db.Text)

    bp = db.Column(db.String(20))
    pulse = db.Column(db.String(20))
    temperature = db.Column(db.String(20))
    weight = db.Column(db.String(20))
    spo2 = db.Column(db.String(20))

    follow_up_date = db.Column(db.Date)
    follow_up_notes = db.Column(db.Text)
    status = db.Column(db.String(20), default="IN_PROGRESS")

    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ConsultationItem(db.Model):
    __tablename__ = "consultation_item"

    id = db.Column(db.Integer, primary_key=True)
    consultation_id = db.Column(db.Integer, nullable=False, index=True)

    medicine_name = db.Column(db.String(150))
    dosage = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    duration = db.Column(db.String(50))
    instructions = db.Column(db.String(255))
    cancelled_at = db.Column(db.DateTime)

    @validates("age")
    def _sanitize_age(self, _key, value):
        if value in (None, "", " "):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None


# ================= STOCK HISTORY =================
class StockHistory(db.Model):
    __tablename__ = "stock_history"

    id = db.Column(db.Integer, primary_key=True)

    medicine_id = db.Column(db.Integer)
    medicine_name = db.Column(db.String(150))
    batch = db.Column(db.String(50))

    action = db.Column(db.String(20))
    # ADD, SALE, RETURN, ADJUST

    qty_change = db.Column(db.Integer)      # + / -
    stock_before = db.Column(db.Integer)
    stock_after = db.Column(db.Integer)

    remark = db.Column(db.String(255))
    user = db.Column(db.String(50))
    ref_table = db.Column(db.String(50))
    ref_id = db.Column(db.Integer)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ================= VENDOR =================
class Vendor(db.Model):
    __tablename__ = "vendor"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(150), nullable=False)
    mobile = db.Column(db.String(20))
    email = db.Column(db.String(120))
    gst_no = db.Column(db.String(30))
    shop_name = db.Column(db.String(150))
    area = db.Column(db.String(150))
    city = db.Column(db.String(100))
    state = db.Column(db.String(100))
    pincode = db.Column(db.String(20))
    address = db.Column(db.String(255))

    vendor_type = db.Column(db.String(50))
    credit_days = db.Column(db.Integer, default=0)
    credit_limit = db.Column(db.Float, default=0)

    bank_name = db.Column(db.String(100))
    account_holder_name = db.Column(db.String(150))
    account_no = db.Column(db.String(50))
    ifsc = db.Column(db.String(20))
    upi = db.Column(db.String(50))

    categories = db.Column(db.String(255))
    salts = db.Column(db.String(255))

    last_purchase_date = db.Column(db.Date)
    total_purchases = db.Column(db.Float, default=0)
    outstanding_balance = db.Column(db.Float, default=0)
    payment_status = db.Column(db.String(30))

    rate_history = db.Column(db.Text)
    default_payment_mode = db.Column(db.String(20))
    notes = db.Column(db.Text)
    attachment_ref = db.Column(db.String(255))

    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ================= VENDOR PURCHASE =================
class VendorPurchase(db.Model):
    __tablename__ = "vendor_purchase"

    id = db.Column(db.Integer, primary_key=True)
    vendor_id = db.Column(db.Integer, nullable=False)
    purchase_no = db.Column(db.String(30), unique=True)
    invoice_no = db.Column(db.String(60))
    purchase_date = db.Column(db.DateTime, default=datetime.utcnow)
    payment_mode = db.Column(db.String(30))
    payment_status = db.Column(db.String(30))
    paid_amount = db.Column(db.Float, default=0)
    subtotal = db.Column(db.Float, default=0)
    gst_total = db.Column(db.Float, default=0)
    discount_total = db.Column(db.Float, default=0)
    total_amount = db.Column(db.Float, default=0)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class VendorPurchaseItem(db.Model):
    __tablename__ = "vendor_purchase_item"

    id = db.Column(db.Integer, primary_key=True)
    purchase_id = db.Column(db.Integer, nullable=False)
    vendor_id = db.Column(db.Integer, nullable=False)
    medicine_id = db.Column(db.Integer)
    medicine_name = db.Column(db.String(150))
    composition = db.Column(db.String(255))
    company = db.Column(db.String(150))
    distributor_name = db.Column(db.String(150))
    pack_type = db.Column(db.String(50))
    pack_qty = db.Column(db.Integer)
    batch = db.Column(db.String(50))
    expiry = db.Column(db.String(10))
    qty = db.Column(db.Integer, default=0)
    free_qty = db.Column(db.Integer, default=0)
    remaining_qty = db.Column(db.Integer, default=0)
    purchase_rate = db.Column(db.Float, default=0)
    mrp = db.Column(db.Float, default=0)
    gst_percent = db.Column(db.Float, default=0)
    discount_percent = db.Column(db.Float, default=0)
    total_value = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ================= VENDOR NOTES (DEBIT/CREDIT) =================
class VendorNote(db.Model):
    __tablename__ = "vendor_notes"

    id = db.Column(db.Integer, primary_key=True)
    note_no = db.Column(db.String(40), unique=True, nullable=False)
    note_type = db.Column(db.String(10), nullable=False)  # DEBIT / CREDIT
    vendor_id = db.Column(db.Integer, db.ForeignKey("vendor.id"), nullable=False, index=True)
    reference_purchase_id = db.Column(db.Integer, db.ForeignKey("vendor_purchase.id"), index=True)
    supplier_bill_no = db.Column(db.String(60))
    note_date = db.Column(db.Date, nullable=False, index=True)
    status = db.Column(db.String(20), default="DRAFT")
    reason_code = db.Column(db.String(30))
    reason_text = db.Column(db.String(255))
    subtotal = db.Column(db.Numeric(12, 4), default=0)
    gst_total = db.Column(db.Numeric(12, 4), default=0)
    round_off = db.Column(db.Numeric(12, 4), default=0)
    grand_total = db.Column(db.Numeric(12, 4), default=0)
    outstanding_impact = db.Column(db.Numeric(12, 4), default=0)
    remarks = db.Column(db.Text)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    posted_at = db.Column(db.DateTime)
    cancelled_at = db.Column(db.DateTime)
    cancel_reason = db.Column(db.Text)


class VendorNoteItem(db.Model):
    __tablename__ = "vendor_note_items"

    id = db.Column(db.Integer, primary_key=True)
    note_id = db.Column(db.Integer, db.ForeignKey("vendor_notes.id"), nullable=False, index=True)
    medicine_id = db.Column(db.Integer, db.ForeignKey("medicine.id"), index=True)
    batch_no = db.Column(db.String(50))
    expiry = db.Column(db.String(10))
    qty = db.Column(db.Integer)
    free_qty = db.Column(db.Integer, default=0)
    purchase_rate = db.Column(db.Numeric(12, 4), default=0)
    mrp = db.Column(db.Numeric(12, 4))
    gst_percent = db.Column(db.Numeric(5, 2))
    disc_percent = db.Column(db.Numeric(5, 2))
    line_total = db.Column(db.Numeric(12, 4), default=0)
    hsn = db.Column(db.String(30))


class VendorNoteAllocation(db.Model):
    __tablename__ = "vendor_note_allocations"

    id = db.Column(db.Integer, primary_key=True)
    note_id = db.Column(db.Integer, db.ForeignKey("vendor_notes.id"), nullable=False, index=True)
    note_item_id = db.Column(db.Integer, db.ForeignKey("vendor_note_items.id"), nullable=False, index=True)
    purchase_item_id = db.Column(db.Integer, db.ForeignKey("vendor_purchase_item.id"), nullable=False, index=True)
    qty = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class VendorLedgerEntry(db.Model):
    __tablename__ = "vendor_ledger"

    id = db.Column(db.Integer, primary_key=True)
    vendor_id = db.Column(db.Integer, db.ForeignKey("vendor.id"), nullable=False, index=True)
    txn_date = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    txn_type = db.Column(db.String(30))  # PURCHASE / PAYMENT / DEBIT_NOTE / CREDIT_NOTE / REVERSAL
    ref_table = db.Column(db.String(50))
    ref_id = db.Column(db.Integer)
    debit = db.Column(db.Numeric(12, 4), default=0)
    credit = db.Column(db.Numeric(12, 4), default=0)
    running_balance = db.Column(db.Numeric(12, 4))
    notes = db.Column(db.Text)


# ================= FIFO SALES ALLOCATION =================
class SalesAllocation(db.Model):
    __tablename__ = "sales_allocation"

    id = db.Column(db.Integer, primary_key=True)
    invoice_item_id = db.Column(db.Integer, nullable=False)
    purchase_item_id = db.Column(db.Integer)
    qty = db.Column(db.Integer, default=0)
    cost_rate = db.Column(db.Float, default=0)
    returned_qty = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ================= AUDIT LOG =================
class AuditLog(db.Model):
    __tablename__ = "audit_log"

    id = db.Column(db.Integer, primary_key=True)
    user = db.Column(db.String(50))
    action = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


