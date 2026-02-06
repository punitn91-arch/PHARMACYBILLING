from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

db = SQLAlchemy()

# ================= USER =================
from werkzeug.security import generate_password_hash, check_password_hash

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)

    password_hash = db.Column(db.String(255), nullable=True)  # ⚠️ IMPORTANT

    role = db.Column(db.String(20), default="staff")

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

    # 🔐 password set
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    # 🔐 password check
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


# ================= MEDICINE =================
class Medicine(db.Model):
    __tablename__ = "medicine"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    composition = db.Column(db.String(255))
    company = db.Column(db.String(150))
    pack_type = db.Column(db.String(50))
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


