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
    access_profile = db.Column(db.String(40), default="custom")
    session_version = db.Column(db.Integer, default=0)
    is_active = db.Column(db.Boolean, default=True, index=True)
    deleted_at = db.Column(db.DateTime, index=True)
    deleted_by = db.Column(db.String(50))
    last_login_at = db.Column(db.DateTime, index=True)
    last_login_ip = db.Column(db.String(80))
    last_login_user_agent = db.Column(db.String(255))

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
    can_manage_purchases = db.Column(db.Boolean, default=False)
    can_view_audit_logs = db.Column(db.Boolean, default=False)
    can_view_profit_dashboard = db.Column(db.Boolean, default=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

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
    name = db.Column(db.String(150), nullable=False, index=True)
    medicine_code = db.Column(db.String(40), index=True)
    composition = db.Column(db.String(255))
    company = db.Column(db.String(150))
    pack_type = db.Column(db.String(50))
    pack_qty = db.Column(db.Integer)
    batch = db.Column(db.String(50), nullable=False, index=True)
    expiry = db.Column(db.String(10), nullable=False)
    mrp = db.Column(db.Float, nullable=False)
    qty = db.Column(db.Integer, default=0)
    discount_percent = db.Column(db.Integer, default=0)
    barcode = db.Column(db.String(80), index=True)
    reorder_level = db.Column(db.Integer, default=10)
    is_active = db.Column(db.Boolean, default=True)
    # GST rate (%) included in the MRP and drug schedule ("", "H", "H1", "X")
    # for the Schedule H1 register.
    gst_percent = db.Column(db.Float, default=5)
    schedule_type = db.Column(db.String(10), default="")

    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


# ================= INVOICE =================
class Invoice(db.Model):
    __tablename__ = "invoice"

    id = db.Column(db.Integer, primary_key=True)
    invoice_no = db.Column(db.String(50), unique=True)
    patient_id = db.Column(db.Integer, index=True)
    customer = db.Column(db.String(100), index=True)
    mobile = db.Column(db.String(20), index=True)
    customer_gst_no = db.Column(db.String(30), index=True)
    doctor = db.Column(db.String(100))
    gender = db.Column(db.String(10))

    subtotal = db.Column(db.Float, default=0)
    discount = db.Column(db.Float, default=0)
    cgst = db.Column(db.Float, default=0)
    sgst = db.Column(db.Float, default=0)
    total = db.Column(db.Float, default=0)
    payment_mode = db.Column(db.String(20), default="CASH", index=True)
    cash_amount = db.Column(db.Numeric(10, 2), default=0)
    online_amount = db.Column(db.Numeric(10, 2), default=0)
    is_split_payment = db.Column(db.Boolean, default=False)
    return_credit_used = db.Column(db.Float, default=0)
    final_payable = db.Column(db.Float, default=0)
    refund_amount = db.Column(db.Float, default=0)
    internal_note = db.Column(db.Text)
    print_profile_code = db.Column(db.String(40))
    print_address_line_1 = db.Column(db.String(255))
    print_address_line_2 = db.Column(db.String(255))
    print_mobile = db.Column(db.String(30))
    print_gst_no = db.Column(db.String(40))
    print_licence_no = db.Column(db.String(80))
    print_logo_path = db.Column(db.String(255))

    # Invoices are never deleted (GST rule): they are cancelled and kept.
    is_cancelled = db.Column(db.Boolean, default=False, index=True)
    cancelled_at = db.Column(db.DateTime)
    cancelled_by = db.Column(db.String(50))
    cancel_reason = db.Column(db.String(255))

    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

class InvoiceItem(db.Model):
    __tablename__ = "invoice_item"

    id = db.Column(db.Integer, primary_key=True)
    invoice_id = db.Column(db.Integer, nullable=False, index=True)

    name = db.Column(db.String(150), index=True)
    qty = db.Column(db.Integer)
    price = db.Column(db.Float)
    amount = db.Column(db.Float)

    batch = db.Column(db.String(50), index=True)
    expiry = db.Column(db.String(10))

    discount_percent = db.Column(db.Float, default=0)
    discount_amount = db.Column(db.Float, default=0)
    net_amount = db.Column(db.Float, default=0)
    cost_price = db.Column(db.Float, default=0)
    cost_amount = db.Column(db.Float, default=0)
    # GST snapshot at the time of sale (MRP is GST-inclusive).
    gst_percent = db.Column(db.Float)
    taxable_amount = db.Column(db.Float)
    gst_amount = db.Column(db.Float)

# ================= RETURN =================
class Return(db.Model):
    __tablename__ = "return_bill"

    id = db.Column(db.Integer, primary_key=True)
    return_no = db.Column(db.String(30), unique=True)
    invoice_id = db.Column(db.Integer, nullable=False, index=True)
    invoice_no = db.Column(db.String(50))
    customer = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    total_refund = db.Column(db.Float, default=0)
    cgst = db.Column(db.Float, default=0)
    sgst = db.Column(db.Float, default=0)
    payment_mode = db.Column(db.String(20), default="CASH")
    adjusted_invoice_id = db.Column(db.Integer, index=True)
    adjusted_amount = db.Column(db.Float, default=0)
    refund_amount = db.Column(db.Float, default=0)
    cash_refund_amount = db.Column(db.Numeric(10, 2), default=0)
    online_refund_amount = db.Column(db.Numeric(10, 2), default=0)
    is_split_refund = db.Column(db.Boolean, default=False)
    is_cancelled = db.Column(db.Boolean, default=False)
    cancelled_by = db.Column(db.String(50))
    cancelled_at = db.Column(db.DateTime)

    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class ReturnItem(db.Model):
    __tablename__ = "return_item"

    id = db.Column(db.Integer, primary_key=True)
    return_id = db.Column(db.Integer, nullable=False, index=True)
    invoice_item_id = db.Column(db.Integer, nullable=False, index=True)

    medicine_id = db.Column(db.Integer)
    medicine_name = db.Column(db.String(150), index=True)
    batch = db.Column(db.String(50), index=True)
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
    # RESTOCK = back to sellable stock, DAMAGED / EXPIRED = kept aside in
    # quarantine (never sold again until an admin decides).
    disposition = db.Column(db.String(20), default="RESTOCK")


class ReturnLotAllocation(db.Model):
    """Purchase lots that received stock back from a manual (no-invoice) return."""

    __tablename__ = "return_lot_allocation"

    id = db.Column(db.Integer, primary_key=True)
    return_item_id = db.Column(db.Integer, nullable=False, index=True)
    purchase_item_id = db.Column(db.Integer, nullable=False, index=True)
    qty = db.Column(db.Integer, default=0)
    cost_rate = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class QuarantineStock(db.Model):
    """Damaged / expired returned units kept out of sellable stock."""

    __tablename__ = "quarantine_stock"

    id = db.Column(db.Integer, primary_key=True)
    medicine_id = db.Column(db.Integer, index=True)
    medicine_name = db.Column(db.String(150), index=True)
    batch = db.Column(db.String(50), index=True)
    expiry = db.Column(db.String(10))
    qty = db.Column(db.Integer, default=0)
    reason = db.Column(db.String(20))  # DAMAGED / EXPIRED
    note = db.Column(db.String(255))
    status = db.Column(db.String(20), default="PENDING", index=True)  # PENDING / RESTOCKED / WRITTEN_OFF / CANCELLED
    return_id = db.Column(db.Integer, index=True)
    return_item_id = db.Column(db.Integer, index=True)
    invoice_item_id = db.Column(db.Integer)
    cost_rate = db.Column(db.Float, default=0)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    resolved_by = db.Column(db.String(50))
    resolved_at = db.Column(db.DateTime)

# ================= HOLD BILL =================
class HoldBill(db.Model):
    __tablename__ = "hold_bill"

    id = db.Column(db.Integer, primary_key=True)
    customer = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    doctor = db.Column(db.String(100))
    gender = db.Column(db.String(10))

    data = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    is_deleted = db.Column(db.Boolean, default=False, index=True)
    deleted_at = db.Column(db.DateTime, index=True)
    deleted_by = db.Column(db.String(50))


class PendingBillStore(db.Model):
    __tablename__ = "pending_bill_store"

    id = db.Column(db.Integer, primary_key=True)
    legacy_hold_bill_id = db.Column(db.Integer, unique=True, index=True)
    customer = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    doctor = db.Column(db.String(100))
    gender = db.Column(db.String(10))
    data_text = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    is_deleted = db.Column(db.Boolean, default=False, index=True)
    deleted_at = db.Column(db.DateTime, index=True)
    deleted_by = db.Column(db.String(50))


class Patient(db.Model):
    __tablename__ = "patient"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, index=True)
    mobile = db.Column(db.String(20), unique=True, index=True)
    age = db.Column(db.Integer)
    gender = db.Column(db.String(10))
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
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
    mobile = db.Column(db.String(20), index=True)
    age = db.Column(db.Integer)
    gender = db.Column(db.String(10))
    doctor_name = db.Column(db.String(120), nullable=False)
    clinician_id = db.Column(
        db.Integer,
        db.ForeignKey("clinician.id", ondelete="SET NULL"),
        index=True,
    )
    location_id = db.Column(
        db.Integer,
        db.ForeignKey("clinic_location.id", ondelete="SET NULL"),
        index=True,
    )

    appointment_date = db.Column(db.Date, nullable=False, index=True)
    appointment_time = db.Column(db.Time, nullable=False)
    payment_mode = db.Column(db.String(20), default="CASH", index=True)
    payment_status = db.Column(db.String(20), default="UNPAID")
    doctor_discount = db.Column(db.Float, default=0)
    consultation_fee = db.Column(db.Float, default=0)

    status = db.Column(db.String(20), default="BOOKED", index=True)
    reason = db.Column(db.String(255))
    notes = db.Column(db.Text)
    symptoms = db.Column(db.Text)
    previous_visit_notes = db.Column(db.Text)

    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    checked_in_at = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)
    cancelled_at = db.Column(db.DateTime)
    source = db.Column(db.String(30), nullable=False, default="ADMIN", index=True)
    external_call_id = db.Column(db.String(128), index=True)
    external_session_id = db.Column(db.String(128), index=True)
    external_request_id = db.Column(db.String(80), index=True)
    slot_start_at = db.Column(db.DateTime, index=True)
    slot_end_at = db.Column(db.DateTime, index=True)
    idempotency_key_hash = db.Column(db.String(64), index=True)
    cancellation_reason = db.Column(db.String(255))
    cancelled_by_source = db.Column(db.String(30))
    rescheduled_from_id = db.Column(db.Integer, index=True)
    previous_appointment_date = db.Column(db.Date, index=True)
    previous_appointment_time = db.Column(db.Time)
    rescheduled_at = db.Column(db.DateTime, index=True)
    late_arrival_status = db.Column(db.String(30), index=True)
    late_arrival_at = db.Column(db.DateTime, index=True)
    is_deleted = db.Column(db.Boolean, default=False, index=True)
    deleted_at = db.Column(db.DateTime, index=True)
    deleted_by = db.Column(db.String(50))

    @validates("age")
    def _sanitize_age(self, _key, value):
        if value in (None, "", " "):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None


# ================= LAB =================
class LabTest(db.Model):
    """Central, admin-managed catalog used by the lab billing screen."""

    __tablename__ = "lab_test"

    id = db.Column(db.Integer, primary_key=True)
    test_code = db.Column(db.String(40), unique=True, nullable=False, index=True)
    name = db.Column(db.String(180), nullable=False, index=True)
    category = db.Column(db.String(100), index=True)
    specimen_type = db.Column(db.String(100))
    preparation = db.Column(db.String(500))
    default_price = db.Column(db.Float, nullable=False, default=0)
    # JSON array of alternate spoken/search names, e.g. ["CBC", "complete blood count"].
    aliases_json = db.Column(db.Text, nullable=False, default="[]")
    fasting_required = db.Column(db.Boolean, nullable=False, default=False, index=True)
    turnaround_text = db.Column(db.String(120))
    is_active = db.Column(db.Boolean, default=True, nullable=False, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class LabOrder(db.Model):
    """A lab receipt/order kept independent from medicine invoices and stock."""

    __tablename__ = "lab_order"

    id = db.Column(db.Integer, primary_key=True)
    order_no = db.Column(db.String(40), unique=True, index=True)
    patient_id = db.Column(db.Integer, index=True)
    patient_name = db.Column(db.String(120), nullable=False, index=True)
    mobile = db.Column(db.String(20), index=True)
    gender = db.Column(db.String(10))
    doctor = db.Column(db.String(120))

    subtotal = db.Column(db.Float, default=0)
    discount = db.Column(db.Float, default=0)
    total = db.Column(db.Float, default=0)
    payment_mode = db.Column(db.String(20), default="CASH", index=True)
    cash_amount = db.Column(db.Numeric(10, 2), default=0)
    online_amount = db.Column(db.Numeric(10, 2), default=0)
    is_split_payment = db.Column(db.Boolean, default=False)
    internal_note = db.Column(db.Text)

    # Phase 1 creates ORDERED records. Sample/report workflow can extend this safely.
    status = db.Column(db.String(20), default="ORDERED", nullable=False, index=True)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class LabOrderItem(db.Model):
    """Immutable test and price snapshot for a lab order."""

    __tablename__ = "lab_order_item"

    id = db.Column(db.Integer, primary_key=True)
    lab_order_id = db.Column(db.Integer, nullable=False, index=True)
    lab_test_id = db.Column(db.Integer, index=True)
    test_code = db.Column(db.String(40), index=True)
    test_name = db.Column(db.String(180), nullable=False, index=True)
    category = db.Column(db.String(100))
    specimen_type = db.Column(db.String(100))
    preparation = db.Column(db.String(500))
    qty = db.Column(db.Integer, nullable=False, default=1)
    unit_price = db.Column(db.Float, nullable=False, default=0)
    amount = db.Column(db.Float, nullable=False, default=0)
    discount_percent = db.Column(db.Float, default=0)
    discount_amount = db.Column(db.Float, default=0)
    net_amount = db.Column(db.Float, default=0)


class LabReport(db.Model):
    """A private, publishable PDF report linked to a lab order."""

    __tablename__ = "lab_report"

    id = db.Column(db.Integer, primary_key=True)
    lab_order_id = db.Column(db.Integer, nullable=False, index=True)
    lab_order_item_id = db.Column(db.Integer, index=True)
    patient_id = db.Column(db.Integer, index=True)
    patient_name = db.Column(db.String(120), nullable=False, index=True)
    mobile = db.Column(db.String(20), nullable=False, index=True)
    title = db.Column(db.String(180), nullable=False)
    report_date = db.Column(db.Date, index=True)
    patient_note = db.Column(db.String(500))
    storage_key = db.Column(db.String(255), nullable=False, unique=True)
    original_filename = db.Column(db.String(255), nullable=False)
    mime_type = db.Column(db.String(100), nullable=False, default="application/pdf")
    file_size = db.Column(db.Integer, default=0)
    file_sha256 = db.Column(db.String(64), index=True)
    status = db.Column(db.String(20), nullable=False, default="DRAFT", index=True)
    uploaded_by = db.Column(db.String(50))
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    published_by = db.Column(db.String(50))
    published_at = db.Column(db.DateTime, index=True)
    revoked_by = db.Column(db.String(50))
    revoked_at = db.Column(db.DateTime, index=True)
    revoke_reason = db.Column(db.String(500))
    download_count = db.Column(db.Integer, nullable=False, default=0)
    last_downloaded_at = db.Column(db.DateTime, index=True)
    delivery_status = db.Column(db.String(30), nullable=False, default="NOT_SENT", index=True)
    last_delivery_at = db.Column(db.DateTime, index=True)


class PortalOtpChallenge(db.Model):
    """Short-lived, hashed OTP challenge for public patient portals."""

    __tablename__ = "portal_otp_challenge"

    id = db.Column(db.Integer, primary_key=True)
    purpose = db.Column(db.String(30), nullable=False, index=True)
    context_ref = db.Column(db.String(100), index=True)
    mobile = db.Column(db.String(20), nullable=False, index=True)
    otp_hash = db.Column(db.String(255), nullable=False)
    attempt_count = db.Column(db.Integer, nullable=False, default=0)
    max_attempts = db.Column(db.Integer, nullable=False, default=5)
    sent_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    verified_at = db.Column(db.DateTime, index=True)
    used_at = db.Column(db.DateTime, index=True)
    request_ip = db.Column(db.String(80))
    request_user_agent = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class AppointmentBookingSettings(db.Model):
    """Admin-controlled policy for the public QR appointment portal."""

    __tablename__ = "appointment_booking_settings"

    id = db.Column(db.Integer, primary_key=True)
    booking_enabled = db.Column(db.Boolean, nullable=False, default=True)
    normal_daily_limit = db.Column(db.Integer, nullable=False, default=15)
    priority_daily_limit = db.Column(db.Integer, nullable=False, default=2)
    normal_fee = db.Column(db.Float, nullable=False, default=600)
    priority_fee = db.Column(db.Float, nullable=False, default=1000)
    # ``opening_time`` and ``slot_minutes`` are retained for older staff
    # settings records.  The public portal is now first-come, first-served;
    # its patient-facing arrival window is deliberately separate from staff
    # appointment times.
    opening_time = db.Column(db.String(5), nullable=False, default="17:30")
    slot_minutes = db.Column(db.Integer, nullable=False, default=20)
    arrival_window_start = db.Column(db.String(5), nullable=False, default="17:30")
    arrival_window_end = db.Column(db.String(5), nullable=False, default="19:45")
    max_days_ahead = db.Column(db.Integer, nullable=False, default=14)
    booking_cutoff_minutes = db.Column(db.Integer, nullable=False, default=30)
    updated_by = db.Column(db.String(50))
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PublicAppointmentBooking(db.Model):
    """Public QR booking state kept separate until an appointment is confirmed."""

    __tablename__ = "public_appointment_booking"

    id = db.Column(db.Integer, primary_key=True)
    booking_ref = db.Column(db.String(64), nullable=False, unique=True, index=True)
    appointment_id = db.Column(db.Integer, index=True)
    patient_id = db.Column(db.Integer, index=True)
    patient_name = db.Column(db.String(120), nullable=False, index=True)
    mobile = db.Column(db.String(20), nullable=False, index=True)
    gender = db.Column(db.String(10))
    appointment_date = db.Column(db.Date, nullable=False, index=True)
    appointment_time = db.Column(db.Time, nullable=False)
    symptoms = db.Column(db.Text)
    booking_type = db.Column(db.String(20), nullable=False, default="NORMAL", index=True)
    amount = db.Column(db.Float, nullable=False, default=0)
    status = db.Column(db.String(30), nullable=False, default="OTP_PENDING", index=True)
    otp_verified_at = db.Column(db.DateTime, index=True)
    reservation_expires_at = db.Column(db.DateTime, index=True)
    payment_provider = db.Column(db.String(40))
    payment_order_id = db.Column(db.String(120), index=True)
    payment_id = db.Column(db.String(120), index=True)
    payment_verified_at = db.Column(db.DateTime, index=True)
    source_ip = db.Column(db.String(80))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PublicAppointmentDayLock(db.Model):
    """One durable row per visit date used to serialize public capacity changes.

    The row is not a counter or a patient record.  It lets PostgreSQL lock a
    deterministic record while public reservations are created/confirmed,
    preventing two simultaneous OTP requests from consuming the same final
    capacity place.  SQLite still serializes writes safely for local use.
    """

    __tablename__ = "public_appointment_day_lock"

    id = db.Column(db.Integer, primary_key=True)
    appointment_date = db.Column(db.Date, nullable=False, unique=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


# ================= CLINIC SCHEDULE =================
class ClinicLocation(db.Model):
    """A clinic branch whose public operating information can be configured."""

    __tablename__ = "clinic_location"

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(40), nullable=False, unique=True, index=True)
    display_name = db.Column(db.String(120), nullable=False, index=True)
    public_address = db.Column(db.String(255))
    public_phone = db.Column(db.String(30))
    location_type = db.Column(db.String(30), nullable=False, default="CLINIC", index=True)
    landmark = db.Column(db.String(160))
    city = db.Column(db.String(100), index=True)
    state = db.Column(db.String(100))
    postal_code = db.Column(db.String(12), index=True)
    latitude = db.Column(db.Numeric(10, 7))
    longitude = db.Column(db.Numeric(10, 7))
    maps_url = db.Column(db.String(500))
    parking_information = db.Column(db.String(500))
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    is_default = db.Column(db.Boolean, nullable=False, default=False, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Clinician(db.Model):
    """A public-facing clinician record; it does not imply a booked slot."""

    __tablename__ = "clinician"

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(40), nullable=False, unique=True, index=True)
    display_name = db.Column(db.String(120), nullable=False, index=True)
    public_title = db.Column(db.String(80))
    specialty = db.Column(db.String(120), index=True)
    qualification = db.Column(db.String(180))
    consultation_fee = db.Column(db.Numeric(10, 2))
    follow_up_fee = db.Column(db.Numeric(10, 2))
    follow_up_days = db.Column(db.Integer)
    public_bio = db.Column(db.String(500))
    # Admin-approved public JSON arrays of short text values. Kept as text
    # columns (matching ClinicProfile.available_services_json) instead of a
    # separate lookup table, since these are simple caller-facing lists.
    sub_specialties_json = db.Column(db.Text, nullable=False, default="[]")
    languages_json = db.Column(db.Text, nullable=False, default="[]")
    conditions_treated_json = db.Column(db.Text, nullable=False, default="[]")
    services_offered_json = db.Column(db.Text, nullable=False, default="[]")
    default_location_id = db.Column(
        db.Integer,
        db.ForeignKey("clinic_location.id", ondelete="SET NULL"),
        index=True,
    )
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ClinicScheduleRule(db.Model):
    """An admin-managed recurring public schedule rule.

    A rule with no clinician applies to the whole location. These rules expose
    an arrival window and capacity only; they never create individual patient
    time slots.
    """

    __tablename__ = "clinic_schedule_rule"

    id = db.Column(db.Integer, primary_key=True)
    location_id = db.Column(
        db.Integer,
        db.ForeignKey("clinic_location.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    clinician_id = db.Column(
        db.Integer,
        db.ForeignKey("clinician.id", ondelete="RESTRICT"),
        index=True,
    )
    weekday = db.Column(db.Integer, nullable=False, index=True)  # Monday=0 ... Sunday=6
    booking_enabled = db.Column(db.Boolean, nullable=False, default=True, index=True)
    arrival_window_start = db.Column(db.String(5), nullable=False, default="17:30")
    arrival_window_end = db.Column(db.String(5), nullable=False, default="19:45")
    normal_daily_limit = db.Column(db.Integer)
    priority_daily_limit = db.Column(db.Integer)
    slot_duration_minutes = db.Column(db.Integer, nullable=False, default=20)
    max_patients_per_slot = db.Column(db.Integer, nullable=False, default=1)
    # True (FIXED_SLOT): the arrival window is divided into discrete bookable
    # times. False (ARRIVAL_WINDOW): the window is one FCFS capacity pool with
    # no individual promised time. Shared by public, admin and AI booking.
    individual_time_slots = db.Column(db.Boolean, nullable=False, default=True, index=True)
    effective_from = db.Column(db.Date, index=True)
    effective_to = db.Column(db.Date, index=True)
    public_note = db.Column(db.String(240))
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ClinicScheduleException(db.Model):
    """A date-specific closure, leave, holiday, or approved schedule override.

    ``public_note`` must be a patient-safe explanation. Internal leave reasons
    are intentionally not stored in this public-call configuration table.
    """

    __tablename__ = "clinic_schedule_exception"

    id = db.Column(db.Integer, primary_key=True)
    location_id = db.Column(
        db.Integer,
        db.ForeignKey("clinic_location.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    clinician_id = db.Column(
        db.Integer,
        db.ForeignKey("clinician.id", ondelete="RESTRICT"),
        index=True,
    )
    schedule_date = db.Column(db.Date, nullable=False, index=True)
    exception_type = db.Column(db.String(20), nullable=False, index=True)
    booking_enabled = db.Column(db.Boolean, index=True)
    arrival_window_start = db.Column(db.String(5))
    arrival_window_end = db.Column(db.String(5))
    normal_daily_limit = db.Column(db.Integer)
    priority_daily_limit = db.Column(db.Integer)
    slot_duration_minutes = db.Column(db.Integer)
    max_patients_per_slot = db.Column(db.Integer)
    public_note = db.Column(db.String(240))
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ================= STOCK HISTORY =================
class StockHistory(db.Model):
    __tablename__ = "stock_history"

    id = db.Column(db.Integer, primary_key=True)

    medicine_id = db.Column(db.Integer)
    medicine_name = db.Column(db.String(150), index=True)
    batch = db.Column(db.String(50), index=True)

    action = db.Column(db.String(20))
    # ADD, SALE, RETURN, ADJUST

    qty_change = db.Column(db.Integer)      # + / -
    stock_before = db.Column(db.Integer)
    stock_after = db.Column(db.Integer)

    remark = db.Column(db.String(255))
    user = db.Column(db.String(50))
    ref_table = db.Column(db.String(50))
    ref_id = db.Column(db.Integer)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


# ================= VENDOR =================
class Vendor(db.Model):
    __tablename__ = "vendor"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(150), nullable=False, index=True)
    mobile = db.Column(db.String(20), index=True)
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
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    deleted_at = db.Column(db.DateTime, index=True)
    deleted_by = db.Column(db.String(50))


# ================= VENDOR PURCHASE =================
class VendorPurchase(db.Model):
    __tablename__ = "vendor_purchase"

    id = db.Column(db.Integer, primary_key=True)
    vendor_id = db.Column(db.Integer, nullable=False)
    purchase_no = db.Column(db.String(30), unique=True)
    invoice_no = db.Column(db.String(60), index=True)
    bill_attachment_ref = db.Column(db.String(255))
    purchase_date = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    payment_mode = db.Column(db.String(30))
    payment_status = db.Column(db.String(30))
    paid_amount = db.Column(db.Float, default=0)
    notes = db.Column(db.Text)
    subtotal = db.Column(db.Float, default=0)
    gst_total = db.Column(db.Float, default=0)
    discount_total = db.Column(db.Float, default=0)
    total_amount = db.Column(db.Float, default=0)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class VendorPurchaseItem(db.Model):
    __tablename__ = "vendor_purchase_item"

    id = db.Column(db.Integer, primary_key=True)
    purchase_id = db.Column(db.Integer, nullable=False, index=True)
    vendor_id = db.Column(db.Integer, nullable=False)
    medicine_id = db.Column(db.Integer)
    medicine_name = db.Column(db.String(150), index=True)
    medicine_code = db.Column(db.String(40), index=True)
    barcode = db.Column(db.String(80), index=True)
    composition = db.Column(db.String(255))
    company = db.Column(db.String(150))
    distributor_name = db.Column(db.String(150))
    pack_type = db.Column(db.String(50))
    pack_qty = db.Column(db.Integer)
    batch = db.Column(db.String(50), index=True)
    expiry = db.Column(db.String(10))
    qty = db.Column(db.Integer, default=0)
    free_qty = db.Column(db.Integer, default=0)
    remaining_qty = db.Column(db.Integer, default=0)
    purchase_rate = db.Column(db.Float, default=0)
    mrp = db.Column(db.Float, default=0)
    gst_percent = db.Column(db.Float, default=0)
    discount_percent = db.Column(db.Float, default=0)
    notes = db.Column(db.Text)
    total_value = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


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
    user = db.Column(db.String(50), index=True)
    action = db.Column(db.String(255), index=True)
    entity_type = db.Column(db.String(50))
    entity_id = db.Column(db.Integer)
    ref_code = db.Column(db.String(120))
    before_json = db.Column(db.Text)
    after_json = db.Column(db.Text)
    extra_json = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class LoginSecurityEvent(db.Model):
    __tablename__ = "login_security_event"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), index=True)
    user_id = db.Column(db.Integer, index=True)
    ip_address = db.Column(db.String(80), index=True)
    user_agent = db.Column(db.String(255))
    outcome = db.Column(db.String(30), index=True)
    reason = db.Column(db.String(255))
    is_suspicious = db.Column(db.Boolean, default=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


# ================= AI CALL RECEPTIONIST =================
class VoiceCall(db.Model):
    """Minimum necessary state for one incoming AI-reception call.

    The source phone number is deliberately not stored here. The telephony
    boundary persists only a keyed fingerprint and last four digits, so a call
    can be correlated for support without turning the call log into a patient
    directory. Audio, transcript, OTP, and report contents do not belong in
    this table.
    """

    __tablename__ = "voice_call"
    __table_args__ = (
        db.UniqueConstraint(
            "provider", "provider_call_id", name="uq_voice_call_provider_call_id"
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    provider = db.Column(db.String(64), nullable=False, index=True)
    provider_call_id = db.Column(db.String(128), nullable=False, index=True)
    caller_fingerprint = db.Column(db.String(64), index=True)
    caller_last4 = db.Column(db.String(4))
    direction = db.Column(db.String(16), nullable=False, default="INBOUND", index=True)
    status = db.Column(db.String(32), nullable=False, default="RECEIVED", index=True)
    language = db.Column(db.String(12), index=True)
    current_intent = db.Column(db.String(80), index=True)
    current_stage = db.Column(db.String(80), index=True)
    # This contains only allow-listed state such as a selected date or opaque
    # internal identifier. It must never contain caller speech, a name, mobile
    # number, OTP, report result, or recording URL.
    context_json = db.Column(db.Text)
    patient_id = db.Column(db.Integer, index=True)
    public_booking_id = db.Column(db.Integer, index=True)
    verified_at = db.Column(db.DateTime, index=True)
    transfer_status = db.Column(db.String(32), index=True)
    outcome = db.Column(db.String(48), index=True)
    error_code = db.Column(db.String(80), index=True)
    started_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    ended_at = db.Column(db.DateTime, index=True)
    duration_seconds = db.Column(db.Integer)
    last_event_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class VoiceCallEvent(db.Model):
    """Append-only, idempotent provider-event inbox for an incoming call."""

    __tablename__ = "voice_call_event"
    __table_args__ = (
        db.UniqueConstraint(
            "provider", "provider_event_id", name="uq_voice_call_event_provider_event_id"
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    voice_call_id = db.Column(
        db.Integer,
        db.ForeignKey("voice_call.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    provider = db.Column(db.String(64), nullable=False, index=True)
    provider_event_id = db.Column(db.String(128), nullable=False, index=True)
    event_type = db.Column(db.String(64), nullable=False, index=True)
    occurred_at = db.Column(db.DateTime, nullable=False, index=True)
    # Keyed fingerprint of normalized, non-sensitive event facts. This is not
    # a plain hash of the raw provider body, which could contain caller data.
    event_fingerprint = db.Column(db.String(64), nullable=False, index=True)
    sequence_number = db.Column(db.Integer)
    processing_status = db.Column(db.String(32), nullable=False, default="ACCEPTED", index=True)
    error_code = db.Column(db.String(80), index=True)
    received_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)
    processed_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


# ================= AI INTEGRATION API =================
class AIAPIClient(db.Model):
    """A machine identity allowed to call the versioned AI integration API.

    Only a slow password hash of the client secret is stored. ``client_id`` is
    public identification material; it is intentionally separate from the
    numeric database primary key used by relationships and audit records.
    """

    __tablename__ = "ai_api_client"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    client_id = db.Column(db.String(80), nullable=False, unique=True, index=True)
    secret_hash = db.Column(db.String(512), nullable=False)
    allowed_scopes_json = db.Column(db.Text, nullable=False, default="[]")
    allowed_ips_json = db.Column(db.Text, nullable=False, default="[]")
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    secret_version = db.Column(db.Integer, nullable=False, default=1)
    last_used_at = db.Column(db.DateTime, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class AIAccessToken(db.Model):
    """Short-lived opaque bearer token; the raw token is never persisted."""

    __tablename__ = "ai_access_token"

    id = db.Column(db.Integer, primary_key=True)
    api_client_id = db.Column(
        db.Integer,
        db.ForeignKey("ai_api_client.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    token_hash = db.Column(db.String(64), nullable=False, unique=True, index=True)
    scopes_json = db.Column(db.Text, nullable=False, default="[]")
    secret_version = db.Column(db.Integer, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    revoked_at = db.Column(db.DateTime, index=True)
    last_used_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class AIAPIRequestAudit(db.Model):
    """Privacy-minimized request metadata for support and security review."""

    __tablename__ = "ai_api_request_audit"

    id = db.Column(db.Integer, primary_key=True)
    api_client_id = db.Column(
        db.Integer,
        db.ForeignKey("ai_api_client.id", ondelete="SET NULL"),
        index=True,
    )
    request_id = db.Column(db.String(80), nullable=False, index=True)
    call_id = db.Column(db.String(128), index=True)
    session_id = db.Column(db.String(128), index=True)
    method = db.Column(db.String(10), nullable=False)
    endpoint = db.Column(db.String(255), nullable=False, index=True)
    status_code = db.Column(db.Integer, nullable=False, index=True)
    outcome = db.Column(db.String(20), nullable=False, index=True)
    error_code = db.Column(db.String(80), index=True)
    action = db.Column(db.String(80), index=True)
    resource_type = db.Column(db.String(50), index=True)
    resource_id = db.Column(db.String(80), index=True)
    latency_ms = db.Column(db.Integer, nullable=False, default=0)
    client_ip_fingerprint = db.Column(db.String(64), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class AIIdempotencyRecord(db.Model):
    """Reusable replay protection for consequential AI API operations."""

    __tablename__ = "ai_idempotency_record"
    __table_args__ = (
        db.UniqueConstraint(
            "api_client_id",
            "operation",
            "idempotency_key_hash",
            name="uq_ai_idempotency_client_operation_key",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    api_client_id = db.Column(
        db.Integer,
        db.ForeignKey("ai_api_client.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    operation = db.Column(db.String(80), nullable=False, index=True)
    idempotency_key_hash = db.Column(db.String(64), nullable=False, index=True)
    request_fingerprint = db.Column(db.String(64), nullable=False)
    state = db.Column(db.String(20), nullable=False, default="IN_PROGRESS", index=True)
    response_status = db.Column(db.Integer)
    response_json = db.Column(db.Text)
    resource_type = db.Column(db.String(50))
    resource_id = db.Column(db.String(80), index=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class OmnidimGatewayAction(db.Model):
    """Replay protection for the narrow OmniDimension voice gateway.

    The table intentionally contains only keyed hashes and the minimal result
    needed to replay a successful request.  Patient names, phone numbers,
    complaint text and call transcripts remain in their appropriate business
    records and are never copied into gateway request storage.
    """

    __tablename__ = "omnidim_gateway_action"
    __table_args__ = (
        db.UniqueConstraint(
            "operation",
            "idempotency_key_hash",
            name="uq_omnidim_gateway_action_key",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    operation = db.Column(db.String(80), nullable=False, index=True)
    idempotency_key_hash = db.Column(db.String(64), nullable=False, index=True)
    request_fingerprint = db.Column(db.String(64), nullable=False)
    state = db.Column(db.String(20), nullable=False, default="IN_PROGRESS", index=True)
    response_json = db.Column(db.Text)
    response_status = db.Column(db.Integer)
    resource_type = db.Column(db.String(50), index=True)
    resource_id = db.Column(db.String(80), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    completed_at = db.Column(db.DateTime, index=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class ClinicProfile(db.Model):
    """Structured, admin-approved public clinic information."""

    __tablename__ = "clinic_profile"

    id = db.Column(db.Integer, primary_key=True)
    clinic_name = db.Column(db.String(160), nullable=False)
    phone = db.Column(db.String(30))
    email = db.Column(db.String(160))
    reception_phone = db.Column(db.String(30))
    website_url = db.Column(db.String(500))
    emergency_wording = db.Column(db.String(500))
    consultation_information = db.Column(db.Text)
    general_policies = db.Column(db.Text)
    payment_methods_json = db.Column(db.Text, nullable=False, default="[]")
    available_services_json = db.Column(db.Text, nullable=False, default="[]")
    # Compatibility column retained for installations that had the original
    # draft AI schema before the finalized ``timezone_name`` field existed.
    # Both values are written together; the public service reads
    # ``timezone_name``. Removing the legacy NOT NULL column would require a
    # destructive SQLite table rebuild, so keeping it mapped is safer.
    legacy_timezone = db.Column(
        "timezone", db.String(64), nullable=False, default="Asia/Kolkata"
    )
    timezone_name = db.Column(db.String(64), nullable=False, default="Asia/Kolkata")
    late_grace_minutes = db.Column(db.Integer, nullable=False, default=15)
    auto_reschedule_after_minutes = db.Column(db.Integer)
    late_staff_notification_required = db.Column(db.Boolean, nullable=False, default=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class AppointmentSlotLock(db.Model):
    """Deterministic row locked while one clinician slot is booked."""

    __tablename__ = "appointment_slot_lock"
    __table_args__ = (
        db.UniqueConstraint(
            "clinician_id",
            "location_id",
            "appointment_date",
            "slot_time",
            name="uq_appointment_slot_lock_identity",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    clinician_id = db.Column(db.Integer, nullable=False, index=True)
    location_id = db.Column(db.Integer, nullable=False, index=True)
    appointment_date = db.Column(db.Date, nullable=False, index=True)
    slot_time = db.Column(db.Time, nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class AppointmentSlotBlock(db.Model):
    __tablename__ = "appointment_slot_block"

    id = db.Column(db.Integer, primary_key=True)
    clinician_id = db.Column(db.Integer, db.ForeignKey("clinician.id"), index=True)
    location_id = db.Column(db.Integer, db.ForeignKey("clinic_location.id"), index=True)
    block_date = db.Column(db.Date, nullable=False, index=True)
    start_time = db.Column(db.Time, index=True)
    end_time = db.Column(db.Time, index=True)
    all_day = db.Column(db.Boolean, nullable=False, default=False, index=True)
    public_reason = db.Column(db.String(240))
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class AppointmentWaitlist(db.Model):
    __tablename__ = "appointment_waitlist"

    id = db.Column(db.Integer, primary_key=True)
    waitlist_ref = db.Column(db.String(40), nullable=False, unique=True, index=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), nullable=False, index=True)
    clinician_id = db.Column(db.Integer, db.ForeignKey("clinician.id"), nullable=False, index=True)
    location_id = db.Column(db.Integer, db.ForeignKey("clinic_location.id"), nullable=False, index=True)
    preferred_date = db.Column(db.Date, nullable=False, index=True)
    preferred_start_time = db.Column(db.Time)
    preferred_end_time = db.Column(db.Time)
    status = db.Column(db.String(30), nullable=False, default="WAITING", index=True)
    source = db.Column(db.String(30), nullable=False, default="AI_CALL", index=True)
    external_call_id = db.Column(db.String(128), index=True)
    external_session_id = db.Column(db.String(128), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class PatientVerificationSession(db.Model):
    """Server-owned authorization after successful OTP verification."""

    __tablename__ = "patient_verification_session"

    id = db.Column(db.Integer, primary_key=True)
    token_hash = db.Column(db.String(64), nullable=False, unique=True, index=True)
    api_client_id = db.Column(db.Integer, db.ForeignKey("ai_api_client.id"), nullable=False, index=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), nullable=False, index=True)
    mobile = db.Column(db.String(20), nullable=False, index=True)
    purpose = db.Column(db.String(40), nullable=False, default="AI_PATIENT_ACCESS", index=True)
    otp_challenge_id = db.Column(db.Integer, db.ForeignKey("portal_otp_challenge.id"), index=True)
    external_call_id = db.Column(db.String(128), index=True)
    external_session_id = db.Column(db.String(128), index=True)
    # Kept for compatibility with earlier clinic databases where a verified
    # session requires this timestamp. New sessions always set it explicitly
    # in the OTP verification service below.
    verified_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    last_used_at = db.Column(db.DateTime, index=True)
    revoked_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class PatientVerificationIntent(db.Model):
    """Opaque, short-lived bridge between identification and OTP delivery."""

    __tablename__ = "patient_verification_intent"

    id = db.Column(db.Integer, primary_key=True)
    reference_hash = db.Column(db.String(64), nullable=False, unique=True, index=True)
    api_client_id = db.Column(db.Integer, db.ForeignKey("ai_api_client.id"), nullable=False, index=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), index=True)
    mobile = db.Column(db.String(20), nullable=False, index=True)
    claimed_name = db.Column(db.String(120))
    external_call_id = db.Column(db.String(128), index=True)
    external_session_id = db.Column(db.String(128), index=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    used_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class SecureDocumentToken(db.Model):
    __tablename__ = "secure_document_token"

    id = db.Column(db.Integer, primary_key=True)
    token_hash = db.Column(db.String(64), nullable=False, unique=True, index=True)
    report_id = db.Column(db.Integer, db.ForeignKey("lab_report.id"), nullable=False, index=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), nullable=False, index=True)
    verification_session_id = db.Column(
        db.Integer, db.ForeignKey("patient_verification_session.id"), nullable=False, index=True
    )
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    max_downloads = db.Column(db.Integer, nullable=False, default=1)
    download_count = db.Column(db.Integer, nullable=False, default=0)
    last_downloaded_at = db.Column(db.DateTime, index=True)
    revoked_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class ClinicKnowledgeEntry(db.Model):
    __tablename__ = "clinic_knowledge_entry"

    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(80), nullable=False, index=True)
    question = db.Column(db.String(240), nullable=False, index=True)
    answer = db.Column(db.Text, nullable=False)
    language = db.Column(db.String(12), nullable=False, default="en", index=True)
    keywords_json = db.Column(db.Text, nullable=False, default="[]")
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class ReceptionSchedule(db.Model):
    __tablename__ = "reception_schedule"

    id = db.Column(db.Integer, primary_key=True)
    location_id = db.Column(db.Integer, db.ForeignKey("clinic_location.id"), nullable=False, index=True)
    weekday = db.Column(db.Integer, nullable=False, index=True)
    is_open = db.Column(db.Boolean, nullable=False, default=True, index=True)
    open_time = db.Column(db.Time)
    close_time = db.Column(db.Time)
    public_note = db.Column(db.String(240))
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class ReceptionScheduleOverride(db.Model):
    __tablename__ = "reception_schedule_override"

    id = db.Column(db.Integer, primary_key=True)
    location_id = db.Column(db.Integer, db.ForeignKey("clinic_location.id"), nullable=False, index=True)
    schedule_date = db.Column(db.Date, nullable=False, index=True)
    is_open = db.Column(db.Boolean, nullable=False, default=False, index=True)
    open_time = db.Column(db.Time)
    close_time = db.Column(db.Time)
    public_note = db.Column(db.String(240))
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


class CallbackRequest(db.Model):
    __tablename__ = "callback_request"

    id = db.Column(db.Integer, primary_key=True)
    callback_ref = db.Column(db.String(40), nullable=False, unique=True, index=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), index=True)
    caller_name = db.Column(db.String(120))
    mobile = db.Column(db.String(20), nullable=False, index=True)
    reason = db.Column(db.String(500), nullable=False)
    category = db.Column(db.String(80), index=True)
    priority = db.Column(db.String(20), nullable=False, default="NORMAL", index=True)
    ai_summary = db.Column(db.String(1000))
    preferred_at = db.Column(db.DateTime, index=True)
    status = db.Column(db.String(30), nullable=False, default="PENDING", index=True)
    source = db.Column(db.String(30), nullable=False, default="AI_CALL", index=True)
    external_call_id = db.Column(db.String(128), index=True)
    external_session_id = db.Column(db.String(128), index=True)
    assigned_to = db.Column(db.String(50), index=True)
    resolution_note = db.Column(db.String(500))
    contacted_at = db.Column(db.DateTime, index=True)
    completed_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class Complaint(db.Model):
    __tablename__ = "complaint"

    id = db.Column(db.Integer, primary_key=True)
    complaint_ref = db.Column(db.String(40), nullable=False, unique=True, index=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), index=True)
    caller_name = db.Column(db.String(120))
    mobile = db.Column(db.String(20), nullable=False, index=True)
    category = db.Column(db.String(80), nullable=False, index=True)
    summary = db.Column(db.String(240), nullable=False)
    details = db.Column(db.Text)
    priority = db.Column(db.String(20), nullable=False, default="NORMAL", index=True)
    status = db.Column(db.String(30), nullable=False, default="OPEN", index=True)
    source = db.Column(db.String(30), nullable=False, default="AI_CALL", index=True)
    external_call_id = db.Column(db.String(128), index=True)
    external_session_id = db.Column(db.String(128), index=True)
    assigned_to = db.Column(db.String(50), index=True)
    resolution_note = db.Column(db.String(1000))
    resolved_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class NotificationTemplate(db.Model):
    __tablename__ = "notification_template"
    __table_args__ = (
        db.UniqueConstraint(
            "event_code", "channel", "language", name="uq_notification_template_identity"
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    event_code = db.Column(db.String(80), nullable=False, index=True)
    channel = db.Column(db.String(20), nullable=False, index=True)
    language = db.Column(db.String(12), nullable=False, default="en", index=True)
    body_template = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class NotificationDelivery(db.Model):
    __tablename__ = "notification_delivery"
    __table_args__ = (
        db.UniqueConstraint(
            "api_client_id",
            "idempotency_key_hash",
            name="uq_notification_delivery_client_idempotency",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    api_client_id = db.Column(db.Integer, db.ForeignKey("ai_api_client.id"), nullable=False, index=True)
    template_id = db.Column(db.Integer, db.ForeignKey("notification_template.id"), nullable=False, index=True)
    channel = db.Column(db.String(20), nullable=False, index=True)
    recipient_masked = db.Column(db.String(30), nullable=False)
    recipient_fingerprint = db.Column(db.String(64), nullable=False, index=True)
    idempotency_key_hash = db.Column(db.String(64), nullable=False, index=True)
    request_fingerprint = db.Column(db.String(64), nullable=False)
    status = db.Column(db.String(30), nullable=False, default="PENDING", index=True)
    provider = db.Column(db.String(40))
    provider_reference = db.Column(db.String(120), index=True)
    error_code = db.Column(db.String(80), index=True)
    sent_at = db.Column(db.DateTime, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
