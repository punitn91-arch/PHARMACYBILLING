# app.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory, g
from datetime import datetime, time, timedelta, date, timezone
from functools import wraps
import json
import webbrowser
import threading
import os
import secrets
from decimal import Decimal, ROUND_HALF_UP
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None
from werkzeug.utils import secure_filename
from sqlalchemy import text, or_, and_, inspect, cast, String
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
try:
    from flask_migrate import Migrate
except Exception:  # pragma: no cover
    Migrate = None

try:
    from .models import (
        db,
        User,
        Medicine,
        StockHistory,
        Invoice,
        InvoiceItem,
        Return,
        ReturnItem,
        HoldBill,
        Patient,
        Appointment,
        Vendor,
        VendorPurchase,
        VendorPurchaseItem,
        SalesAllocation,
        VendorNote,
        VendorNoteItem,
        VendorNoteAllocation,
        VendorLedgerEntry,
        AuditLog,
        LoginSecurityEvent,
    )
    from .routes.appointments import mark_appointment_paid as handle_mark_appointment_paid
    from .routes.appointments import render_appointments_page
    from .routes.billing import prepare_billing_context
    from .routes.reports import render_reports_page
    from .routes.vendors import render_vendor_reports_page
    from .services.background_jobs import init_background_jobs, queue_report_export_job
    from .services.infra_safety import (
        build_backup_snapshot,
        build_restore_commands,
        ensure_runtime_indexes,
        get_backup_summary,
        list_backup_snapshots,
        restore_backup_snapshot,
    )
    from .services.monitoring import configure_monitoring
    from .services.validation import (
        ACCESS_PROFILE_PRESETS,
        USER_PERMISSION_FIELDS,
        derive_access_profile,
        validate_billing_submission,
        validate_mark_paid_transition,
        validate_report_request,
        validate_return_submission,
        validate_user_form,
    )
except ImportError:  # pragma: no cover - script/local fallback
    from models import (
        db,
        User,
        Medicine,
        StockHistory,
        Invoice,
        InvoiceItem,
        Return,
        ReturnItem,
        HoldBill,
        Patient,
        Appointment,
        Vendor,
        VendorPurchase,
        VendorPurchaseItem,
        SalesAllocation,
        VendorNote,
        VendorNoteItem,
        VendorNoteAllocation,
        VendorLedgerEntry,
        AuditLog,
        LoginSecurityEvent,
    )
    from routes.appointments import mark_appointment_paid as handle_mark_appointment_paid
    from routes.appointments import render_appointments_page
    from routes.billing import prepare_billing_context
    from routes.reports import render_reports_page
    from routes.vendors import render_vendor_reports_page
    from services.background_jobs import init_background_jobs, queue_report_export_job
    from services.infra_safety import (
        build_backup_snapshot,
        build_restore_commands,
        ensure_runtime_indexes,
        get_backup_summary,
        list_backup_snapshots,
        restore_backup_snapshot,
    )
    from services.monitoring import configure_monitoring
    from services.validation import (
        ACCESS_PROFILE_PRESETS,
        USER_PERMISSION_FIELDS,
        derive_access_profile,
        validate_billing_submission,
        validate_mark_paid_transition,
        validate_report_request,
        validate_return_submission,
        validate_user_form,
    )

def open_browser():
    try:
        webbrowser.open("http://127.0.0.1:5000")
    except Exception:
        pass

IS_PROD = bool(
    os.environ.get("RAILWAY_ENVIRONMENT")
    or os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    or os.environ.get("RENDER")
    or os.environ.get("FLY_APP_NAME")
    or os.environ.get("VERCEL")
    or (os.environ.get("VERCEL_ENV") or "").lower() == "production"
)
IS_SERVERLESS = bool(os.environ.get("VERCEL") or os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

if (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    and not IS_PROD
    and str(os.environ.get("AUTO_OPEN_BROWSER", "1")).strip().lower() not in {"0", "false", "no", "off"}
):
    threading.Timer(1, open_browser).start()


APP_TIMEZONE = (os.environ.get("APP_TIMEZONE") or "Asia/Kolkata").strip() or "Asia/Kolkata"
APP_ENV = (os.environ.get("APP_ENV") or ("production" if IS_PROD else "local")).strip().lower() or "local"


def clinic_now():
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(APP_TIMEZONE))
        except Exception:
            pass
    return datetime.now()


APP_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(
    __name__,
    template_folder=os.path.join(APP_BASE_DIR, "templates"),
    static_folder=os.path.join(APP_BASE_DIR, "static"),
    static_url_path="/static"
)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
try:
    max_upload_mb = max(1, int(str(os.environ.get("MAX_CONTENT_LENGTH_MB", "12")).strip()))
except (TypeError, ValueError):
    max_upload_mb = 12
try:
    session_idle_minutes = max(5, int(str(os.environ.get("SESSION_IDLE_MINUTES", "90")).strip()))
except (TypeError, ValueError):
    session_idle_minutes = 90
try:
    session_absolute_hours = max(1, int(str(os.environ.get("SESSION_ABSOLUTE_HOURS", "24")).strip()))
except (TypeError, ValueError):
    session_absolute_hours = 24
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = IS_PROD
app.config["PREFERRED_URL_SCHEME"] = "https" if IS_PROD else "http"
app.config["MAX_CONTENT_LENGTH"] = max_upload_mb * 1024 * 1024
app.config["JSON_SORT_KEYS"] = False
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=session_absolute_hours)
app.config["APP_ENV"] = APP_ENV

ASYNC_REDIRECT_STATUSES = {301, 302, 303, 307, 308}

TMP_BASE_DIR = os.path.join("/tmp", "billingwebapp")


def ensure_writable_dir(preferred_path, fallback_path):
    try:
        os.makedirs(preferred_path, exist_ok=True)
        return preferred_path
    except OSError:
        os.makedirs(fallback_path, exist_ok=True)
        return fallback_path


def env_flag(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def normalize_database_url(raw_url):
    db_url = (raw_url or "").strip()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return db_url


def resolve_database_url(default_db):
    for env_name in ("DATABASE_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL"):
        candidate = normalize_database_url(os.environ.get(env_name))
        if candidate:
            return candidate
    if IS_PROD:
        raise RuntimeError(
            "Database URL is missing. Set DATABASE_URL or POSTGRES_URL in environment variables."
        )
    return default_db

# ---------------- UPLOADS ----------------
UPLOAD_STORAGE_ROOT = os.path.abspath(
    (os.environ.get("APP_STORAGE_ROOT") or os.path.join(APP_BASE_DIR, "static", "uploads")).strip()
)


def resolve_upload_dir(*parts):
    return ensure_writable_dir(
        os.path.join(UPLOAD_STORAGE_ROOT, *parts),
        os.path.join(TMP_BASE_DIR, "uploads", *parts)
    )


UPLOAD_FOLDER = resolve_upload_dir("vendors")
VENDOR_BILL_UPLOAD_FOLDER = resolve_upload_dir("vendor_bills")
BACKUP_ROOT = os.path.abspath(
    (os.environ.get("APP_BACKUP_ROOT") or os.path.join(app.instance_path, "backups")).strip()
)
app.config["APP_BASE_DIR"] = APP_BASE_DIR
app.config["APP_STORAGE_ROOT"] = UPLOAD_STORAGE_ROOT
app.config["BACKUP_ROOT"] = BACKUP_ROOT
app.config["INFRA_UPLOAD_DIRECTORIES"] = {
    "vendor_uploads": UPLOAD_FOLDER,
    "vendor_bill_uploads": VENDOR_BILL_UPLOAD_FOLDER,
}
ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def save_uploaded_file(file_storage, upload_dir, prefix):
    if not file_storage or not (file_storage.filename or "").strip():
        return ""
    if not allowed_file(file_storage.filename):
        raise ValueError("Only PDF, JPG and PNG files are allowed")
    original_name = secure_filename(file_storage.filename)
    name_root, ext = os.path.splitext(original_name)
    safe_root = secure_filename(name_root)[:80] or "file"
    stamp = clinic_now().strftime("%Y%m%d%H%M%S")
    token = secrets.token_hex(4)
    saved_name = f"{prefix}_{stamp}_{token}_{safe_root}{ext.lower()}"
    file_storage.save(os.path.join(upload_dir, saved_name))
    return saved_name


def delete_uploaded_file(upload_dir, filename):
    cleaned_name = (filename or "").strip()
    if not cleaned_name:
        return
    file_path = os.path.join(upload_dir, cleaned_name)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except OSError:
        pass

def is_async_request():
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"

@app.after_request
def adapt_redirect_for_async(response):
    if not is_async_request():
        final_response = response
    elif response.status_code not in ASYNC_REDIRECT_STATUSES:
        final_response = response
    else:
        flashes = session.get("_flashes", []) or []
        messages = []
        has_error = False
        for category, message in flashes:
            cat = str(category or "info").lower()
            msg = str(message or "").strip()
            if not msg:
                continue
            if cat in ("danger", "error"):
                has_error = True
            messages.append({
                "category": cat,
                "message": msg
            })

        if "_flashes" in session:
            session.pop("_flashes", None)

        payload = {
            "ok": not has_error,
            "redirect_url": response.headers.get("Location"),
            "messages": messages
        }
        async_response = jsonify(payload)
        async_response.status_code = 200
        final_response = async_response

    # Ensure browsers/CDNs don't keep serving stale HTML after redeploy.
    if request.method == "GET" and str(final_response.mimetype or "").lower() == "text/html":
        final_response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        final_response.headers["Pragma"] = "no-cache"
        final_response.headers["Expires"] = "0"

    final_response.headers["X-App-Release"] = os.environ.get("RAILWAY_GIT_COMMIT_SHA", "local")
    return final_response

def to_int(val):
    return int(val) if val not in (None, "", " ") else 0

def to_float(val):
    return float(val) if val not in (None, "", " ") else 0.0

def is_cash_payment_mode(value):
    return ((value or "CASH").strip().upper() or "CASH") == "CASH"

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


def normalize_medicine_name(value):
    return " ".join(str(value or "").strip().split())


def build_medicine_name_suggestions(medicines):
    unique_names = {}
    for med in medicines:
        cleaned_name = normalize_medicine_name(getattr(med, "name", ""))
        if not cleaned_name:
            continue
        unique_names.setdefault(cleaned_name.casefold(), cleaned_name)
    return sorted(unique_names.values(), key=str.lower)

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

def serialize_json_text(payload):
    if payload in (None, "", [], {}):
        return ""
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return json.dumps({"value": str(payload)}, ensure_ascii=False, sort_keys=True)

def parse_json_text(payload):
    raw = (payload or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw}


def get_release_identifier():
    return (
        os.environ.get("RAILWAY_GIT_COMMIT_SHA")
        or os.environ.get("RENDER_GIT_COMMIT")
        or os.environ.get("VERCEL_GIT_COMMIT_SHA")
        or os.environ.get("SOURCE_VERSION")
        or "local"
    )


def mask_database_target(engine_url):
    try:
        backend = engine_url.get_backend_name()
    except Exception:
        return "unknown"

    if backend == "sqlite":
        database_name = os.path.basename(engine_url.database or "") or "pharmacy.db"
        return database_name

    host = (getattr(engine_url, "host", "") or "").strip()
    port = getattr(engine_url, "port", None)
    database_name = (getattr(engine_url, "database", "") or "").strip()
    host_part = host or backend
    if port:
        host_part = f"{host_part}:{port}"
    if database_name:
        return f"{host_part}/{database_name}"
    return host_part


def format_bytes(size_value):
    size = float(size_value or 0)
    units = ("B", "KB", "MB", "GB", "TB")
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    return f"{size:.2f} {units[unit_index]}"


def summarize_directory(path_value):
    summary = {
        "path": path_value,
        "available": os.path.isdir(path_value),
        "file_count": 0,
        "total_bytes": 0,
        "total_size": "0 B",
        "latest_file_at": None
    }
    if not summary["available"]:
        return summary

    latest_mtime = None
    for root, _dirs, files in os.walk(path_value):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                stat_result = os.stat(file_path)
            except OSError:
                continue
            summary["file_count"] += 1
            summary["total_bytes"] += int(stat_result.st_size or 0)
            if latest_mtime is None or stat_result.st_mtime > latest_mtime:
                latest_mtime = stat_result.st_mtime
    summary["total_size"] = format_bytes(summary["total_bytes"])
    if latest_mtime is not None:
        summary["latest_file_at"] = datetime.fromtimestamp(latest_mtime).strftime("%d-%m-%Y %I:%M %p")
    return summary


def current_client_ip():
    forwarded = (request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.headers.get("X-Real-IP") or request.remote_addr or "").strip() or "unknown"


def current_user_agent():
    return (request.headers.get("User-Agent") or "").strip()[:255]


def _normalized_boolish_column(column):
    return db.func.lower(db.func.trim(cast(column, String)))


def truthy_column_expr(column):
    return _normalized_boolish_column(column).in_(("1", "true", "t", "yes", "on"))


def falsey_or_null_column_expr(column):
    normalized = _normalized_boolish_column(column)
    return or_(column.is_(None), normalized.in_(("0", "false", "f", "no", "off", "")))


def active_user_query():
    return User.query.filter(User.deleted_at.is_(None), truthy_column_expr(User.is_active))


def active_vendor_query():
    return Vendor.query.filter(Vendor.deleted_at.is_(None))


def active_hold_bill_query():
    return HoldBill.query.filter(falsey_or_null_column_expr(HoldBill.is_deleted))


def active_appointment_query():
    return Appointment.query.filter(falsey_or_null_column_expr(Appointment.is_deleted))


def active_user_by_id(user_id):
    if not user_id:
        return None
    return active_user_query().filter(User.id == user_id).first()


def log_login_security_event(*, username="", user=None, outcome="", reason="", is_suspicious=False):
    try:
        db.session.add(
            LoginSecurityEvent(
                username=(username or (user.username if user else "") or "").strip()[:50],
                user_id=(user.id if user else None),
                ip_address=current_client_ip(),
                user_agent=current_user_agent(),
                outcome=(outcome or "").strip().upper()[:30],
                reason=(reason or "").strip()[:255],
                is_suspicious=bool(is_suspicious),
            )
        )
        db.session.commit()
    except Exception:
        db.session.rollback()
        app.logger.exception("Failed to log login security event")


def failed_login_attempts_recent(username="", ip_address=""):
    username = (username or "").strip()
    ip_address = (ip_address or "").strip()
    window_started = datetime.utcnow() - timedelta(
        minutes=max(5, int(str(os.environ.get("SUSPICIOUS_LOGIN_WINDOW_MINUTES", "15")).strip() or 15))
    )
    query = LoginSecurityEvent.query.filter(
        LoginSecurityEvent.outcome == "FAILED",
        LoginSecurityEvent.created_at >= window_started,
    )
    filters = []
    if username:
        filters.append(LoginSecurityEvent.username == username)
    if ip_address:
        filters.append(LoginSecurityEvent.ip_address == ip_address)
    if filters:
        query = query.filter(or_(*filters))
    return query.count()


def run_system_health_checks(include_counts=False):
    release_id = get_release_identifier()
    backup_summary = get_backup_summary(app)
    suspicious_since = datetime.utcnow() - timedelta(hours=24)
    payload = {
        "status": "ok",
        "checked_at": clinic_now().isoformat(),
        "release": release_id,
        "environment": APP_ENV,
        "timezone": APP_TIMEZONE,
        "database": {
            "ok": True,
            "backend": "unknown",
            "target": "unknown",
            "latency_ms": None,
            "error": ""
        },
        "storage": {
            "vendor_uploads": summarize_directory(UPLOAD_FOLDER),
            "vendor_bill_uploads": summarize_directory(VENDOR_BILL_UPLOAD_FOLDER),
            "root_path": UPLOAD_STORAGE_ROOT
        },
        "backups": backup_summary,
        "security": {
            "suspicious_logins_24h": LoginSecurityEvent.query.filter(
                truthy_column_expr(LoginSecurityEvent.is_suspicious),
                LoginSecurityEvent.created_at >= suspicious_since,
            ).count(),
            "failed_logins_24h": LoginSecurityEvent.query.filter(
                LoginSecurityEvent.outcome == "FAILED",
                LoginSecurityEvent.created_at >= suspicious_since,
            ).count(),
        },
    }

    try:
        engine_url = db.engine.url
        payload["database"]["backend"] = engine_url.get_backend_name()
        payload["database"]["target"] = mask_database_target(engine_url)
        probe_started = datetime.utcnow()
        db.session.execute(text("SELECT 1"))
        probe_elapsed = datetime.utcnow() - probe_started
        payload["database"]["latency_ms"] = round(probe_elapsed.total_seconds() * 1000, 2)
    except Exception as exc:
        payload["status"] = "degraded"
        payload["database"]["ok"] = False
        payload["database"]["error"] = str(exc)

    if not payload["storage"]["vendor_uploads"]["available"] or not payload["storage"]["vendor_bill_uploads"]["available"]:
        payload["status"] = "degraded"

    if include_counts:
        today_local = clinic_now().date()
        payload["counts"] = {
            "users": active_user_query().count(),
            "patients": Patient.query.count(),
            "appointments": active_appointment_query().count(),
            "invoices": Invoice.query.count(),
            "returns": Return.query.count(),
            "medicines": Medicine.query.count(),
            "active_medicines": Medicine.query.filter(truthy_column_expr(Medicine.is_active)).count(),
            "low_stock_medicines": Medicine.query.filter(Medicine.qty <= Medicine.reorder_level).count(),
            "vendors": active_vendor_query().count(),
            "audit_events": AuditLog.query.count(),
            "today_invoices": Invoice.query.filter(db.func.date(Invoice.created_at) == today_local).count(),
            "today_appointments": active_appointment_query().filter(Appointment.appointment_date == today_local).count()
        }

    return payload


def build_upgrade_tracker():
    return [
        {
            "title": "Already Live In App",
            "items": [
                "Patient profile, customer directory, and linked visit history",
                "Live queue board with token flow for appointments",
                "Global search across patient, medicine, invoice, and appointment",
                "Audit logs with before/after change snapshots",
                "Medicine, profit, and appointment reporting exports",
                "Role-based access controls for medicines, reports, invoices, and users"
            ]
        },
        {
            "title": "Upgraded In This Pass",
            "items": [
                "Health and readiness endpoints for deployment monitoring",
                "Admin System Center with storage, database, and release diagnostics",
                "Automated smoke-test base for future safe changes",
                "Operational roadmap documented inside the repository"
            ]
        },
        {
            "title": "Next Pro Phases",
            "items": [
                "Patient credit ledger and due collection workflow",
                "Scheduled backups, restore drill, and release checklist",
                "WhatsApp reminder automation and refill nudges",
                "Chart-based analytics dashboard and deeper stock forecasting",
                "Modular route/service split plus formal database migrations"
            ]
        }
    ]


def storage_datetime_to_local(value):
    if not value:
        return None
    tzinfo = get_clinic_tzinfo()
    if not tzinfo:
        return value
    try:
        return value.replace(tzinfo=timezone.utc).astimezone(tzinfo)
    except Exception:
        return value


def storage_datetime_to_local_date(value):
    local_dt = storage_datetime_to_local(value)
    return local_dt.date() if local_dt else None


def build_dashboard_sales_trend(days=7):
    days = max(3, min(to_int_safe(days, 7), 31))
    end_date = clinic_now().date()
    start_date = end_date - timedelta(days=days - 1)
    start_bound, end_bound = local_date_range_to_storage_bounds(start_date, end_date)

    ordered_days = [start_date + timedelta(days=offset) for offset in range(days)]
    bucket_map = {
        day_key: {
            "date": day_key.isoformat(),
            "label": day_key.strftime("%d %b"),
            "sales": 0.0,
            "returns": 0.0,
            "net": 0.0,
            "height_pct": 0
        }
        for day_key in ordered_days
    }

    invoices = Invoice.query.filter(
        Invoice.created_at >= start_bound,
        Invoice.created_at < end_bound
    ).all()
    for invoice in invoices:
        local_day = storage_datetime_to_local_date(invoice.created_at)
        if local_day in bucket_map:
            bucket_map[local_day]["sales"] += to_float(invoice.total)

    returns = Return.query.filter(
        Return.created_at >= start_bound,
        Return.created_at < end_bound,
        falsey_or_null_column_expr(Return.is_cancelled)
    ).all()
    for return_bill in returns:
        local_day = storage_datetime_to_local_date(return_bill.created_at)
        if local_day in bucket_map:
            bucket_map[local_day]["returns"] += to_float(return_bill.total_refund)

    max_value = 0.0
    points = []
    for day_key in ordered_days:
        bucket = bucket_map[day_key]
        bucket["sales"] = round(bucket["sales"], 2)
        bucket["returns"] = round(bucket["returns"], 2)
        bucket["net"] = round(bucket["sales"] - bucket["returns"], 2)
        max_value = max(max_value, bucket["net"])
        points.append(bucket)

    for bucket in points:
        if bucket["net"] <= 0 or max_value <= 0:
            bucket["height_pct"] = 8 if bucket["net"] > 0 else 0
        else:
            bucket["height_pct"] = max(12, int(round((bucket["net"] / max_value) * 100)))
    return points


def build_dashboard_appointment_trend(days=7):
    days = max(3, min(to_int_safe(days, 7), 31))
    end_date = clinic_now().date()
    start_date = end_date - timedelta(days=days - 1)
    ordered_days = [start_date + timedelta(days=offset) for offset in range(days)]
    bucket_map = {
        day_key: {
            "date": day_key.isoformat(),
            "label": day_key.strftime("%d %b"),
            "booked": 0,
            "completed": 0,
            "cancelled": 0,
            "booked_height_pct": 0,
            "completed_height_pct": 0
        }
        for day_key in ordered_days
    }

    appointments = active_appointment_query().filter(
        Appointment.appointment_date >= start_date,
        Appointment.appointment_date <= end_date
    ).all()
    for appointment in appointments:
        day_key = appointment.appointment_date
        if day_key not in bucket_map:
            continue
        bucket_map[day_key]["booked"] += 1
        normalized_status = (appointment.status or "BOOKED").strip().upper()
        if normalized_status == "COMPLETED":
            bucket_map[day_key]["completed"] += 1
        elif normalized_status == "CANCELLED":
            bucket_map[day_key]["cancelled"] += 1

    max_count = 0
    points = []
    for day_key in ordered_days:
        bucket = bucket_map[day_key]
        max_count = max(max_count, bucket["booked"], bucket["completed"])
        points.append(bucket)

    for bucket in points:
        if max_count <= 0:
            bucket["booked_height_pct"] = 0
            bucket["completed_height_pct"] = 0
            continue
        bucket["booked_height_pct"] = max(10, int(round((bucket["booked"] / max_count) * 100))) if bucket["booked"] else 0
        bucket["completed_height_pct"] = max(10, int(round((bucket["completed"] / max_count) * 100))) if bucket["completed"] else 0
    return points


def build_dashboard_dead_stock(limit=5, dormant_days=60):
    limit = max(1, min(to_int_safe(limit, 5), 12))
    dormant_days = max(30, min(to_int_safe(dormant_days, 60), 365))
    end_date = clinic_now().date()
    start_date = end_date - timedelta(days=dormant_days - 1)

    medicine_data, _errors = build_medicine_report_data(
        start_date.isoformat(),
        end_date.isoformat(),
        top_n=max(limit, 10)
    )
    dormant_rows = {}
    for row in medicine_data.get("medicine_summary") or []:
        if to_int_safe(row.get("current_stock"), 0) <= 0:
            continue
        if to_float_safe(row.get("net_sold_qty"), 0) > 0:
            continue
        dormant_rows[(row.get("medicine") or "").strip().upper()] = {
            "name": (row.get("medicine") or "").strip(),
            "qty": to_int_safe(row.get("current_stock"), 0),
            "blocked_value": 0.0,
            "days_without_sale": dormant_days
        }

    if not dormant_rows:
        return []

    med_name_by_id = {
        med.id: (med.name or "").strip()
        for med in Medicine.query.with_entities(Medicine.id, Medicine.name).all()
    }
    purchase_items = VendorPurchaseItem.query.filter(
        db.func.coalesce(VendorPurchaseItem.remaining_qty, 0) > 0
    ).all()
    for item in purchase_items:
        med_name = (item.medicine_name or med_name_by_id.get(item.medicine_id) or "").strip().upper()
        if med_name in dormant_rows:
            dormant_rows[med_name]["blocked_value"] += (
                to_int_safe(item.remaining_qty, 0) * to_float_safe(item.purchase_rate, 0)
            )

    rows = list(dormant_rows.values())
    for row in rows:
        row["blocked_value"] = round(row["blocked_value"], 2)
    rows.sort(key=lambda row: (row["blocked_value"], row["qty"], row["name"].lower()), reverse=True)
    return rows[:limit]


def build_dashboard_top_medicines(limit=5, period_days=30):
    limit = max(1, min(to_int_safe(limit, 5), 12))
    period_days = max(7, min(to_int_safe(period_days, 30), 120))
    end_date = clinic_now().date()
    start_date = end_date - timedelta(days=period_days - 1)
    medicine_data, _errors = build_medicine_report_data(
        start_date.isoformat(),
        end_date.isoformat(),
        top_n=max(limit, 10)
    )
    rows = [row for row in (medicine_data.get("fast_movers") or []) if to_float_safe(row.get("sales_value"), 0) > 0]
    rows = rows[:limit]
    max_sales = max((to_float_safe(row.get("sales_value"), 0) for row in rows), default=0.0)
    result = []
    for row in rows:
        sales_value = to_float_safe(row.get("sales_value"), 0)
        result.append({
            "name": (row.get("medicine") or "").strip(),
            "qty": to_int_safe(row.get("net_sold_qty"), 0),
            "sales_value": round(sales_value, 2),
            "net_profit": round(to_float_safe(row.get("net_profit"), 0), 2),
            "bar_pct": max(12, int(round((sales_value / max_sales) * 100))) if max_sales > 0 else 0
        })
    return result

def is_truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

def compact_display_value(value, limit=56):
    if value in (None, "", [], {}):
        return "-"
    if isinstance(value, bool):
        text = "Yes" if value else "No"
    elif isinstance(value, float):
        text = f"{value:.2f}".rstrip("0").rstrip(".")
    elif isinstance(value, Decimal):
        text = f"{float(value):.2f}".rstrip("0").rstrip(".")
    elif isinstance(value, (int,)):
        text = str(value)
    elif isinstance(value, datetime):
        text = value.strftime("%d-%m-%Y %I:%M %p")
    elif isinstance(value, date):
        text = value.strftime("%d-%m-%Y")
    elif isinstance(value, time):
        text = value.strftime("%I:%M %p")
    elif isinstance(value, dict):
        keys = [str(key).replace("_", " ").title() for key in list(value.keys())[:3]]
        text = ", ".join(keys)
        if len(value) > 3:
            text = f"{text} +{len(value) - 3} more"
    elif isinstance(value, (list, tuple, set)):
        items = [compact_display_value(item, limit=18) for item in list(value)[:3]]
        text = ", ".join(item for item in items if item and item != "-")
        if len(value) > 3:
            text = f"{text} +{len(value) - 3} more"
    else:
        text = str(value).strip()
    if not text:
        return "-"
    if len(text) > limit:
        return text[:limit - 3] + "..."
    return text

def classify_audit_action(action):
    normalized = (action or "").strip().lower()
    if not normalized:
        return "OTHER"
    if "delete" in normalized or "remove" in normalized:
        return "DELETE"
    if (
        "status" in normalized
        or "paid" in normalized
        or "cancel" in normalized
        or "posted" in normalized
        or "restored" in normalized
    ):
        return "STATUS"
    if (
        "create" in normalized
        or "added" in normalized
        or "booked" in normalized
        or normalized.startswith("new ")
    ):
        return "CREATE"
    if "update" in normalized or "edit" in normalized or "change" in normalized:
        return "UPDATE"
    return "OTHER"

def build_audit_change_rows(before_data, after_data, limit=8):
    if isinstance(before_data, dict) and isinstance(after_data, dict):
        changes = []
        ordered_keys = []
        for key in list(before_data.keys()) + list(after_data.keys()):
            if key not in ordered_keys:
                ordered_keys.append(key)
        for key in ordered_keys:
            before_value = before_data.get(key)
            after_value = after_data.get(key)
            if serialize_json_text(before_value) == serialize_json_text(after_value):
                continue
            changes.append({
                "field": str(key).replace("_", " ").title(),
                "before": compact_display_value(before_value),
                "after": compact_display_value(after_value)
            })
        return changes[:limit], len(changes)

    if before_data and not after_data:
        return [{
            "field": "Deleted Record",
            "before": compact_display_value(before_data),
            "after": "-"
        }], 1

    if after_data and not before_data:
        return [{
            "field": "Created Record",
            "before": "-",
            "after": compact_display_value(after_data)
        }], 1

    if before_data or after_data:
        return [{
            "field": "Snapshot",
            "before": compact_display_value(before_data),
            "after": compact_display_value(after_data)
        }], 1

    return [], 0

def resolve_patient_directory_match(patient_lookup, mobile_lookup, name_lookup, patient_id=None, mobile="", patient_name=""):
    if patient_id in patient_lookup:
        return patient_lookup[patient_id]
    mobile_digits = normalize_patient_mobile(mobile)
    if mobile_digits and mobile_digits in mobile_lookup:
        return patient_lookup.get(mobile_lookup[mobile_digits])
    normalized_name = (patient_name or "").strip().lower()
    if normalized_name and normalized_name in name_lookup:
        return patient_lookup.get(name_lookup[normalized_name])
    return None

def build_customer_directory_rows(patients):
    rows = []
    if not patients:
        return rows

    patient_lookup = {}
    mobile_lookup = {}
    name_lookup = {}
    patient_ids = []
    mobile_values = []
    name_values = []

    for patient in patients:
        row = {
            "patient": patient,
            "appointment_count": 0,
            "invoice_count": 0,
            "total_billed": 0.0,
            "last_appointment_at": None,
            "last_invoice_at": None,
            "last_activity_at": patient.updated_at,
        }
        rows.append(row)
        patient_lookup[patient.id] = row
        patient_ids.append(patient.id)

        mobile_digits = normalize_patient_mobile(patient.mobile)
        if mobile_digits and mobile_digits not in mobile_lookup:
            mobile_lookup[mobile_digits] = patient.id
            mobile_values.append(mobile_digits)

        normalized_name = (patient.name or "").strip().lower()
        if normalized_name and normalized_name not in name_lookup:
            name_lookup[normalized_name] = patient.id
            name_values.append(normalized_name)

    appointment_conditions = []
    if patient_ids:
        appointment_conditions.append(Appointment.patient_id.in_(patient_ids))
    if mobile_values:
        appointment_conditions.append(Appointment.mobile.in_(mobile_values))
    if name_values:
        appointment_conditions.append(
            db.func.lower(db.func.coalesce(Appointment.patient_name, "")).in_(name_values)
        )

    if appointment_conditions:
        appointment_rows = active_appointment_query().filter(or_(*appointment_conditions)).with_entities(
            Appointment.patient_id,
            Appointment.patient_name,
            Appointment.mobile,
            Appointment.appointment_date,
            Appointment.appointment_time
        ).all()
        for appt in appointment_rows:
            row = resolve_patient_directory_match(
                patient_lookup,
                mobile_lookup,
                name_lookup,
                patient_id=appt.patient_id,
                mobile=appt.mobile,
                patient_name=appt.patient_name
            )
            if not row:
                continue
            row["appointment_count"] += 1
            if appt.appointment_date:
                occurred_at = datetime.combine(appt.appointment_date, appt.appointment_time or time.min)
                if not row["last_appointment_at"] or occurred_at > row["last_appointment_at"]:
                    row["last_appointment_at"] = occurred_at
                if not row["last_activity_at"] or occurred_at > row["last_activity_at"]:
                    row["last_activity_at"] = occurred_at

    invoice_conditions = []
    if patient_ids:
        invoice_conditions.append(Invoice.patient_id.in_(patient_ids))
    if mobile_values:
        invoice_conditions.append(Invoice.mobile.in_(mobile_values))
    if name_values:
        invoice_conditions.append(
            db.func.lower(db.func.coalesce(Invoice.customer, "")).in_(name_values)
        )

    if invoice_conditions:
        invoice_rows = Invoice.query.filter(or_(*invoice_conditions)).with_entities(
            Invoice.patient_id,
            Invoice.customer,
            Invoice.mobile,
            Invoice.total,
            Invoice.created_at
        ).all()
        for invoice in invoice_rows:
            row = resolve_patient_directory_match(
                patient_lookup,
                mobile_lookup,
                name_lookup,
                patient_id=invoice.patient_id,
                mobile=invoice.mobile,
                patient_name=invoice.customer
            )
            if not row:
                continue
            row["invoice_count"] += 1
            row["total_billed"] += round(to_float_safe(invoice.total, 0), 2)
            if invoice.created_at and (not row["last_invoice_at"] or invoice.created_at > row["last_invoice_at"]):
                row["last_invoice_at"] = invoice.created_at
            if invoice.created_at and (not row["last_activity_at"] or invoice.created_at > row["last_activity_at"]):
                row["last_activity_at"] = invoice.created_at

    for row in rows:
        row["total_billed"] = round(row["total_billed"], 2)
        row["has_mobile"] = bool((row["patient"].mobile or "").strip())
        row["has_notes"] = bool((row["patient"].notes or "").strip())

    return rows

def build_medicine_audit_snapshot(med):
    if not med:
        return None
    return {
        "id": med.id,
        "name": (med.name or "").strip(),
        "batch": (med.batch or "").strip(),
        "expiry": med.expiry,
        "mrp": round(to_float_safe(med.mrp, 0), 3),
        "qty": to_int_safe(med.qty, 0),
        "discount_percent": to_int_safe(med.discount_percent, 0),
        "pack_type": (med.pack_type or "").strip(),
        "pack_qty": med.pack_qty,
        "barcode": (getattr(med, "barcode", "") or "").strip(),
        "reorder_level": to_int_safe(getattr(med, "reorder_level", 10), 10),
        "is_active": bool(getattr(med, "is_active", True))
    }

def build_invoice_audit_snapshot(inv):
    if not inv:
        return None
    return {
        "id": inv.id,
        "invoice_no": (inv.invoice_no or "").strip(),
        "patient_id": getattr(inv, "patient_id", None),
        "customer": (inv.customer or "").strip(),
        "mobile": (inv.mobile or "").strip(),
        "payment_mode": (inv.payment_mode or "").strip().upper(),
        "subtotal": round(to_float_safe(inv.subtotal, 0), 2),
        "discount": round(to_float_safe(inv.discount, 0), 2),
        "total": round(to_float_safe(inv.total, 0), 2),
        "created_by": (inv.created_by or "").strip()
    }

def build_appointment_audit_snapshot(appt):
    if not appt:
        return None
    return {
        "id": appt.id,
        "appointment_no": (appt.appointment_no or "").strip(),
        "patient_id": appt.patient_id,
        "patient_name": (appt.patient_name or "").strip(),
        "mobile": (appt.mobile or "").strip(),
        "appointment_date": appt.appointment_date.isoformat() if appt.appointment_date else "",
        "appointment_time": appt.appointment_time.strftime("%H:%M:%S") if appt.appointment_time else "",
        "status": (appt.status or "").strip().upper(),
        "payment_mode": (appt.payment_mode or "").strip().upper(),
        "payment_status": (appt.payment_status or "").strip().upper(),
        "consultation_fee": round(to_float_safe(appt.consultation_fee, 0), 2),
        "doctor_discount": round(to_float_safe(appt.doctor_discount, 0), 2)
    }

def build_patient_audit_snapshot(patient):
    if not patient:
        return None
    return {
        "id": patient.id,
        "name": (patient.name or "").strip(),
        "mobile": (patient.mobile or "").strip(),
        "age": patient.age,
        "gender": (patient.gender or "").strip().upper(),
        "notes": (patient.notes or "").strip()
    }

def can_manage_patient_records(user=None):
    actor = user
    if actor is None:
        actor = active_user_by_id(session.get("user_id"))
    if not actor:
        return False
    return bool(actor.role == "admin" or actor.can_invoice_action)

def normalize_patient_name(value):
    return " ".join(str(value or "").strip().lower().split())

def patient_name_expression(column):
    return db.func.lower(db.func.trim(db.func.coalesce(column, "")))

def invoice_name_expression():
    return patient_name_expression(Invoice.customer)

def appointment_name_expression():
    return patient_name_expression(Appointment.patient_name)

def patient_master_name_expression():
    return patient_name_expression(Patient.name)

def build_patient_duplicate_name_counts():
    rows = db.session.query(
        patient_master_name_expression().label("normalized_name"),
        db.func.count(Patient.id)
    ).filter(
        db.func.length(db.func.trim(db.func.coalesce(Patient.name, ""))) > 0
    ).group_by(
        patient_master_name_expression()
    ).having(
        db.func.count(Patient.id) > 1
    ).all()
    return {
        (normalized_name or "").strip(): int(count or 0)
        for normalized_name, count in rows
        if (normalized_name or "").strip()
    }

def build_patient_duplicate_candidates(patient, limit=8):
    normalized_name = normalize_patient_name(getattr(patient, "name", ""))
    if not normalized_name:
        return []
    rows = Patient.query.filter(
        Patient.id != patient.id,
        patient_master_name_expression() == normalized_name
    ).order_by(
        Patient.updated_at.desc(),
        Patient.id.desc()
    ).limit(max(1, min(to_int_safe(limit, 8), 15))).all()
    return rows

def build_patient_link_summary(patient):
    patient_name = normalize_patient_name(patient.name)
    patient_mobile = normalize_patient_mobile(patient.mobile)

    direct_appointments = active_appointment_query().filter(Appointment.patient_id == patient.id).count()
    direct_invoices = Invoice.query.filter(getattr(Invoice, "patient_id") == patient.id).count()

    orphan_appointment_conditions = [Appointment.patient_id.is_(None)]
    appointment_match_conditions = []
    if patient_mobile:
        appointment_match_conditions.append(Appointment.mobile == patient_mobile)
    if patient_name:
        appointment_match_conditions.append(appointment_name_expression() == patient_name)
    if appointment_match_conditions:
        orphan_appointment_conditions.append(or_(*appointment_match_conditions))
        orphan_appointments = active_appointment_query().filter(and_(*orphan_appointment_conditions)).count()
    else:
        orphan_appointments = 0

    orphan_invoice_conditions = [Invoice.patient_id.is_(None)]
    invoice_match_conditions = []
    if patient_mobile:
        invoice_match_conditions.append(Invoice.mobile == patient_mobile)
    if patient_name:
        invoice_match_conditions.append(invoice_name_expression() == patient_name)
    if invoice_match_conditions:
        orphan_invoice_conditions.append(or_(*invoice_match_conditions))
        orphan_invoices = Invoice.query.filter(and_(*orphan_invoice_conditions)).count()
    else:
        orphan_invoices = 0

    return {
        "linked_appointments": direct_appointments,
        "linked_invoices": direct_invoices,
        "orphan_appointments": orphan_appointments,
        "orphan_invoices": orphan_invoices
    }

def relink_patient_records(patient, matching_names=None, matching_mobiles=None, source_patient_ids=None, sync_display_fields=False):
    normalized_names = {
        normalize_patient_name(name)
        for name in (matching_names or [])
        if normalize_patient_name(name)
    }
    patient_name = (patient.name or "").strip()
    if normalize_patient_name(patient_name):
        normalized_names.add(normalize_patient_name(patient_name))

    normalized_mobiles = {
        normalize_patient_mobile(mobile)
        for mobile in (matching_mobiles or [])
        if normalize_patient_mobile(mobile)
    }
    patient_mobile = normalize_patient_mobile(patient.mobile)
    if patient_mobile:
        normalized_mobiles.add(patient_mobile)

    source_ids = {to_int_safe(source_id, 0) for source_id in (source_patient_ids or []) if to_int_safe(source_id, 0)}
    source_ids.discard(patient.id)

    appointment_conditions = [Appointment.patient_id == patient.id]
    if source_ids:
        appointment_conditions.append(Appointment.patient_id.in_(source_ids))
    if normalized_mobiles:
        appointment_conditions.append(Appointment.mobile.in_(sorted(normalized_mobiles)))
    if normalized_names:
        appointment_conditions.append(appointment_name_expression().in_(sorted(normalized_names)))
    appointment_rows = active_appointment_query().filter(or_(*appointment_conditions)).all() if appointment_conditions else []

    invoice_conditions = [Invoice.patient_id == patient.id]
    if source_ids:
        invoice_conditions.append(Invoice.patient_id.in_(source_ids))
    if normalized_mobiles:
        invoice_conditions.append(Invoice.mobile.in_(sorted(normalized_mobiles)))
    if normalized_names:
        invoice_conditions.append(invoice_name_expression().in_(sorted(normalized_names)))
    invoice_rows = Invoice.query.filter(or_(*invoice_conditions)).all() if invoice_conditions else []

    appointment_relinked = 0
    appointment_synced = 0
    invoice_relinked = 0
    invoice_synced = 0

    for appt in appointment_rows:
        if appt.patient_id != patient.id:
            appt.patient_id = patient.id
            appointment_relinked += 1
        if sync_display_fields:
            row_changed = False
            appt_name_token = normalize_patient_name(appt.patient_name)
            appt_mobile_token = normalize_patient_mobile(appt.mobile)
            if patient_name and (appt_name_token in normalized_names or not appt_name_token):
                if appt.patient_name != patient_name:
                    appt.patient_name = patient_name
                    row_changed = True
            if patient_mobile and (appt_mobile_token in normalized_mobiles or not appt_mobile_token):
                if appt.mobile != patient_mobile:
                    appt.mobile = patient_mobile
                    row_changed = True
            if patient.age not in (None, "") and appt.age != patient.age:
                appt.age = patient.age
                row_changed = True
            if (patient.gender or "").strip() and (appt.gender != patient.gender):
                appt.gender = patient.gender
                row_changed = True
            if row_changed:
                appointment_synced += 1

    for invoice in invoice_rows:
        if getattr(invoice, "patient_id", None) != patient.id:
            invoice.patient_id = patient.id
            invoice_relinked += 1
        if sync_display_fields:
            row_changed = False
            invoice_name_token = normalize_patient_name(invoice.customer)
            invoice_mobile_token = normalize_patient_mobile(invoice.mobile)
            if patient_name and (invoice_name_token in normalized_names or not invoice_name_token):
                if invoice.customer != patient_name:
                    invoice.customer = patient_name
                    row_changed = True
            if patient_mobile and (invoice_mobile_token in normalized_mobiles or not invoice_mobile_token):
                if invoice.mobile != patient_mobile:
                    invoice.mobile = patient_mobile
                    row_changed = True
            if (patient.gender or "").strip() and (not (invoice.gender or "").strip() or (invoice_name_token in normalized_names)):
                if invoice.gender != patient.gender:
                    invoice.gender = patient.gender
                    row_changed = True
            if row_changed:
                invoice_synced += 1

    return {
        "appointment_relinked": appointment_relinked,
        "appointment_synced": appointment_synced,
        "invoice_relinked": invoice_relinked,
        "invoice_synced": invoice_synced
    }

def record_audit_event(action, entity_type="", entity_id=None, ref_code="", before=None, after=None, extra=None):
    details = {}
    if isinstance(extra, dict):
        details.update(extra)
    elif extra not in (None, ""):
        details["info"] = extra
    try:
        details.setdefault("path", request.path)
        details.setdefault("method", request.method)
    except RuntimeError:
        pass

    try:
        db.session.add(AuditLog(
            user=session.get("username"),
            action=(action or "").strip(),
            entity_type=((entity_type or "").strip().upper() or None),
            entity_id=entity_id,
            ref_code=(ref_code or "").strip() or None,
            before_json=serialize_json_text(before),
            after_json=serialize_json_text(after),
            extra_json=serialize_json_text(details)
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()
        app.logger.exception("Audit log write failed for action=%s entity=%s", action, entity_type)

def upsert_patient_from_invoice(customer_name, mobile, gender=""):
    patient_name = (customer_name or "").strip()
    mobile_digits = normalize_patient_mobile(mobile)
    gender = (gender or "").strip().upper()

    if not patient_name and not mobile_digits:
        return None, mobile_digits

    patient = None
    if mobile_digits:
        patient = Patient.query.filter_by(mobile=mobile_digits).first()
        if not patient:
            patient = Patient(name=patient_name or mobile_digits, mobile=mobile_digits)
            db.session.add(patient)
    elif patient_name:
        patient = Patient.query.filter(
            db.func.lower(Patient.name) == patient_name.lower()
        ).order_by(Patient.id.desc()).first()
        if not patient:
            patient = Patient(name=patient_name, mobile=None)
            db.session.add(patient)

    if patient_name:
        patient.name = patient_name
    if mobile_digits:
        patient.mobile = mobile_digits
    if gender and (not patient.gender or patient.gender == "OTHER"):
        patient.gender = gender
    return patient, mobile_digits

def find_medicine_for_hold(name, batch=""):
    if not name:
        return None
    if batch:
        med = Medicine.query.filter_by(name=name, batch=batch).first()
        if med:
            return med
    return Medicine.query.filter_by(name=name).first()

def build_hold_rows_from_form(form, include_partial=False):
    rows = []
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
        qty_val = to_int_safe(qty_list[idx] if idx < len(qty_list) else 0, 0)
        batch = (batch_overrides[idx] if idx < len(batch_overrides) else "").strip()
        batch_mode = "MANUAL" if batch else "AUTO"
        expiry = (expiry_list[idx] if idx < len(expiry_list) else "").strip()
        qoh_raw = qoh_list[idx] if idx < len(qoh_list) else ""
        mrp_raw = mrp_list[idx] if idx < len(mrp_list) else ""
        discount_raw = discount_list[idx] if idx < len(discount_list) else ""
        net_raw = net_list[idx] if idx < len(net_list) else ""

        if not name:
            continue
        if qty_val <= 0 and not include_partial:
            continue

        qoh = to_float_safe(qoh_raw, 0)
        mrp = to_float_safe(mrp_raw, 0)
        discount_percent = to_float_safe(discount_raw, 0)
        net_amount = to_float_safe(net_raw, 0)

        med = find_medicine_for_hold(name, batch)
        if med:
            if not batch and not include_partial:
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

        rows.append({
            "name": name,
            "batch_mode": batch_mode,
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

    return rows


def build_hold_items_from_form(form):
    return build_hold_rows_from_form(form, include_partial=False)

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
    raw_draft_items = []
    totals = {}
    header = {}

    if isinstance(payload, dict):
        header = payload.get("header") if isinstance(payload.get("header"), dict) else {}
        raw_items = payload.get("items") if isinstance(payload.get("items"), list) else []
        raw_draft_items = payload.get("draft_items") if isinstance(payload.get("draft_items"), list) else []
        if not raw_items and isinstance(payload.get("cart"), list):
            raw_items = payload.get("cart")
        totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    elif isinstance(payload, list):
        raw_items = payload

    def normalize_rows(raw_list, allow_zero_qty=False):
        normalized = []
        for raw in raw_list:
            if not isinstance(raw, dict):
                continue
            name = (raw.get("name") or raw.get("medicine_name") or "").strip().upper()
            if not name:
                continue
            batch_mode = (raw.get("batch_mode") or "").strip().upper()
            qty_val = to_int_safe(raw.get("qty"), 0)
            if qty_val < 0:
                qty_val = 0
            if qty_val <= 0 and not allow_zero_qty:
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
                if not batch and batch_mode != "AUTO":
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

            normalized.append({
                "name": name,
                "batch_mode": batch_mode,
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
        return normalized

    items = normalize_rows(raw_draft_items or raw_items, allow_zero_qty=bool(raw_draft_items))
    finalized_items = normalize_rows(raw_items, allow_zero_qty=False)
    subtotal = to_float_safe(totals.get("subtotal"), round(sum(i["net_amount"] for i in finalized_items), 2))
    discount = to_float_safe(totals.get("discount"), round(sum(i["discount_amount"] for i in finalized_items), 2))
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


def medicine_pack_display(med):
    if med.pack_type and med.pack_qty:
        return f"{med.pack_type} of {med.pack_qty}"
    if med.pack_type:
        return med.pack_type
    if med.pack_qty:
        return str(med.pack_qty)
    return "-"


def medicine_expiry_display(expiry):
    raw = (expiry or "").strip()
    exp_dt = parse_expiry_date(raw)
    if exp_dt:
        return exp_dt.strftime("%m/%Y"), exp_dt
    return raw or "-", None


def build_medicine_master_groups(show_archived=False):
    today = date.today()
    medicines = Medicine.query.order_by(
        db.func.lower(Medicine.name).asc(),
        Medicine.batch.asc(),
        Medicine.id.asc()
    ).all()

    grouped = {}
    stats = {
        "visible_medicine_count": 0,
        "visible_batch_count": 0,
        "archived_batch_count": 0,
        "hidden_archived_batch_count": 0,
        "total_stock": 0
    }

    for med in medicines:
        name = (med.name or "").strip()
        if not name:
            continue

        key = name.lower()
        group = grouped.get(key)
        if not group:
            group = {
                "name": name,
                "total_stock": 0,
                "total_batches": 0,
                "active_batches": 0,
                "archived_batches": 0,
                "has_barcode": False,
                "visible_stock": 0,
                "next_expiry": "-",
                "next_expiry_sort": date.max,
                "batches": [],
                "search_text": name.lower()
            }
            grouped[key] = group

        qty = to_int(med.qty)
        is_archived = qty <= 0
        expiry_display, expiry_sort = medicine_expiry_display(med.expiry)
        is_expired = bool(expiry_sort and expiry_sort < today)
        status = "Archived" if is_archived else ("Expired" if is_expired else "Active")
        if (getattr(med, "barcode", "") or "").strip():
            group["has_barcode"] = True

        group["total_batches"] += 1
        group["total_stock"] += qty
        stats["total_stock"] += qty

        if is_archived:
            group["archived_batches"] += 1
            stats["archived_batch_count"] += 1
            if not show_archived:
                stats["hidden_archived_batch_count"] += 1
        else:
            group["active_batches"] += 1
            if expiry_sort and expiry_sort < group["next_expiry_sort"]:
                group["next_expiry_sort"] = expiry_sort
                group["next_expiry"] = expiry_display

        group["search_text"] += " " + " ".join(
            part.lower()
            for part in (
                med.batch or "",
                getattr(med, "barcode", "") or "",
                medicine_pack_display(med),
                expiry_display,
                status
            )
            if part
        )

        if is_archived and not show_archived:
            continue

        batch_row = {
            "id": med.id,
            "batch": (med.batch or "").strip() or "-",
            "pack": medicine_pack_display(med),
            "expiry": expiry_display,
            "expiry_sort": expiry_sort or date.max,
            "mrp": to_float(med.mrp),
            "qty": qty,
            "status": status,
            "status_class": "archived" if is_archived else ("expired" if is_expired else "active")
        }
        group["batches"].append(batch_row)
        group["visible_stock"] += qty
        stats["visible_batch_count"] += 1

    groups = []
    for group in grouped.values():
        if not group["batches"]:
            continue
        group["batches"].sort(
            key=lambda item: (
                item["status"] != "Active",
                item["expiry_sort"],
                item["batch"].lower(),
                item["id"]
            )
        )
        if group["next_expiry"] == "-" and group["batches"]:
            group["next_expiry"] = group["batches"][0]["expiry"]
        groups.append(group)

    groups.sort(key=lambda item: item["name"].lower())
    stats["visible_medicine_count"] = len(groups)
    return groups, stats

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
        "outstanding_impact": decimal_str(note.outstanding_impact),
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

def adjust_vendor_outstanding(vendor_id, delta, return_applied=False):
    vendor = Vendor.query.get(vendor_id) if vendor_id else None
    if not vendor:
        return Decimal("0")
    before = to_decimal(vendor.outstanding_balance or 0)
    balance = before + to_decimal(delta or 0)
    if balance < Decimal("0"):
        balance = Decimal("0")
    after = quantize_decimal(balance, "0.01")
    vendor.outstanding_balance = float(after)
    if vendor.outstanding_balance <= 0:
        vendor.payment_status = "Paid"
    elif (vendor.payment_status or "").strip().lower() == "paid":
        vendor.payment_status = "Unpaid"
    db.session.add(vendor)
    applied = quantize_decimal(after - before, "0.0001")
    if return_applied:
        return applied
    return Decimal("0")


def get_note_outstanding_impact(note):
    if note is None:
        return Decimal("0")
    impact = getattr(note, "outstanding_impact", None)
    if impact not in (None, "", " "):
        return to_decimal(impact)
    grand_total = to_decimal(getattr(note, "grand_total", 0) or 0)
    if (getattr(note, "note_type", "") or "").upper() == "DEBIT":
        return -grand_total
    return grand_total

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None

def get_clinic_tzinfo():
    if ZoneInfo is not None:
        try:
            return ZoneInfo(APP_TIMEZONE)
        except Exception:
            return None
    return None

def local_date_range_to_storage_bounds(start_date, end_date=None):
    if not start_date:
        return None, None
    end_date = end_date or start_date
    start_local = datetime.combine(start_date, time.min)
    end_local_exclusive = datetime.combine(end_date + timedelta(days=1), time.min)
    tzinfo = get_clinic_tzinfo()
    if not tzinfo:
        return start_local, end_local_exclusive
    start_utc = start_local.replace(tzinfo=tzinfo).astimezone(timezone.utc).replace(tzinfo=None)
    end_utc = end_local_exclusive.replace(tzinfo=tzinfo).astimezone(timezone.utc).replace(tzinfo=None)
    return start_utc, end_utc

def local_month_to_storage_bounds(year, month):
    month_start = date(year, month, 1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return local_date_range_to_storage_bounds(month_start, next_month - timedelta(days=1))

def parse_time_value(val):
    if not val:
        return None
    raw = str(val).strip()
    normalized = raw.upper().replace(".", "")
    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M%p", "%I:%M:%S %p", "%I:%M:%S%p"):
        try:
            time_source = normalized if "%p" in fmt else raw
            return datetime.strptime(time_source, fmt).time()
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

APPOINTMENT_STATUSES = ("BOOKED", "WAITING", "CHECKED_IN", "IN_CONSULTATION", "COMPLETED", "CANCELLED")
APPOINTMENT_PAYMENT_MODES = ("CASH", "ONLINE", "UPI", "CARD")
APPOINTMENT_PAYMENT_STATUSES = ("PAID", "UNPAID")
APPOINTMENT_CONSULTATION_FEES = ("600", "1000")
APPOINTMENT_DEFAULT_DOCTOR = "GENERAL"
APPOINTMENT_GENDERS = ("MALE", "FEMALE", "OTHER")

APPOINTMENT_STATUS_FLOW = {
    "BOOKED": {"WAITING", "CHECKED_IN", "IN_CONSULTATION", "COMPLETED", "CANCELLED"},
    "WAITING": {"BOOKED", "CHECKED_IN", "IN_CONSULTATION", "COMPLETED", "CANCELLED"},
    "CHECKED_IN": {"WAITING", "IN_CONSULTATION", "COMPLETED", "CANCELLED"},
    "IN_CONSULTATION": {"CHECKED_IN", "COMPLETED", "CANCELLED"},
    "COMPLETED": set(),
    "CANCELLED": set()
}

def find_medicine_by_name_batch(name, batch):
    n = (name or "").strip()
    b = (batch or "").strip()
    if not n or not b:
        return None
    return Medicine.query.filter(
        db.func.lower(Medicine.name) == n.lower(),
        db.func.lower(Medicine.batch) == b.lower()
    ).first()

def find_medicine_discount_template(name, pack_type="", pack_qty=None, exclude_medicine_id=None):
    normalized_name = normalize_medicine_name(name)
    if not normalized_name:
        return None

    base_query = Medicine.query.filter(
        db.func.lower(db.func.trim(Medicine.name)) == normalized_name.lower()
    )
    if exclude_medicine_id:
        base_query = base_query.filter(Medicine.id != exclude_medicine_id)

    exact_query = base_query
    normalized_pack_type = (pack_type or "").strip().lower()
    if normalized_pack_type:
        exact_query = exact_query.filter(
            db.func.lower(db.func.coalesce(Medicine.pack_type, "")) == normalized_pack_type
        )
    if pack_qty is not None:
        exact_query = exact_query.filter(Medicine.pack_qty == pack_qty)

    template = exact_query.order_by(Medicine.created_at.desc(), Medicine.id.desc()).first()
    if template:
        return template
    return base_query.order_by(Medicine.created_at.desc(), Medicine.id.desc()).first()

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
instance_dir = ensure_writable_dir(
    app.instance_path,
    os.path.join(TMP_BASE_DIR, "instance")
)
default_db = "sqlite:///" + os.path.join(instance_dir, "pharmacy.db")
db_url = resolve_database_url(default_db)
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)
migrate = Migrate(app, db, compare_type=True, render_as_batch=db_url.startswith("sqlite")) if Migrate else None
AUTO_DATA_BACKFILL_ON_BOOT = env_flag("AUTO_DATA_BACKFILL_ON_BOOT", not IS_SERVERLESS)

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
        ensure_column("user", "can_view_medicine", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_add_medicine", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_edit_medicine", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_delete_medicine", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_edit_invoice", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_delete_invoice", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_invoice_action", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_view_stock_history", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_view_reports", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_manage_users", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_manage_purchases", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_view_audit_logs", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "can_view_profit_dashboard", "BOOLEAN DEFAULT FALSE")
        ensure_column("user", "access_profile", "TEXT")
        ensure_column("user", "session_version", "INTEGER DEFAULT 0")
        ensure_column("user", "is_active", "BOOLEAN DEFAULT TRUE")
        ensure_column("user", "deleted_at", "TIMESTAMP")
        ensure_column("user", "deleted_by", "TEXT")
        ensure_column("user", "last_login_at", "TIMESTAMP")
        ensure_column("user", "last_login_ip", "TEXT")
        ensure_column("user", "last_login_user_agent", "TEXT")
        db.session.execute(text(
            'UPDATE "user" '
            'SET "session_version" = 0 '
            'WHERE "session_version" IS NULL'
        ))
        db.session.execute(text(
            'UPDATE "user" '
            'SET "is_active" = 1 '
            'WHERE "is_active" IS NULL'
        ))
        db.session.commit()

        # Medicine extra fields
        ensure_column("medicine", "composition", "TEXT")
        ensure_column("medicine", "company", "TEXT")
        ensure_column("medicine", "pack_type", "TEXT")
        ensure_column("medicine", "pack_qty", "INTEGER")
        ensure_column("medicine", "barcode", "TEXT")
        ensure_column("medicine", "reorder_level", "INTEGER DEFAULT 10")
        ensure_column("medicine", "is_active", "BOOLEAN DEFAULT TRUE")

        # Vendor extra fields
        ensure_column("vendor", "shop_name", "TEXT")
        ensure_column("vendor", "area", "TEXT")
        ensure_column("vendor", "city", "TEXT")
        ensure_column("vendor", "state", "TEXT")
        ensure_column("vendor", "pincode", "TEXT")
        ensure_column("vendor", "account_holder_name", "TEXT")
        ensure_column("vendor_purchase", "invoice_no", "TEXT")
        ensure_column("vendor_purchase", "bill_attachment_ref", "TEXT")
        ensure_column("vendor_purchase", "paid_amount", "REAL")

        ensure_column("return_bill", "return_no", "TEXT")
        ensure_column("return_bill", "payment_mode", "TEXT")
        ensure_column("return_bill", "cgst", "REAL")
        ensure_column("return_bill", "sgst", "REAL")
        ensure_column("return_bill", "is_cancelled", "BOOLEAN DEFAULT FALSE")
        ensure_column("return_bill", "cancelled_by", "TEXT")
        ensure_column("return_bill", "cancelled_at", "TEXT")
        ensure_column("return_item", "medicine_id", "INTEGER")
        ensure_column("return_item", "purchase_rate", "REAL")
        ensure_column("return_item", "selling_rate", "REAL")
        ensure_column("return_item", "gst_percent", "REAL")
        ensure_column("return_item", "reason", "TEXT")
        ensure_column("invoice", "patient_id", "INTEGER")
        ensure_column("invoice_item", "cost_price", "REAL")
        ensure_column("invoice_item", "cost_amount", "REAL")
        ensure_column("return_item", "cost_price", "REAL")
        ensure_column("return_item", "cost_amount", "REAL")
        ensure_column("vendor_purchase_item", "remaining_qty", "INTEGER")
        ensure_column("vendor_purchase_item", "pack_type", "TEXT")
        ensure_column("vendor_purchase_item", "pack_qty", "INTEGER")
        ensure_column("vendor_purchase_item", "barcode", "TEXT")
        ensure_column("vendor_notes", "outstanding_impact", "NUMERIC(12,4)")
        ensure_column("stock_history", "ref_table", "TEXT")
        ensure_column("stock_history", "ref_id", "INTEGER")
        ensure_column("audit_log", "user", "TEXT")
        ensure_column("audit_log", "action", "TEXT")
        ensure_column("audit_log", "entity_type", "TEXT")
        ensure_column("audit_log", "entity_id", "INTEGER")
        ensure_column("audit_log", "ref_code", "TEXT")
        ensure_column("audit_log", "before_json", "TEXT")
        ensure_column("audit_log", "after_json", "TEXT")
        ensure_column("audit_log", "extra_json", "TEXT")
        ensure_column("audit_log", "created_at", "TIMESTAMP")
        ensure_column("appointment", "payment_mode", "TEXT")
        ensure_column("appointment", "payment_status", "TEXT")
        ensure_column("appointment", "doctor_discount", "REAL")
        ensure_column("appointment", "consultation_fee", "REAL")
        ensure_column("appointment", "token_no", "INTEGER")
        ensure_column("appointment", "patient_id", "INTEGER")
        ensure_column("appointment", "age", "INTEGER")
        ensure_column("appointment", "gender", "TEXT")
        ensure_column("appointment", "symptoms", "TEXT")
        ensure_column("appointment", "previous_visit_notes", "TEXT")
        ensure_column("appointment", "is_deleted", "BOOLEAN DEFAULT FALSE")
        ensure_column("appointment", "deleted_at", "TIMESTAMP")
        ensure_column("appointment", "deleted_by", "TEXT")
        ensure_column("hold_bill", "is_deleted", "BOOLEAN DEFAULT FALSE")
        ensure_column("hold_bill", "deleted_at", "TIMESTAMP")
        ensure_column("hold_bill", "deleted_by", "TEXT")
        ensure_column("vendor", "is_active", "BOOLEAN DEFAULT TRUE")
        ensure_column("vendor", "deleted_at", "TIMESTAMP")
        ensure_column("vendor", "deleted_by", "TEXT")
        if (db.session.bind.dialect.name if db.session.bind else "").lower() == "postgresql":
            legacy_boolean_columns = {
                "user": {
                    "is_active": True,
                    "can_view_medicine": False,
                    "can_add_medicine": False,
                    "can_edit_medicine": False,
                    "can_delete_medicine": False,
                    "can_edit_invoice": False,
                    "can_delete_invoice": False,
                    "can_invoice_action": False,
                    "can_view_stock_history": False,
                    "can_view_reports": False,
                    "can_manage_users": False,
                    "can_manage_purchases": False,
                    "can_view_audit_logs": False,
                    "can_view_profit_dashboard": False,
                },
                "medicine": {"is_active": True},
                "vendor": {"is_active": True},
                "return_bill": {"is_cancelled": False},
                "appointment": {"is_deleted": False},
                "hold_bill": {"is_deleted": False},
                "login_security_event": {"is_suspicious": False},
            }
            pg_inspector = inspect(db.engine)
            for table_name, columns in legacy_boolean_columns.items():
                if not pg_inspector.has_table(table_name):
                    continue
                existing_columns = {col["name"]: col for col in pg_inspector.get_columns(table_name)}
                for column_name, default_value in columns.items():
                    column_meta = existing_columns.get(column_name)
                    if not column_meta:
                        continue
                    type_name = column_meta["type"].__class__.__name__.lower()
                    if "bool" in type_name:
                        continue
                    default_sql = "TRUE" if default_value else "FALSE"
                    db.session.execute(text(
                        f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" '
                        'TYPE BOOLEAN USING ('
                        f'CASE WHEN "{column_name}" IS NULL THEN NULL '
                        f'WHEN LOWER(TRIM(CAST("{column_name}" AS TEXT))) IN '
                        "('1', 'true', 't', 'yes', 'on') THEN TRUE ELSE FALSE END)"
                    ))
                    db.session.execute(text(
                        f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" SET DEFAULT {default_sql}'
                    ))
            db.session.commit()
        if AUTO_DATA_BACKFILL_ON_BOOT:
            db.session.execute(text(
                'UPDATE "appointment" '
                'SET "payment_status" = CASE '
                'WHEN "payment_status" IS NULL OR "payment_status" = \'\' THEN '
                'CASE WHEN UPPER(COALESCE("payment_mode", \'\')) IN (\'PAID\', \'UNPAID\') '
                'THEN UPPER("payment_mode") ELSE \'UNPAID\' END '
                'ELSE UPPER("payment_status") END'
            ))
            db.session.execute(text(
                'UPDATE "appointment" '
                'SET "payment_mode" = CASE '
                'WHEN UPPER(COALESCE("payment_mode", \'\')) IN (\'CASH\', \'ONLINE\', \'UPI\', \'CARD\') '
                'THEN UPPER("payment_mode") ELSE \'CASH\' END'
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
            db.session.execute(text(
                'UPDATE "medicine" '
                'SET "reorder_level" = 10 '
                'WHERE "reorder_level" IS NULL'
            ))
            db.session.execute(text(
                'UPDATE "medicine" '
                'SET "is_active" = 1 '
                'WHERE "is_active" IS NULL'
            ))
            db.session.execute(text(
                'UPDATE "appointment" '
                'SET "is_deleted" = 0 '
                'WHERE "is_deleted" IS NULL'
            ))
            db.session.execute(text(
                'UPDATE "hold_bill" '
                'SET "is_deleted" = 0 '
                'WHERE "is_deleted" IS NULL'
            ))
            db.session.commit()

            appointments_without_token = Appointment.query.filter(
                Appointment.token_no.is_(None),
                Appointment.deleted_at.is_(None),
            ).order_by(
                Appointment.appointment_date.asc(),
                Appointment.appointment_time.asc(),
                Appointment.id.asc()
            ).all()
            if appointments_without_token:
                token_tracker = {}
                for appt in appointments_without_token:
                    if not appt.appointment_date:
                        continue
                    day_key = appt.appointment_date.isoformat()
                    if day_key not in token_tracker:
                        day_max = db.session.query(db.func.max(Appointment.token_no)).filter(
                            Appointment.appointment_date == appt.appointment_date
                        ).scalar() or 0
                        token_tracker[day_key] = int(day_max)
                    token_tracker[day_key] += 1
                    appt.token_no = token_tracker[day_key]
                db.session.commit()
        else:
            app.logger.info("Skipping boot-time data backfill (AUTO_DATA_BACKFILL_ON_BOOT=0)")

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
        if AUTO_DATA_BACKFILL_ON_BOOT:
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
        ensure_runtime_indexes()
    except Exception:
        # If schema upgrade fails, app should still run
        db.session.rollback()
        pass
    if not active_user_query().filter_by(username="admin").first():
        bootstrap_admin_password = (
            (os.environ.get("DEFAULT_ADMIN_PASSWORD") or "").strip()
            or (os.environ.get("ADMIN_PASSWORD") or "").strip()
        )
        if not bootstrap_admin_password and not IS_PROD:
            bootstrap_admin_password = secrets.token_urlsafe(12)
            app.logger.warning(
                "Generated a local bootstrap admin password because DEFAULT_ADMIN_PASSWORD is missing: %s",
                bootstrap_admin_password
            )
        if bootstrap_admin_password:
            admin = User(
                username="admin",
                role="admin",
                access_profile="admin",
                can_view_medicine=True,
                can_add_medicine=True,
                can_edit_medicine=True,
                can_delete_medicine=True,
                can_edit_invoice=True,
                can_delete_invoice=True,
                can_invoice_action=True,
                can_view_stock_history=True,
                can_view_reports=True,
                can_manage_users=True,
                can_manage_purchases=True,
                can_view_audit_logs=True,
                can_view_profit_dashboard=True
            )
            admin.set_password(bootstrap_admin_password)
            db.session.add(admin)
            db.session.commit()
        else:
            app.logger.warning(
                "Admin bootstrap skipped because DEFAULT_ADMIN_PASSWORD/ADMIN_PASSWORD is not set."
            )

configure_monitoring(app, db, env_flag=env_flag)
background_jobs = init_background_jobs(
    app,
    enabled=env_flag("ENABLE_BACKGROUND_JOBS", IS_PROD and not IS_SERVERLESS)
)

# ---------------- AUTH DECORATOR ----------------
def set_login_session(user):
    session.clear()
    session.permanent = True
    now_iso = datetime.utcnow().isoformat()
    session["user_id"] = user.id
    session["session_version"] = int(user.session_version or 0)
    session["username"] = user.username
    session["role"] = user.role
    session["login_at"] = now_iso
    session["last_seen_at"] = now_iso
    session["access_profile"] = getattr(user, "access_profile", "custom") or "custom"
    session["can_view_medicine"] = user.can_view_medicine
    session["can_edit_invoice"] = user.can_edit_invoice
    session["can_delete_invoice"] = user.can_delete_invoice
    session["can_view_stock_history"] = user.can_view_stock_history
    session["can_view_reports"] = user.can_view_reports
    session["can_manage_users"] = user.can_manage_users
    session["can_manage_purchases"] = getattr(user, "can_manage_purchases", False)
    session["can_view_audit_logs"] = getattr(user, "can_view_audit_logs", False)
    session["can_view_profit_dashboard"] = getattr(user, "can_view_profit_dashboard", False)
    session["can_invoice_action"] = user.can_invoice_action
    session["can_add_medicine"] = user.can_add_medicine
    session["can_edit_medicine"] = user.can_edit_medicine
    session["can_delete_medicine"] = user.can_delete_medicine


def login_required(f):
    @wraps(f)
    def w(*args, **kwargs):
        user_id = session.get("user_id")
        if not user_id:
            return redirect("/login")
        user = active_user_by_id(user_id)
        if not user:
            session.clear()
            return redirect("/login")
        now_utc = datetime.utcnow()
        last_seen_raw = (session.get("last_seen_at") or "").strip()
        if last_seen_raw:
            try:
                last_seen_at = datetime.fromisoformat(last_seen_raw)
            except ValueError:
                last_seen_at = None
            if last_seen_at and (now_utc - last_seen_at) > timedelta(minutes=session_idle_minutes):
                log_login_security_event(
                    username=user.username,
                    user=user,
                    outcome="SESSION_EXPIRED",
                    reason="Idle session timeout",
                    is_suspicious=False,
                )
                session.clear()
                flash("Your session expired due to inactivity. Please login again.", "warning")
                return redirect("/login")
        login_at_raw = (session.get("login_at") or "").strip()
        if login_at_raw:
            try:
                login_at = datetime.fromisoformat(login_at_raw)
            except ValueError:
                login_at = None
            if login_at and (now_utc - login_at) > timedelta(hours=session_absolute_hours):
                log_login_security_event(
                    username=user.username,
                    user=user,
                    outcome="SESSION_EXPIRED",
                    reason="Absolute session lifetime exceeded",
                    is_suspicious=False,
                )
                session.clear()
                flash("Your session has expired. Please login again.", "warning")
                return redirect("/login")
        session_version = int(session.get("session_version", 0))
        if session_version != int(user.session_version or 0):
            log_login_security_event(
                username=user.username,
                user=user,
                outcome="SESSION_REVOKED",
                reason="Session version mismatch",
                is_suspicious=False,
            )
            session.clear()
            flash("You have been logged out from all devices. Please login again.", "warning")
            return redirect("/login")
        g.user = user
        session["last_seen_at"] = now_utc.isoformat()
        return f(*args, **kwargs)
    return w


@app.route("/healthz")
def healthz():
    payload = run_system_health_checks(include_counts=False)
    status_code = 200 if payload["status"] == "ok" else 503
    return jsonify(payload), status_code


@app.route("/readyz")
def readyz():
    payload = run_system_health_checks(include_counts=False)
    database_ok = bool(payload["database"]["ok"])
    storage_ok = all(
        details.get("available")
        for details in payload.get("storage", {}).values()
        if isinstance(details, dict) and "available" in details
    )
    payload["ready"] = bool(database_ok and storage_ok)
    status_code = 200 if payload["ready"] else 503
    return jsonify(payload), status_code

# ---------------- LOGIN ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            flash("Username and password are required", "danger")
            return render_template("login.html")
        normalized_username = username.lower()
        user = active_user_query().filter(db.func.lower(User.username) == normalized_username).first()
        if user and user.check_password(password):
            suspicious_reason = "Login successful"
            suspicious = False
            request_ip = current_client_ip()
            request_agent = current_user_agent()
            if user.last_login_ip and user.last_login_ip != request_ip:
                suspicious = True
                suspicious_reason = "Successful login from a new IP address"
            elif user.last_login_user_agent and user.last_login_user_agent != request_agent:
                suspicious = True
                suspicious_reason = "Successful login from a new device signature"
            user.last_login_at = datetime.utcnow()
            user.last_login_ip = request_ip
            user.last_login_user_agent = request_agent
            db.session.commit()
            set_login_session(user)
            log_login_security_event(
                username=user.username,
                user=user,
                outcome="SUCCESS",
                reason=suspicious_reason,
                is_suspicious=suspicious,
            )
            return redirect("/")
        suspicious_threshold = max(3, int(str(os.environ.get("SUSPICIOUS_LOGIN_THRESHOLD", "5")).strip() or 5))
        failed_attempts = failed_login_attempts_recent(username, current_client_ip())
        known_user = User.query.filter(db.func.lower(User.username) == normalized_username).first()
        disabled_attempt = bool(known_user and (known_user.deleted_at is not None or not getattr(known_user, "is_active", True)))
        suspicious = disabled_attempt or (failed_attempts + 1 >= suspicious_threshold)
        failure_reason = "Attempted login to disabled user" if disabled_attempt else "Invalid credentials"
        log_login_security_event(
            username=username,
            user=(known_user if known_user else None),
            outcome="FAILED",
            reason=failure_reason,
            is_suspicious=suspicious,
        )
        flash("Invalid credentials", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/logout-all", methods=["POST"])
@login_required
def logout_everywhere():
    if session.get("role") != "admin":
        flash("Access denied", "danger")
        return redirect("/")
    user = active_user_by_id(session.get("user_id"))
    if user:
        user.session_version = int(user.session_version or 0) + 1
        db.session.commit()
    session.clear()
    flash("Logged out from all devices.", "success")
    return redirect("/login")


@app.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    user = active_user_by_id(session.get("user_id"))
    if not user:
        session.clear()
        return redirect("/login")

    if request.method == "POST":
        current_password = (request.form.get("current_password") or "").strip()
        new_password = (request.form.get("new_password") or "").strip()
        confirm_password = (request.form.get("confirm_password") or "").strip()

        if not current_password:
            flash("Current password is required", "danger")
            return redirect(request.url)
        if not user.check_password(current_password):
            flash("Current password is incorrect", "danger")
            return redirect(request.url)
        if not new_password:
            flash("New password is required", "danger")
            return redirect(request.url)
        if new_password != confirm_password:
            flash("New password and confirm password do not match", "danger")
            return redirect(request.url)

        user.set_password(new_password)
        db.session.commit()
        flash("Password updated successfully", "success")
        return redirect("/")

    return render_template(
        "change_password.html",
        user=user,
        require_current_password=True,
        page_title="Change Password"
    )
# ---------------- ADD USER (ADMIN ONLY) ----------------
def admin_required(f):
    @wraps(f)
    def w(*args, **kwargs):
        if session.get("role") != "admin" and not session.get("can_manage_users"):
            flash("Access denied", "danger")
            return redirect("/")
        return f(*args, **kwargs)
    return w

def invoice_access_required(f):
    @wraps(f)
    def w(*args, **kwargs):
        user = active_user_by_id(session.get("user_id"))
        if not user:
            flash("Access denied", "danger")
            return redirect("/")
        if user.role != "admin" and not user.can_invoice_action:
            flash("Access denied", "danger")
            return redirect("/")
        return f(*args, **kwargs)
    return w

def inventory_access_required(f):
    @wraps(f)
    def w(*args, **kwargs):
        user = active_user_by_id(session.get("user_id"))
        if not user:
            flash("Access denied", "danger")
            return redirect("/")
        if user.role != "admin" and not (
            user.can_view_medicine
            or user.can_add_medicine
            or user.can_edit_medicine
            or user.can_delete_medicine
            or getattr(user, "can_manage_purchases", False)
        ):
            flash("Access denied", "danger")
            return redirect("/")
        return f(*args, **kwargs)
    return w

def vendor_note_access_required(api=False):
    def decorator(f):
        @wraps(f)
        def w(*args, **kwargs):
            user = active_user_by_id(session.get("user_id"))
            allowed = bool(
                user and (
                    user.role == "admin"
                    or user.can_invoice_action
                    or getattr(user, "can_manage_purchases", False)
                )
            )
            if allowed:
                return f(*args, **kwargs)
            if api:
                return jsonify({"error": "Access denied"}), 403
            flash("Access denied", "danger")
            return redirect("/")
        return w
    return decorator


@app.route("/system-center")
@login_required
@admin_required
def system_center():
    health = run_system_health_checks(include_counts=True)
    return render_template(
        "system_center.html",
        system_health=health,
        upgrade_tracker=build_upgrade_tracker(),
        release_identifier=get_release_identifier(),
        max_upload_mb=max_upload_mb,
        background_job_status=(background_jobs.snapshot() if background_jobs else {}),
        backup_snapshots=list_backup_snapshots(app, limit=8),
        restore_commands=build_restore_commands(app, "latest"),
        recent_suspicious_logins=LoginSecurityEvent.query.filter(
            truthy_column_expr(LoginSecurityEvent.is_suspicious)
        ).order_by(LoginSecurityEvent.created_at.desc()).limit(8).all(),
        migrate_enabled=bool(migrate),
        sentry_configured=bool((os.environ.get("SENTRY_DSN") or "").strip())
    )


@app.route("/system-center/backups/run", methods=["POST"])
@login_required
@admin_required
def run_backup_snapshot_now():
    snapshot = build_backup_snapshot(
        app,
        upload_dirs=app.config.get("INFRA_UPLOAD_DIRECTORIES", {}),
        keep_count=int(os.environ.get("BACKUP_KEEP_COUNT", "14") or 14),
        include_uploads=True,
    )
    flash(f"Backup snapshot created: {snapshot['snapshot_name']}", "success")
    return redirect("/system-center")


@app.route("/system-center/backups/restore-drill", methods=["POST"])
@login_required
@admin_required
def run_restore_drill():
    snapshot_name = (request.form.get("snapshot_name") or "latest").strip() or "latest"
    try:
        result = restore_backup_snapshot(
            app,
            snapshot_name=snapshot_name,
            include_uploads=True,
            dry_run=True,
            allow_production=False,
        )
        flash(
            f"Restore drill passed for {result['snapshot_name']}. "
            f"Tables checked: {sum((result.get('table_counts') or {}).values())} rows in snapshot manifest.",
            "success",
        )
    except Exception as exc:
        flash(f"Restore drill failed: {exc}", "danger")
    return redirect("/system-center")

# ---------------- DASHBOARD (DAILY LIVE SALE) ----------------
@app.route("/")
@login_required
def index():
    show_profit_cards = bool(
        session.get("role") == "admin" or session.get("can_view_profit_dashboard")
    )
    # ---------- TODAY SALE ----------
    today = clinic_now().date()
    today_iso = today.isoformat()
    today_start, tomorrow_start = local_date_range_to_storage_bounds(today, today)
    today_invoices = Invoice.query.filter(
        Invoice.created_at >= today_start,
        Invoice.created_at < tomorrow_start
    ).all()

    cash_today_sale = 0
    online_today_sale = 0
    cash_bills_today = 0
    online_bills_today = 0

    for invoice in today_invoices:
        total_amount = invoice.total or 0
        # Treat every non-cash payment mode as online so existing flows keep working.
        if is_cash_payment_mode(invoice.payment_mode):
            cash_today_sale += total_amount
            cash_bills_today += 1
        else:
            online_today_sale += total_amount
            online_bills_today += 1

    today_sale = cash_today_sale + online_today_sale
    bills_today = cash_bills_today + online_bills_today

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

    # ---------- APPOINTMENTS (TODAY) ----------
    appointments_today = active_appointment_query().filter(
        Appointment.appointment_date == today
    )
    appointment_total_today = appointments_today.count()
    appointment_booked_today = appointments_today.filter(
        Appointment.status == "BOOKED"
    ).count()
    appointment_completed_today = appointments_today.filter(
        Appointment.status == "COMPLETED"
    ).count()

    sales_trend = build_dashboard_sales_trend(days=7)
    appointment_trend = build_dashboard_appointment_trend(days=7)
    top_medicines = build_dashboard_top_medicines(limit=5, period_days=30)
    dead_stock_items = build_dashboard_dead_stock(limit=5, dormant_days=60)

    today_profit = None
    month_profit = None
    if show_profit_cards:
        today_profit, _today_profit_error = build_profit_report_summary(today_iso, today_iso)
        month_start = today.replace(day=1)
        month_profit, _month_profit_error = build_profit_report_summary(month_start.isoformat(), today_iso)
        today_profit = today_profit or {
            "gross_profit": 0.0,
            "gross_profit_percentage": 0.0,
            "net_sales": 0.0
        }
        month_profit = month_profit or {
            "gross_profit": 0.0,
            "gross_profit_percentage": 0.0,
            "net_sales": 0.0
        }

    return render_template(
        "index.html",
        today_sale=today_sale,
        bills_today=bills_today,
        cash_today_sale=cash_today_sale,
        online_today_sale=online_today_sale,
        cash_bills_today=cash_bills_today,
        online_bills_today=online_bills_today,
        low_stock=low_stock_count,
        expiring_soon=expiring_soon,
        inventory_value=inventory_value,
        gst_collected=gst_collected,
        appointment_total_today=appointment_total_today,
        appointment_booked_today=appointment_booked_today,
        appointment_completed_today=appointment_completed_today,
        today_date=today_iso,
        sales_trend=sales_trend,
        appointment_trend=appointment_trend,
        top_medicines=top_medicines,
        dead_stock_items=dead_stock_items,
        today_profit=today_profit,
        month_profit=month_profit,
        show_profit_cards=show_profit_cards
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
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (
        user.can_view_medicine or user.can_add_medicine or user.can_edit_medicine or user.can_delete_medicine
    ):
        flash("Access denied", "danger")
        return redirect("/")
    show_archived = (request.args.get("show_archived") or "").strip().lower() in {"1", "true", "yes", "on"}
    initial_search = (request.args.get("search") or "").strip()
    medicine_groups, medicine_stats = build_medicine_master_groups(show_archived=show_archived)
    return render_template(
        "medicines.html",
        medicine_groups=medicine_groups,
        medicine_stats=medicine_stats,
        show_archived=show_archived,
        initial_search=initial_search
    )

def build_patient_medicine_usage_report(
    from_date_raw,
    to_date_raw,
    search_query=""
):
    from_date_raw = (from_date_raw or "").strip()
    to_date_raw = (to_date_raw or "").strip()
    search_filter = (search_query or "").strip()
    search_query = search_filter.lower()
    search_digits = normalize_patient_mobile(search_filter)

    if not from_date_raw or not to_date_raw:
        return [], [], None, "Please select both from date and to date."

    start_date = parse_date(from_date_raw)
    end_date = parse_date(to_date_raw)
    if not start_date or not end_date:
        return [], [], None, "Please enter valid from/to dates."

    if end_date < start_date:
        return [], [], None, "To date must be greater than or equal to from date."

    start_bound, end_bound = local_date_range_to_storage_bounds(start_date, end_date)
    invoices = Invoice.query.filter(
        Invoice.created_at >= start_bound,
        Invoice.created_at < end_bound
    ).order_by(
        Invoice.created_at.asc(),
        Invoice.id.asc()
    ).all()

    invoice_map = {invoice.id: invoice for invoice in invoices}
    invoice_ids = list(invoice_map.keys())
    items = []
    if invoice_ids:
        items = InvoiceItem.query.filter(
            InvoiceItem.invoice_id.in_(invoice_ids)
        ).order_by(
            InvoiceItem.invoice_id.asc(),
            InvoiceItem.id.asc()
        ).all()

    grouped = {}
    for item in items:
        invoice = invoice_map.get(item.invoice_id)
        if not invoice:
            continue

        medicine_name = (item.name or "").strip()
        if not medicine_name:
            continue

        patient_name = (invoice.customer or "").strip() or "Walk-in"
        mobile = (invoice.mobile or "").strip()
        mobile_digits = normalize_patient_mobile(mobile)

        if search_query:
            search_haystack = " ".join([
                patient_name.lower(),
                mobile.lower(),
                medicine_name.lower()
            ])
            matches_search = search_query in search_haystack
            if not matches_search and search_digits and len(search_digits) >= 3:
                matches_search = search_digits in mobile_digits
            if not matches_search:
                continue

        mobile_key = normalize_patient_mobile(mobile) or mobile.lower()
        patient_key = f"{patient_name.lower()}||{mobile_key}"

        patient_entry = grouped.get(patient_key)
        if not patient_entry:
            patient_entry = {
                "patient_name": patient_name,
                "mobile": mobile or "-",
                "invoice_ids": set(),
                "invoice_nos": set(),
                "total_qty": 0,
                "medicines": {}
            }
            grouped[patient_key] = patient_entry

        patient_entry["invoice_ids"].add(invoice.id)
        patient_entry["invoice_nos"].add(invoice.invoice_no or f"INV-{invoice.id}")

        qty = to_int(item.qty)
        patient_entry["total_qty"] += qty

        medicine_key = medicine_name.upper()
        medicine_entry = patient_entry["medicines"].get(medicine_key)
        if not medicine_entry:
            medicine_entry = {
                "medicine": medicine_name,
                "invoice_ids": set(),
                "invoice_nos": set(),
                "total_qty": 0,
                "last_purchase_at": None
            }
            patient_entry["medicines"][medicine_key] = medicine_entry

        medicine_entry["invoice_ids"].add(invoice.id)
        medicine_entry["invoice_nos"].add(invoice.invoice_no or f"INV-{invoice.id}")
        medicine_entry["total_qty"] += qty
        if not medicine_entry["last_purchase_at"] or (
            invoice.created_at and invoice.created_at > medicine_entry["last_purchase_at"]
        ):
            medicine_entry["last_purchase_at"] = invoice.created_at

    patients = []
    detail_rows = []
    total_purchase_count = 0
    total_qty = 0

    for patient_entry in sorted(
        grouped.values(),
        key=lambda row: ((row["patient_name"] or "").lower(), (row["mobile"] or "").lower())
    ):
        medicine_rows = []
        patient_purchase_count = 0

        for medicine_entry in sorted(
            patient_entry["medicines"].values(),
            key=lambda row: (-row["total_qty"], (row["medicine"] or "").lower())
        ):
            purchase_count = len(medicine_entry["invoice_ids"])
            patient_purchase_count += purchase_count
            total_purchase_count += purchase_count

            row = {
                "patient_name": patient_entry["patient_name"],
                "mobile": patient_entry["mobile"],
                "medicine": medicine_entry["medicine"],
                "purchase_count": purchase_count,
                "total_qty": medicine_entry["total_qty"],
                "last_purchase_date": medicine_entry["last_purchase_at"].strftime("%d-%m-%Y") if medicine_entry["last_purchase_at"] else "-",
                "invoice_nos": ", ".join(sorted(medicine_entry["invoice_nos"]))
            }
            medicine_rows.append(row)
            detail_rows.append(row)

        patient_total_qty = patient_entry["total_qty"]
        total_qty += patient_total_qty
        patients.append({
            "patient_name": patient_entry["patient_name"],
            "mobile": patient_entry["mobile"],
            "invoice_count": len(patient_entry["invoice_ids"]),
            "purchase_count": patient_purchase_count,
            "distinct_medicines": len(medicine_rows),
            "total_qty": patient_total_qty,
            "invoice_nos": ", ".join(sorted(patient_entry["invoice_nos"])),
            "medicines": medicine_rows
        })

    summary = {
        "from_date": from_date_raw,
        "to_date": to_date_raw,
        "search_filter": search_filter,
        "patient_count": len(patients),
        "patient_medicine_count": len(detail_rows),
        "purchase_count": total_purchase_count,
        "total_qty": total_qty
    }
    return patients, detail_rows, summary, None

def build_profit_report_summary(from_date_raw, to_date_raw):
    start_date = parse_date(from_date_raw)
    end_date = parse_date(to_date_raw)

    if not from_date_raw or not to_date_raw:
        return None, "Please select from and to dates"
    if not start_date or not end_date:
        return None, "Please enter valid from and to dates."
    if end_date < start_date:
        return None, "To date must be greater than or equal to from date."

    start_bound, end_bound = local_date_range_to_storage_bounds(start_date, end_date)

    sales_total = db.session.query(db.func.coalesce(db.func.sum(Invoice.subtotal), 0)).filter(
        Invoice.created_at >= start_bound,
        Invoice.created_at < end_bound
    ).scalar() or 0
    returns_total = db.session.query(db.func.coalesce(db.func.sum(ReturnItem.net_amount), 0)).join(
        Return, ReturnItem.return_id == Return.id
    ).filter(
        Return.created_at >= start_bound,
        Return.created_at < end_bound,
        falsey_or_null_column_expr(Return.is_cancelled)
    ).scalar() or 0
    cogs = db.session.query(db.func.coalesce(db.func.sum(InvoiceItem.cost_amount), 0)).join(
        Invoice, InvoiceItem.invoice_id == Invoice.id
    ).filter(
        Invoice.created_at >= start_bound,
        Invoice.created_at < end_bound
    ).scalar() or 0
    return_cogs = db.session.query(db.func.coalesce(db.func.sum(ReturnItem.cost_amount), 0)).join(
        Return, ReturnItem.return_id == Return.id
    ).filter(
        Return.created_at >= start_bound,
        Return.created_at < end_bound,
        falsey_or_null_column_expr(Return.is_cancelled)
    ).scalar() or 0

    net_sales = sales_total - returns_total
    net_cogs = cogs - return_cogs
    gross_profit = net_sales - net_cogs
    gross_profit_percentage = (gross_profit / net_sales * 100) if net_sales else 0

    return {
        "from_date": start_date.isoformat(),
        "to_date": end_date.isoformat(),
        "sales_total": round(sales_total, 2),
        "returns_total": round(returns_total, 2),
        "net_sales": round(net_sales, 2),
        "cogs": round(cogs, 2),
        "return_cogs": round(return_cogs, 2),
        "net_cogs": round(net_cogs, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_profit_percentage": round(gross_profit_percentage, 2)
    }, None

def build_medicine_report_data(from_date_raw="", to_date_raw="", medicine_query="", top_n=10):
    from_date_raw = (from_date_raw or "").strip()
    to_date_raw = (to_date_raw or "").strip()
    query_text = (medicine_query or "").strip().lower()
    top_n = to_int_safe(top_n, 10)
    if top_n < 1:
        top_n = 10
    if top_n > 100:
        top_n = 100

    errors = []
    start_date = parse_date(from_date_raw) if from_date_raw else None
    end_date = parse_date(to_date_raw) if to_date_raw else None
    if from_date_raw and not start_date:
        errors.append("Invalid from date")
    if to_date_raw and not end_date:
        errors.append("Invalid to date")

    result = {
        "medicine_summary": [],
        "fast_movers": [],
        "medicine_totals": None,
        "top_n": top_n
    }

    if start_date and end_date and end_date < start_date:
        errors.append("To date must be greater than or equal to from date")
        return result, errors

    start_bound = None
    end_bound = None
    if start_date:
        start_bound, _ = local_date_range_to_storage_bounds(start_date, start_date)
    if end_date:
        _, end_bound = local_date_range_to_storage_bounds(end_date, end_date)

    period_days = 0
    if start_date and end_date:
        period_days = (end_date - start_date).days + 1
    elif start_date and not end_date:
        period_days = (clinic_now().date() - start_date).days + 1
    elif end_date and not start_date:
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
                "inward_purchase_value": 0.0,
                "gross_sales_value": 0.0,
                "return_sales_value": 0.0,
                "sales_value": 0.0,
                "purchase_amount": 0.0,
                "return_purchase_amount": 0.0,
                "net_purchase_amount": 0.0,
                "net_profit": 0.0,
                "profit_percent": 0.0,
                "avg_daily_sale": 0.0
            }
        return rows[key]

    for med in Medicine.query.all():
        row = get_row(med.name)
        if not row:
            continue
        row["current_stock"] += to_int(med.qty)

    purchase_query = VendorPurchaseItem.query
    if start_bound:
        purchase_query = purchase_query.filter(VendorPurchaseItem.created_at >= start_bound)
    if end_bound:
        purchase_query = purchase_query.filter(VendorPurchaseItem.created_at < end_bound)
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
        row["inward_purchase_value"] += to_float(item.total_value)

    sales_query = InvoiceItem.query.join(Invoice, InvoiceItem.invoice_id == Invoice.id)
    if start_bound:
        sales_query = sales_query.filter(Invoice.created_at >= start_bound)
    if end_bound:
        sales_query = sales_query.filter(Invoice.created_at < end_bound)
    for item in sales_query.all():
        row = get_row(item.name)
        if not row:
            continue
        row["sold_qty"] += to_int(item.qty)
        sales_value = item.net_amount if item.net_amount not in (None, 0) else item.amount
        row["gross_sales_value"] += to_float(sales_value)
        row["purchase_amount"] += to_float(item.cost_amount)

    return_query = ReturnItem.query.join(Return, ReturnItem.return_id == Return.id).filter(
        falsey_or_null_column_expr(Return.is_cancelled)
    )
    if start_bound:
        return_query = return_query.filter(Return.created_at >= start_bound)
    if end_bound:
        return_query = return_query.filter(Return.created_at < end_bound)
    for item in return_query.all():
        row = get_row(item.medicine_name)
        if not row:
            continue
        row["return_qty"] += to_int(item.qty)
        return_sales_value = item.net_amount if item.net_amount not in (None, 0) else item.amount
        row["return_sales_value"] += to_float(return_sales_value)
        row["return_purchase_amount"] += to_float(item.cost_amount)

    for row in rows.values():
        row["net_sold_qty"] = row["sold_qty"] - row["return_qty"]
        row["sales_value"] = row["gross_sales_value"] - row["return_sales_value"]
        row["net_purchase_amount"] = row["purchase_amount"] - row["return_purchase_amount"]
        row["net_profit"] = row["sales_value"] - row["net_purchase_amount"]
        row["profit_percent"] = round((row["net_profit"] / row["sales_value"] * 100), 2) if row["sales_value"] else 0.0
        if period_days > 0:
            row["avg_daily_sale"] = round(row["net_sold_qty"] / period_days, 2)
        else:
            row["avg_daily_sale"] = round(float(row["net_sold_qty"]), 2)
        row["inward_purchase_value"] = round(row["inward_purchase_value"], 2)
        row["gross_sales_value"] = round(row["gross_sales_value"], 2)
        row["return_sales_value"] = round(row["return_sales_value"], 2)
        row["purchase_amount"] = round(row["purchase_amount"], 2)
        row["return_purchase_amount"] = round(row["return_purchase_amount"], 2)
        row["net_purchase_amount"] = round(row["net_purchase_amount"], 2)
        row["sales_value"] = round(row["sales_value"], 2)
        row["net_profit"] = round(row["net_profit"], 2)

    medicine_summary = list(rows.values())
    if query_text:
        medicine_summary = [
            row for row in medicine_summary
            if query_text in (row["medicine"] or "").lower()
        ]
    medicine_summary.sort(key=lambda row: (row["medicine"] or "").lower())

    fast_movers = [row for row in medicine_summary if row["net_sold_qty"] > 0]
    fast_movers.sort(
        key=lambda row: (row["avg_daily_sale"], row["net_sold_qty"], row["sales_value"]),
        reverse=True
    )

    result["medicine_summary"] = medicine_summary
    result["fast_movers"] = fast_movers[:top_n]
    result["medicine_totals"] = {
        "count": len(medicine_summary),
        "purchase_qty": sum(row["purchase_qty"] for row in medicine_summary),
        "free_qty": sum(row["free_qty"] for row in medicine_summary),
        "inward_qty": sum(row["inward_qty"] for row in medicine_summary),
        "sold_qty": sum(row["sold_qty"] for row in medicine_summary),
        "return_qty": sum(row["return_qty"] for row in medicine_summary),
        "net_sold_qty": sum(row["net_sold_qty"] for row in medicine_summary),
        "current_stock": sum(row["current_stock"] for row in medicine_summary),
        "inward_purchase_value": round(sum(row["inward_purchase_value"] for row in medicine_summary), 2),
        "gross_sales_value": round(sum(row["gross_sales_value"] for row in medicine_summary), 2),
        "return_sales_value": round(sum(row["return_sales_value"] for row in medicine_summary), 2),
        "purchase_amount": round(sum(row["purchase_amount"] for row in medicine_summary), 2),
        "return_purchase_amount": round(sum(row["return_purchase_amount"] for row in medicine_summary), 2),
        "net_purchase_amount": round(sum(row["net_purchase_amount"] for row in medicine_summary), 2),
        "sales_value": round(sum(row["sales_value"] for row in medicine_summary), 2),
        "net_profit": round(sum(row["net_profit"] for row in medicine_summary), 2),
        "period_days": period_days
    }
    return result, errors

@app.route("/medicines/add", methods=["GET", "POST"])
@login_required
def add_medicine():
    user = active_user_by_id(session.get("user_id"))
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
        barcode = (request.form.get("barcode") or "").strip()
        reorder_level = to_int_safe(request.form.get("reorder_level"), 10)
        if reorder_level < 0:
            reorder_level = 0
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
            pack_qty=pack_qty,
            discount_percent=to_int_safe(request.form.get("discount_percent"), 0),
            barcode=barcode,
            reorder_level=reorder_level,
            is_active=True
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
        record_audit_event(
            action="Created medicine",
            entity_type="MEDICINE",
            entity_id=med.id,
            ref_code=f"{med.name} / {med.batch}",
            before=None,
            after=build_medicine_audit_snapshot(med)
        )

        flash("Medicine added successfully", "success")
        return redirect("/medicines")

    return render_template("add_medicine.html")

@app.route("/medicines/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_medicine(id):
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_edit_medicine:
        flash("Access denied", "danger")
        return redirect("/medicines")
    med = Medicine.query.get_or_404(id)
    old_qty = med.qty

    if request.method == "POST":
        before_snapshot = build_medicine_audit_snapshot(med)
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
        med.barcode = (request.form.get("barcode") or "").strip()
        med.reorder_level = max(0, to_int_safe(request.form.get("reorder_level"), 10))
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
        record_audit_event(
            action="Updated medicine",
            entity_type="MEDICINE",
            entity_id=med.id,
            ref_code=f"{med.name} / {med.batch}",
            before=before_snapshot,
            after=build_medicine_audit_snapshot(med)
        )
        flash("Medicine updated successfully", "success")
        return redirect("/medicines")

    return render_template("edit_medicine.html", med=med)

@app.route("/medicines/delete/<int:id>")
@login_required
def delete_medicine(id):
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_delete_medicine:
        flash("Access denied", "danger")
        return redirect("/medicines")
    med = Medicine.query.get_or_404(id)
    before_snapshot = build_medicine_audit_snapshot(med)

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
    record_audit_event(
        action="Deleted medicine",
        entity_type="MEDICINE",
        entity_id=before_snapshot.get("id") if before_snapshot else None,
        ref_code=f"{before_snapshot.get('name', '')} / {before_snapshot.get('batch', '')}" if before_snapshot else "",
        before=before_snapshot,
        after=None
    )

    flash("Medicine deleted successfully", "danger")
    return redirect("/medicines")


# ---------------- BILLING ----------------
@app.route("/billing", methods=["GET", "POST"])
@login_required
@invoice_access_required
def billing():
    meds = Medicine.query.order_by(Medicine.name).all()
    
    cart = []
    posted_hold_bill_id = to_int_safe(request.form.get("hold_bill_id"), 0) if request.method == "POST" else 0

    def redirect_to_billing_with_context():
        if posted_hold_bill_id > 0:
            return redirect(url_for("billing", hold_bill_id=posted_hold_bill_id))
        return redirect("/billing")

    if request.method == "POST":
        validation_error = validate_billing_submission(request.form)
        if validation_error:
            flash(validation_error, "danger")
            return redirect_to_billing_with_context()
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
        patient, normalized_mobile = upsert_patient_from_invoice(customer, mobile, request.form.get("gender", ""))
        if patient and not patient.id:
            db.session.flush()
        inv = Invoice(
            invoice_no=inv_no,
            patient_id=patient.id if patient else None,
            customer=customer,
            mobile=normalized_mobile or mobile,
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
            held_bill = active_hold_bill_query().filter(HoldBill.id == posted_hold_bill_id).first()
            if held_bill:
                held_bill.is_deleted = True
                held_bill.deleted_at = datetime.utcnow()
                held_bill.deleted_by = session.get("username")

        db.session.commit()
        record_audit_event(
            action="Created invoice",
            entity_type="INVOICE",
            entity_id=inv.id,
            ref_code=inv.invoice_no,
            before=None,
            after=build_invoice_audit_snapshot(inv),
            extra={
                "item_count": len(cart),
                "payment_mode": inv.payment_mode
            }
        )

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
        hold_bill = active_hold_bill_query().filter(HoldBill.id == restore_hold_bill_id).first()
        if not hold_bill:
            flash("Held bill not found. It may have been deleted.", "warning")
        else:
            restored_hold_bill = normalize_hold_bill_data(hold_bill)

    return render_template(
        "billing.html",
        **prepare_billing_context(meds, restored_hold_bill)
    )


# ---------------- RETURN MEDICINE ----------------
@app.route("/return-medicine", methods=["GET", "POST"])
@login_required
@invoice_access_required
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
        return_validation_error = validate_return_submission(request.form)
        if return_validation_error:
            flash(return_validation_error, "danger")
            return redirect("/return-medicine")

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

                total_refund += net_amt
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
                falsey_or_null_column_expr(Return.is_cancelled)
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
        refund_multiplier = 1.0
        if to_float(invoice.subtotal) > 0 and to_float(invoice.total) > 0:
            refund_multiplier = to_float(invoice.total) / to_float(invoice.subtotal)

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
        ret.total_refund = round(subtotal_return * refund_multiplier, 2)
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
                    falsey_or_null_column_expr(Return.is_cancelled)
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
@invoice_access_required
def return_invoice(id):
    ret = Return.query.get_or_404(id)
    items = ReturnItem.query.filter_by(return_id=id).all()
    return_no = ret.return_no or f"RB-{ret.id:06d}"
    subtotal = sum((i.net_amount or 0) for i in items)
    total_refund = ret.total_refund or round(subtotal, 2)

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
@invoice_access_required
def hold_bill():
    items = build_hold_items_from_form(request.form)
    draft_items = build_hold_rows_from_form(request.form, include_partial=True)
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
        "draft_items": draft_items,
        "items": items,
        "totals": build_hold_totals_from_form(request.form, items)
    }

    hold_bill = active_hold_bill_query().filter(HoldBill.id == hold_bill_id).first() if hold_bill_id > 0 else None
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
@invoice_access_required
def pending_bills():
    bills = active_hold_bill_query().order_by(HoldBill.id.desc()).all()
    return render_template("pending_bills.html", bills=bills)

@app.route("/restore-bill/<int:id>")
@login_required
@invoice_access_required
def restore_bill(id):
    hb = active_hold_bill_query().filter(HoldBill.id == id).first()
    if not hb:
        flash("Held bill not found.", "warning")
        return redirect("/pending-bills")
    return redirect(url_for("billing", hold_bill_id=hb.id))

@app.route("/delete-hold/<int:id>")
@login_required
@invoice_access_required
def delete_hold(id):
    hold_bill = active_hold_bill_query().filter(HoldBill.id == id).first_or_404()
    hold_bill.is_deleted = True
    hold_bill.deleted_at = datetime.utcnow()
    hold_bill.deleted_by = session.get("username")
    db.session.commit()
    flash("Pending bill removed from the active hold list.", "success")
    return redirect("/pending-bills")

# ---------------- CANCEL RETURN BILL ----------------
@app.route("/return-bill/delete/<int:id>", methods=["POST"])
@login_required
@invoice_access_required
def delete_return_bill(id):
    ret = Return.query.get_or_404(id)
    if ret.is_cancelled:
        flash("Return bill already cancelled.", "info")
        return redirect("/return-bills")

    items = ReturnItem.query.filter_by(return_id=id).all()
    item_medicines = {}
    required_stock = {}
    for it in items:
        med = None
        if it.medicine_id:
            med = Medicine.query.get(it.medicine_id)
        if not med:
            med = Medicine.query.filter_by(name=it.medicine_name, batch=it.batch).first()
        if not med:
            flash(f"Medicine not found for {it.medicine_name} ({it.batch}).", "danger")
            return redirect("/return-bills")
        item_medicines[it.id] = med
        required_stock[med.id] = {
            "med": med,
            "qty": required_stock.get(med.id, {}).get("qty", 0) + to_int(it.qty)
        }

    for entry in required_stock.values():
        med = entry["med"]
        if to_int(med.qty) < entry["qty"]:
            flash(
                f"Cannot cancel: stock for {med.name} ({med.batch}) is less than total return qty.",
                "danger"
            )
            return redirect("/return-bills")

    for it in items:
        med = item_medicines.get(it.id)
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
@invoice_access_required
def return_bills():
    returns = Return.query.order_by(Return.id.desc()).all()
    return render_template("return_bills.html", returns=returns)

# ---------------- INVOICES LIST ----------------
@app.route("/invoices")
@login_required
@invoice_access_required
def invoices():
    from_str = request.args.get("from", "").strip()
    to_str = request.args.get("to", "").strip()
    search_query = (request.args.get("search") or "").strip()
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
    if search_query:
        like = f"%{search_query}%"
        digits = normalize_patient_mobile(search_query)
        conditions = [
            Invoice.invoice_no.ilike(like),
            Invoice.customer.ilike(like)
        ]
        if digits:
            conditions.append(Invoice.mobile.ilike(f"%{digits}%"))
        query = query.filter(or_(*conditions))
    return render_template(
        "invoices.html",
        invoices=query.order_by(Invoice.id.desc()).all(),
        search_query=search_query
    )

# ---------------- PART-3: VIEW / PRINT INVOICE ----------------
@app.route("/invoice/<int:id>")
@login_required
@invoice_access_required
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
@invoice_access_required
def edit_invoice(id):
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (user.can_edit_invoice or user.can_invoice_action):
        flash("Access denied", "danger")
        return redirect("/invoices")
    invoice = Invoice.query.get_or_404(id)
    items = InvoiceItem.query.filter_by(invoice_id=id).all()
    payment_modes = ("CASH", "ONLINE", "UPI", "CARD", "WALLET", "ADJUSTMENT")

    if request.method == "POST":
        before_snapshot = build_invoice_audit_snapshot(invoice)
        customer = (request.form.get("customer") or "").strip()
        mobile = (request.form.get("mobile") or "").strip()
        payment_mode = (request.form.get("payment_mode") or "CASH").strip().upper() or "CASH"

        if not customer:
            flash("Patient name is required.", "danger")
            return redirect(f"/invoice/edit/{invoice.id}")
        if payment_mode not in payment_modes:
            flash("Invalid payment mode selected.", "danger")
            return redirect(f"/invoice/edit/{invoice.id}")

        invoice.customer = customer
        invoice.mobile = mobile
        invoice.payment_mode = payment_mode
        patient, normalized_mobile = upsert_patient_from_invoice(customer, mobile, invoice.gender)
        if patient and not patient.id:
            db.session.flush()
        invoice.patient_id = patient.id if patient else invoice.patient_id
        invoice.mobile = normalized_mobile or mobile

        db.session.commit()
        record_audit_event(
            action="Updated invoice details",
            entity_type="INVOICE",
            entity_id=invoice.id,
            ref_code=invoice.invoice_no,
            before=before_snapshot,
            after=build_invoice_audit_snapshot(invoice)
        )
        flash("Invoice details updated successfully.", "success")
        return redirect(f"/invoice/{invoice.id}")


    return render_template(
        "edit_invoice.html",
        invoice=invoice,
        items=items,
        payment_modes=payment_modes
    )
@app.route("/delete-invoice/<int:id>")
@login_required
def delete_invoice(id):
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not (user.can_delete_invoice or user.can_invoice_action):
        flash("Access denied", "danger")
        return redirect("/invoices")
    inv = Invoice.query.get_or_404(id)
    before_snapshot = build_invoice_audit_snapshot(inv)
    has_returns = Return.query.filter(
        Return.invoice_id == inv.id,
        falsey_or_null_column_expr(Return.is_cancelled)
    ).first()
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
    record_audit_event(
        action="Deleted invoice",
        entity_type="INVOICE",
        entity_id=before_snapshot.get("id") if before_snapshot else None,
        ref_code=before_snapshot.get("invoice_no", "") if before_snapshot else "",
        before=before_snapshot,
        after=None
    )
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
@inventory_access_required
def vendor():
    vendors = active_vendor_query().order_by(Vendor.name).all()
    return render_template("vendor.html", vendors=vendors)


@app.route("/vendor/attachment/<path:filename>")
@login_required
@inventory_access_required
def view_vendor_attachment(filename):
    cleaned_name = secure_filename(filename or "")
    if not cleaned_name:
        return "Attachment not found.", 404
    file_path = os.path.join(UPLOAD_FOLDER, cleaned_name)
    if not os.path.exists(file_path):
        return "Attachment file not found on server.", 404
    return send_from_directory(UPLOAD_FOLDER, cleaned_name, as_attachment=False)

@app.route("/vendor-notes")
@login_required
@vendor_note_access_required()
def vendor_notes():
    vendors = active_vendor_query().order_by(Vendor.name.asc()).all()
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


@app.route("/vendor-note-invoice/<int:note_id>")
@login_required
@vendor_note_access_required()
def vendor_note_invoice(note_id):
    note = VendorNote.query.get_or_404(note_id)
    vendor = Vendor.query.get(note.vendor_id) if note.vendor_id else None
    ref_purchase = VendorPurchase.query.get(note.reference_purchase_id) if note.reference_purchase_id else None
    ref_bill_no = None
    if ref_purchase:
        ref_bill_no = ref_purchase.invoice_no if ref_purchase.invoice_no else (ref_purchase.purchase_no or f"PB-{ref_purchase.id:06d}")

    note_items = VendorNoteItem.query.filter_by(note_id=note.id).order_by(VendorNoteItem.id.asc()).all()
    med_ids = [it.medicine_id for it in note_items if it.medicine_id]
    med_map = {}
    if med_ids:
        meds = Medicine.query.filter(Medicine.id.in_(med_ids)).all()
        med_map = {m.id: m.name for m in meds}

    items = []
    for idx, it in enumerate(note_items, start=1):
        items.append({
            "sr_no": idx,
            "medicine_name": med_map.get(it.medicine_id) or "-",
            "batch_no": it.batch_no or "-",
            "expiry": it.expiry or "-",
            "qty": to_int(it.qty),
            "free_qty": to_int(it.free_qty),
            "purchase_rate": to_float(it.purchase_rate),
            "mrp": to_float(it.mrp),
            "gst_percent": to_float(it.gst_percent),
            "disc_percent": to_float(it.disc_percent),
            "line_total": to_float(it.line_total)
        })

    auto_print = (request.args.get("print") or "").strip() == "1"
    return render_template(
        "vendor_note_invoice.html",
        note=note,
        vendor=vendor,
        ref_bill_no=ref_bill_no,
        items=items,
        note_date=note.note_date.strftime("%d-%m-%Y") if note.note_date else "-",
        created_at=note.created_at.strftime("%d-%m-%Y %I:%M %p") if note.created_at else "-",
        printed_at=datetime.now().strftime("%d-%m-%Y %I:%M %p"),
        auto_print=auto_print
    )

@app.route("/vendor/reports")
@login_required
@inventory_access_required
def vendor_reports():
    vendors = active_vendor_query().order_by(Vendor.name).all()
    return render_vendor_reports_page(vendors)

@app.route("/vendor/medicine-history")
@login_required
@inventory_access_required
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
@inventory_access_required
def add_vendor():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Vendor name is required", "danger")
            return redirect("/vendor/add")
        existing = active_vendor_query().filter(db.func.lower(Vendor.name) == name.lower()).first()
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

    return render_template("vendor_form.html", vendor=None, medicine_names=[])

@app.route("/vendor/edit/<int:id>", methods=["GET", "POST"])
@login_required
@inventory_access_required
def edit_vendor(id):
    v = active_vendor_query().filter(Vendor.id == id).first_or_404()

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Vendor name is required", "danger")
            return redirect(f"/vendor/edit/{id}")
        existing = active_vendor_query().filter(db.func.lower(Vendor.name) == name.lower(), Vendor.id != v.id).first()
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
    medicine_names = build_medicine_name_suggestions(medicines)
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
        medicine_names=medicine_names,
        purchase_items=purchase_items,
        last_rates=last_rates,
        purchase_bills=purchase_bills
    )

@app.route("/vendor/<int:id>/purchase", methods=["POST"])
@login_required
@inventory_access_required
def add_vendor_purchase(id):
    vendor = active_vendor_query().filter(Vendor.id == id).first_or_404()

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
    bill_attachment_file = request.files.get("bill_attachment_file")
    if bill_attachment_file and (bill_attachment_file.filename or "").strip() and not allowed_file(bill_attachment_file.filename):
        flash("Bill photo must be PDF, JPG or PNG", "danger")
        return redirect(f"/vendor/edit/{vendor.id}")

    names = request.form.getlist("medicine_name")
    compositions = request.form.getlist("composition")
    companies = request.form.getlist("company")
    distributors = request.form.getlist("distributor_name")
    pack_types = request.form.getlist("pack_type")
    pack_qtys = request.form.getlist("pack_qty")
    batches = request.form.getlist("batch")
    barcodes = request.form.getlist("barcode")
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
        barcode = (barcodes[idx] or "").strip() if idx < len(barcodes) else ""
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
            "barcode": barcode,
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
    if bill_attachment_file and (bill_attachment_file.filename or "").strip():
        purchase.bill_attachment_ref = save_uploaded_file(
            bill_attachment_file,
            VENDOR_BILL_UPLOAD_FOLDER,
            f"vendorbill_{vendor.id}_{purchase.id}"
        )

    for item in line_items:
        med = find_medicine_by_name_batch(item["name"], item["batch"])
        if not med:
            discount_template = find_medicine_discount_template(
                item["name"],
                item["pack_type"],
                item.get("pack_qty")
            )
            med = Medicine(
                name=item["name"],
                composition=item["composition"],
                company=item["company"],
                pack_type=item["pack_type"],
                pack_qty=item["pack_qty"],
                batch=item["batch"],
                barcode=item["barcode"] or "",
                expiry=item["expiry"],
                mrp=item["mrp"] or 0,
                qty=0,
                # Keep selling discount in sync across new batches of the same medicine.
                discount_percent=int(
                    to_float_safe(
                        getattr(discount_template, "discount_percent", item["discount_percent"]),
                        item["discount_percent"]
                    ) or 0
                )
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
        if item["barcode"]:
            med.barcode = item["barcode"]

        effective_barcode = item["barcode"] or ((getattr(med, "barcode", "") or "").strip())

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
            barcode=effective_barcode,
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
@inventory_access_required
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

@app.route("/vendor/purchase/<int:purchase_id>/bill-file")
@login_required
@inventory_access_required
def view_vendor_purchase_bill_file(purchase_id):
    purchase = VendorPurchase.query.get_or_404(purchase_id)
    filename = (purchase.bill_attachment_ref or "").strip()
    if not filename:
        return "No bill photo uploaded for this purchase.", 404
    file_path = os.path.join(VENDOR_BILL_UPLOAD_FOLDER, filename)
    if not os.path.exists(file_path):
        return "Bill photo file not found on server.", 404
    return send_from_directory(VENDOR_BILL_UPLOAD_FOLDER, filename, as_attachment=False)

@app.route("/vendor/purchase/<int:purchase_id>/bill-file/upload", methods=["POST"])
@login_required
@inventory_access_required
def upload_vendor_purchase_bill_file(purchase_id):
    purchase = VendorPurchase.query.get_or_404(purchase_id)
    bill_attachment_file = request.files.get("bill_attachment_file")
    if not bill_attachment_file or not (bill_attachment_file.filename or "").strip():
        flash("Please choose a bill photo or PDF to upload", "danger")
        return redirect(url_for("view_vendor_purchase", purchase_id=purchase.id))
    if not allowed_file(bill_attachment_file.filename):
        flash("Bill photo must be PDF, JPG or PNG", "danger")
        return redirect(url_for("view_vendor_purchase", purchase_id=purchase.id))

    old_attachment = purchase.bill_attachment_ref or ""
    purchase.bill_attachment_ref = save_uploaded_file(
        bill_attachment_file,
        VENDOR_BILL_UPLOAD_FOLDER,
        f"vendorbill_{purchase.vendor_id}_{purchase.id}"
    )
    db.session.commit()
    if old_attachment and old_attachment != purchase.bill_attachment_ref:
        delete_uploaded_file(VENDOR_BILL_UPLOAD_FOLDER, old_attachment)
    flash("Bill photo uploaded successfully", "success")
    return redirect(url_for("view_vendor_purchase", purchase_id=purchase.id))

@app.route("/vendor/purchase/delete/<int:purchase_id>")
@login_required
@inventory_access_required
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
    bill_attachment_ref = purchase.bill_attachment_ref or ""
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
    if bill_attachment_ref:
        delete_uploaded_file(VENDOR_BILL_UPLOAD_FOLDER, bill_attachment_ref)
    if legacy_partial_unknown:
        flash(
            f"Bill {bill_no} deleted. Outstanding balance for old partial bill was not auto-adjusted (paid amount missing).",
            "warning"
        )
    flash(f"Bill {bill_no} deleted. Linked purchase entries and stock updates removed.", "warning")
    return redirect(f"/vendor/edit/{purchase.vendor_id}")

@app.route("/vendor/purchase-item/edit/<int:item_id>", methods=["GET", "POST"])
@login_required
@inventory_access_required
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
        barcode = (request.form.get("barcode") or "").strip()
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
            if pack_qty_raw:
                med.pack_qty = pack_qty
            if barcode:
                med.barcode = barcode

        item.medicine_name = name
        if barcode:
            item.barcode = barcode
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

    resolved_barcode = (getattr(item, "barcode", "") or (getattr(med, "barcode", "") if med else "") or "")
    return render_template(
        "edit_purchase_item.html",
        item=item,
        vendor=vendor,
        purchase=purchase,
        sold_qty=sold_qty,
        resolved_barcode=resolved_barcode
    )

@app.route("/api/vendor-purchases/<int:purchase_id>/items", methods=["GET"])
@login_required
@vendor_note_access_required(api=True)
def api_vendor_purchase_items(purchase_id):
    purchase = VendorPurchase.query.get_or_404(purchase_id)
    items = VendorPurchaseItem.query.filter_by(purchase_id=purchase.id).order_by(VendorPurchaseItem.id.asc()).all()

    payload_items = []
    for it in items:
        payload_items.append({
            "id": it.id,
            "medicine_id": it.medicine_id,
            "medicine_name": it.medicine_name,
            "batch": it.batch,
            "barcode": getattr(it, "barcode", "") or "",
            "expiry": it.expiry,
            "qty": to_int(it.qty),
            "free_qty": to_int(it.free_qty),
            "remaining_qty": to_int(it.remaining_qty),
            "purchase_rate": to_float(it.purchase_rate),
            "mrp": to_float(it.mrp),
            "gst_percent": to_float(it.gst_percent),
            "disc_percent": to_float(it.discount_percent),
            "hsn": ""
        })

    return jsonify({
        "purchase_id": purchase.id,
        "vendor_id": purchase.vendor_id,
        "invoice_no": purchase.invoice_no,
        "purchase_no": purchase.purchase_no,
        "items": payload_items
    }), 200

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
        applied = adjust_vendor_outstanding(note.vendor_id, -to_decimal(note.grand_total or 0), return_applied=True)
        note.outstanding_impact = quantize_decimal(applied, "0.0001")

        note.status = "POSTED"
        note.posted_at = datetime.utcnow()
        db.session.add(note)

    elif note.note_type == "CREDIT":
        if to_decimal(note.grand_total) <= 0:
            return jsonify({"error": "grand_total must be > 0"}), 400
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
        applied = adjust_vendor_outstanding(note.vendor_id, to_decimal(note.grand_total or 0), return_applied=True)
        note.outstanding_impact = quantize_decimal(applied, "0.0001")

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
    elif note.note_type == "CREDIT":
        reversal_debit = quantize_decimal(note.grand_total or 0, "0.0001")

    reverse_delta = -get_note_outstanding_impact(note)
    adjust_vendor_outstanding(note.vendor_id, reverse_delta)

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


@app.route("/api/vendor-notes/<int:note_id>", methods=["DELETE"])
@login_required
@vendor_note_access_required(api=True)
def api_delete_vendor_note(note_id):
    note = VendorNote.query.get_or_404(note_id)
    items = VendorNoteItem.query.filter_by(note_id=note.id).all()
    allocations = VendorNoteAllocation.query.filter_by(note_id=note.id).all()

    if note.status == "POSTED":
        if note.note_type == "DEBIT":
            stock_restore = []
            purchase_restore = []

            for it in items:
                total_qty = to_int(it.qty) + to_int(it.free_qty)
                if total_qty <= 0:
                    continue
                med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
                if not med:
                    return jsonify({"error": "Cannot delete: linked medicine missing for posted debit note"}), 400
                stock_restore.append((med, total_qty))

            if allocations:
                for alloc in allocations:
                    qty = to_int(alloc.qty)
                    if qty <= 0:
                        continue
                    pi = VendorPurchaseItem.query.get(alloc.purchase_item_id)
                    if not pi:
                        return jsonify({"error": "Cannot delete: linked purchase allocation missing"}), 400
                    purchase_restore.append((pi, qty))
            else:
                for it in items:
                    total_qty = to_int(it.qty) + to_int(it.free_qty)
                    if total_qty <= 0:
                        continue
                    med = Medicine.query.get(it.medicine_id) if it.medicine_id else None
                    if not med:
                        return jsonify({"error": "Cannot delete: linked medicine missing for purchase restore"}), 400
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
                        purchase_restore.append((pi, take))
                        remaining -= take
                    if remaining > 0:
                        return jsonify({"error": "Cannot delete: purchase restore mapping failed"}), 400

            for med, qty in stock_restore:
                med.qty = to_int(med.qty) + qty
            for pi, qty in purchase_restore:
                pi.remaining_qty = to_int(pi.remaining_qty) + qty

        reverse_delta = -get_note_outstanding_impact(note)
        adjust_vendor_outstanding(note.vendor_id, reverse_delta)

    StockHistory.query.filter_by(ref_table="vendor_notes", ref_id=note.id).delete(synchronize_session=False)
    VendorLedgerEntry.query.filter_by(ref_table="vendor_notes", ref_id=note.id).delete(synchronize_session=False)
    VendorNoteAllocation.query.filter_by(note_id=note.id).delete(synchronize_session=False)
    VendorNoteItem.query.filter_by(note_id=note.id).delete(synchronize_session=False)
    db.session.delete(note)
    db.session.commit()
    return jsonify({"message": "Vendor note deleted"}), 200

@app.route("/vendor/delete/<int:id>")
@login_required
@inventory_access_required
def delete_vendor(id):
    v = active_vendor_query().filter(Vendor.id == id).first_or_404()
    has_purchases = VendorPurchase.query.filter_by(vendor_id=v.id).first()
    if has_purchases:
        flash("Vendor cannot be deleted because purchase history exists.", "danger")
        return redirect("/vendor")
    v.is_active = False
    v.deleted_at = datetime.utcnow()
    v.deleted_by = session.get("username")
    db.session.commit()
    flash("Vendor archived successfully.", "success")
    return redirect("/vendor")

def build_patient_invoice_query(patient):
    patient_name = (patient.name or "").strip()
    mobile_digits = normalize_patient_mobile(patient.mobile)
    conditions = [Invoice.patient_id == patient.id]
    if mobile_digits:
        conditions.append(Invoice.mobile.ilike(f"%{mobile_digits}%"))
    if patient_name:
        conditions.append(
            db.func.lower(db.func.coalesce(Invoice.customer, "")) == patient_name.lower()
        )
    if not conditions:
        return Invoice.query.filter(text("1=0"))
    return Invoice.query.filter(or_(*conditions))

def build_patient_appointment_query(patient):
    patient_name = (patient.name or "").strip()
    mobile_digits = normalize_patient_mobile(patient.mobile)
    conditions = [Appointment.patient_id == patient.id]
    if mobile_digits:
        conditions.append(Appointment.mobile == mobile_digits)
    if patient_name:
        conditions.append(
            db.func.lower(db.func.coalesce(Appointment.patient_name, "")) == patient_name.lower()
        )
    return active_appointment_query().filter(or_(*conditions))

def build_global_search_results(raw_query, per_group=4):
    query = (raw_query or "").strip()
    if len(query) < 2:
        return []

    query_lower = query.lower()
    search_digits = normalize_patient_mobile(query)
    per_group = max(1, min(to_int_safe(per_group, 4), 8))
    expanded_limit = max(per_group * 3, 8)

    def score_text(value):
        text_value = (value or "").strip().lower()
        if not text_value:
            return 0
        if text_value == query_lower:
            return 120
        if text_value.startswith(query_lower):
            return 90
        if query_lower in text_value:
            return 50
        return 0

    def score_digits(value):
        digits_value = normalize_patient_mobile(value)
        if not search_digits or not digits_value:
            return 0
        if digits_value == search_digits:
            return 110
        if digits_value.startswith(search_digits):
            return 85
        if search_digits in digits_value:
            return 45
        return 0

    patient_filters = [Patient.name.ilike(f"%{query}%")]
    if search_digits:
        patient_filters.append(Patient.mobile.ilike(f"%{search_digits}%"))
    patient_rows = Patient.query.filter(or_(*patient_filters)).order_by(
        Patient.updated_at.desc(),
        Patient.id.desc()
    ).limit(expanded_limit).all()
    patient_rows.sort(
        key=lambda row: (
            -max(score_digits(row.mobile), score_text(row.name)),
            -(row.updated_at.timestamp() if row.updated_at else 0),
            -(row.id or 0)
        )
    )

    can_edit_medicine = bool(
        session.get("role") == "admin" or session.get("can_edit_medicine")
    )
    medicine_filters = [
        Medicine.name.ilike(f"%{query}%"),
        Medicine.batch.ilike(f"%{query}%")
    ]
    if query:
        medicine_filters.append(Medicine.barcode.ilike(f"%{query}%"))
    medicine_rows = Medicine.query.filter(or_(*medicine_filters)).order_by(
        db.func.lower(Medicine.name).asc(),
        Medicine.id.desc()
    ).limit(expanded_limit).all()
    medicine_rows.sort(
        key=lambda row: (
            -max(score_text(row.name), score_text(row.batch), score_text(getattr(row, "barcode", ""))),
            (row.name or "").lower(),
            -(row.id or 0)
        )
    )

    invoice_filters = [
        Invoice.invoice_no.ilike(f"%{query}%"),
        Invoice.customer.ilike(f"%{query}%")
    ]
    if search_digits:
        invoice_filters.append(Invoice.mobile.ilike(f"%{search_digits}%"))
    invoice_rows = Invoice.query.filter(or_(*invoice_filters)).order_by(
        Invoice.created_at.desc(),
        Invoice.id.desc()
    ).limit(expanded_limit).all()
    invoice_rows.sort(
        key=lambda row: (
            -max(score_text(row.invoice_no), score_digits(row.mobile), score_text(row.customer)),
            -(row.created_at.timestamp() if row.created_at else 0),
            -(row.id or 0)
        )
    )

    appointment_filters = [
        Appointment.appointment_no.ilike(f"%{query}%"),
        Appointment.patient_name.ilike(f"%{query}%")
    ]
    if search_digits:
        appointment_filters.append(Appointment.mobile.ilike(f"%{search_digits}%"))
    appointment_rows = active_appointment_query().filter(or_(*appointment_filters)).order_by(
        Appointment.appointment_date.desc(),
        Appointment.id.desc()
    ).limit(expanded_limit).all()
    appointment_rows.sort(
        key=lambda row: (
            -max(score_text(row.appointment_no), score_digits(row.mobile), score_text(row.patient_name)),
            -(row.appointment_date.toordinal() if row.appointment_date else 0),
            -(row.id or 0)
        )
    )

    groups = []
    if patient_rows:
        patient_items = [
            {
                "title": (patient.name or "Unnamed Patient").strip(),
                "subtitle": (patient.mobile or "No mobile").strip() or "No mobile",
                "meta": f"Updated {patient.updated_at.strftime('%d-%m-%Y') if patient.updated_at else '-'}",
                "href": url_for("patient_profile", patient_id=patient.id)
            }
            for patient in patient_rows[:per_group]
        ]
        groups.append({
            "key": "patients",
            "label": "Patients",
            "items": patient_items,
            "hint": f"Showing top {len(patient_items)} patient matches",
            "view_all_href": url_for("customer", search=query)
        })
    if medicine_rows:
        medicine_items = []
        for med in medicine_rows[:per_group]:
            if can_edit_medicine:
                href = url_for("edit_medicine", id=med.id)
            else:
                search_value = " ".join(
                    part for part in (
                        (med.name or "").strip(),
                        (med.batch or "").strip(),
                        (getattr(med, "barcode", "") or "").strip()
                    ) if part
                )
                search_kwargs = {"search": search_value}
                if to_int_safe(med.qty, 0) <= 0:
                    search_kwargs["show_archived"] = 1
                href = url_for("medicines", **search_kwargs)
            medicine_items.append({
                "title": (med.name or "Medicine").strip(),
                "subtitle": f"Batch {(med.batch or '-').strip()} · Stock {to_int_safe(med.qty, 0)}",
                "meta": f"MRP Rs {to_float_safe(med.mrp, 0):.2f}",
                "href": href
            })
        groups.append({
            "key": "medicines",
            "label": "Medicines",
            "items": medicine_items,
            "hint": f"Showing top {len(medicine_items)} medicine matches",
            "view_all_href": url_for("medicines", search=query)
        })
    if invoice_rows:
        invoice_items = [
            {
                "title": (inv.invoice_no or f"Invoice #{inv.id}").strip(),
                "subtitle": f"{(inv.customer or 'Walk-in').strip()} · {(inv.mobile or '-').strip() or '-'}",
                "meta": f"Rs {to_float_safe(inv.total, 0):.2f} · {inv.created_at.strftime('%d-%m-%Y') if inv.created_at else '-'}",
                "href": url_for("view_invoice", id=inv.id)
            }
            for inv in invoice_rows[:per_group]
        ]
        groups.append({
            "key": "invoices",
            "label": "Invoices",
            "items": invoice_items,
            "hint": f"Showing top {len(invoice_items)} invoice matches"
        })
    if appointment_rows:
        appointment_items = [
            {
                "title": (appt.appointment_no or f"Appointment #{appt.id}").strip(),
                "subtitle": f"{(appt.patient_name or 'Patient').strip()} · {(appt.mobile or '-').strip() or '-'}",
                "meta": f"{appt.appointment_date.strftime('%d-%m-%Y') if appt.appointment_date else '-'} · {(appt.status or 'BOOKED').replace('_', ' ').title()}",
                "href": url_for("edit_appointment", id=appt.id)
            }
            for appt in appointment_rows[:per_group]
        ]
        groups.append({
            "key": "appointments",
            "label": "Appointments",
            "items": appointment_items,
            "hint": f"Showing top {len(appointment_items)} appointment matches"
        })
    return groups

@app.route("/customer")
@login_required
def customer():
    search_query = (request.args.get("search") or "").strip()
    with_mobile_only = is_truthy(request.args.get("with_mobile"))
    with_notes_only = is_truthy(request.args.get("with_notes"))
    duplicates_only = is_truthy(request.args.get("duplicates"))
    sort_by = (request.args.get("sort") or "recent").strip().lower() or "recent"
    query = Patient.query
    duplicate_name_counts = build_patient_duplicate_name_counts()
    duplicate_names = sorted(duplicate_name_counts.keys())
    if search_query:
        conditions = [Patient.name.ilike(f"%{search_query}%")]
        digits = normalize_patient_mobile(search_query)
        if digits:
            conditions.append(Patient.mobile.ilike(f"%{digits}%"))
        query = query.filter(or_(*conditions))
    if with_mobile_only:
        query = query.filter(db.func.length(db.func.trim(db.func.coalesce(Patient.mobile, ""))) > 0)
    if with_notes_only:
        query = query.filter(db.func.length(db.func.trim(db.func.coalesce(Patient.notes, ""))) > 0)
    if duplicates_only:
        if duplicate_names:
            query = query.filter(patient_master_name_expression().in_(duplicate_names))
        else:
            query = query.filter(text("1=0"))

    matching_patients = query.count()
    patients = query.order_by(Patient.updated_at.desc(), Patient.id.desc()).limit(300).all()
    customer_rows = build_customer_directory_rows(patients)
    for row in customer_rows:
        normalized_name = normalize_patient_name(row["patient"].name)
        row["duplicate_count"] = max(0, duplicate_name_counts.get(normalized_name, 0) - 1)

    if sort_by == "name":
        customer_rows.sort(key=lambda row: ((row["patient"].name or "").lower(), -(row["patient"].id or 0)))
    elif sort_by == "visits":
        customer_rows.sort(
            key=lambda row: (
                -row["appointment_count"],
                -(row["invoice_count"]),
                -to_float_safe(row["total_billed"], 0),
                (row["patient"].name or "").lower()
            )
        )
    elif sort_by == "billing":
        customer_rows.sort(
            key=lambda row: (
                -to_float_safe(row["total_billed"], 0),
                -row["invoice_count"],
                -row["appointment_count"],
                (row["patient"].name or "").lower()
            )
        )
    else:
        customer_rows.sort(
            key=lambda row: (
                -(row["last_activity_at"].timestamp() if row["last_activity_at"] else 0),
                -(row["patient"].updated_at.timestamp() if row["patient"].updated_at else 0),
                -(row["patient"].id or 0)
            )
        )

    customer_rows = customer_rows[:250]
    customer_stats = {
        "matching_patients": matching_patients,
        "visible_patients": len(customer_rows),
        "with_mobile": sum(1 for row in customer_rows if row["has_mobile"]),
        "with_notes": sum(1 for row in customer_rows if row["has_notes"]),
        "duplicate_patients": sum(1 for row in customer_rows if row["duplicate_count"] > 0),
        "total_visits": sum(row["appointment_count"] for row in customer_rows),
        "total_invoices": sum(row["invoice_count"] for row in customer_rows),
        "total_billed": round(sum(to_float_safe(row["total_billed"], 0) for row in customer_rows), 2)
    }
    return render_template(
        "customers.html",
        customer_rows=customer_rows,
        search_query=search_query,
        customer_stats=customer_stats,
        with_mobile_only=with_mobile_only,
        with_notes_only=with_notes_only,
        duplicates_only=duplicates_only,
        sort_by=sort_by
    )

@app.route("/patients/<int:patient_id>")
@login_required
def patient_profile(patient_id):
    patient = Patient.query.get_or_404(patient_id)
    management_enabled = can_manage_patient_records()
    appointment_query = build_patient_appointment_query(patient)
    invoice_query = build_patient_invoice_query(patient)
    appointment_count = appointment_query.count()
    invoice_count = invoice_query.count()
    total_billed = round(
        invoice_query.with_entities(
            db.func.coalesce(db.func.sum(Invoice.total), 0)
        ).scalar() or 0,
        2
    )
    appointment_status_counts = {status: 0 for status in APPOINTMENT_STATUSES}
    for status, count in appointment_query.with_entities(
        Appointment.status,
        db.func.count(Appointment.id)
    ).group_by(Appointment.status).all():
        normalized_status = (status or "BOOKED").strip().upper()
        if normalized_status in appointment_status_counts:
            appointment_status_counts[normalized_status] = int(count or 0)

    invoice_payment_counts = {}
    for mode, count in invoice_query.with_entities(
        Invoice.payment_mode,
        db.func.count(Invoice.id)
    ).group_by(Invoice.payment_mode).all():
        normalized_mode = (mode or "CASH").strip().upper() or "CASH"
        invoice_payment_counts[normalized_mode] = int(count or 0)

    appointments = appointment_query.order_by(
        Appointment.appointment_date.desc(),
        Appointment.appointment_time.desc(),
        Appointment.id.desc()
    ).limit(40).all()
    invoices = invoice_query.order_by(
        Invoice.created_at.desc(),
        Invoice.id.desc()
    ).limit(40).all()

    invoice_ids = [invoice.id for invoice in invoices]
    invoice_items = []
    if invoice_ids:
        invoice_items = InvoiceItem.query.filter(InvoiceItem.invoice_id.in_(invoice_ids)).all()

    medicine_summary = {}
    for item in invoice_items:
        key = (item.name or "").strip().upper()
        if not key:
            continue
        row = medicine_summary.get(key)
        if not row:
            row = {
                "medicine": (item.name or "").strip(),
                "qty": 0,
                "amount": 0.0,
                "last_batch": (item.batch or "").strip()
            }
            medicine_summary[key] = row
        row["qty"] += to_int_safe(item.qty, 0)
        row["amount"] += to_float_safe(item.net_amount if item.net_amount not in (None, 0) else item.amount, 0)
        if (item.batch or "").strip():
            row["last_batch"] = (item.batch or "").strip()

    medicine_rows = sorted(
        medicine_summary.values(),
        key=lambda row: (-row["qty"], (row["medicine"] or "").lower())
    )[:20]

    timeline = []
    for appt in appointments[:12]:
        occurred_at = None
        if appt.appointment_date:
            occurred_at = datetime.combine(
                appt.appointment_date,
                appt.appointment_time or time.min
            )
        timeline.append({
            "type": "Appointment",
            "title": (appt.appointment_no or f"Appointment #{appt.id}").strip(),
            "subtitle": f"Token {appt.token_no or '-'} · {(appt.status or 'BOOKED').replace('_', ' ').title()}",
            "amount": round(to_float_safe(appt.consultation_fee, 0), 2),
            "occurred_at": occurred_at,
            "href": url_for("edit_appointment", id=appt.id)
        })

    for invoice in invoices[:12]:
        timeline.append({
            "type": "Invoice",
            "title": (invoice.invoice_no or f"Invoice #{invoice.id}").strip(),
            "subtitle": f"{(invoice.payment_mode or 'CASH').strip().upper()} · {(invoice.mobile or patient.mobile or '-').strip() or '-'}",
            "amount": round(to_float_safe(invoice.total, 0), 2),
            "occurred_at": invoice.created_at,
            "href": url_for("view_invoice", id=invoice.id)
        })

    timeline.sort(
        key=lambda row: row["occurred_at"] or datetime.min,
        reverse=True
    )
    timeline = timeline[:12]
    duplicate_candidates = build_patient_duplicate_candidates(patient)
    link_summary = build_patient_link_summary(patient)
    profile_search_value = patient.mobile or patient.name or ""
    stats = {
        "appointment_count": appointment_count,
        "invoice_count": invoice_count,
        "medicine_count": len(medicine_summary),
        "total_billed": total_billed,
        "last_invoice_at": invoices[0].created_at if invoices else None,
        "last_appointment_at": appointments[0].appointment_date if appointments else None,
        "completed_visits": appointment_status_counts.get("COMPLETED", 0),
        "cancelled_visits": appointment_status_counts.get("CANCELLED", 0),
        "paid_invoice_count": sum(invoice_payment_counts.values()),
        "cash_invoice_count": invoice_payment_counts.get("CASH", 0),
        "online_invoice_count": sum(
            count for mode, count in invoice_payment_counts.items() if mode != "CASH"
        )
    }
    return render_template(
        "patient_profile.html",
        patient=patient,
        appointments=appointments,
        invoices=invoices,
        medicine_rows=medicine_rows,
        stats=stats,
        management_enabled=management_enabled,
        duplicate_candidates=duplicate_candidates,
        link_summary=link_summary,
        profile_search_value=profile_search_value,
        appointment_status_counts=appointment_status_counts,
        invoice_payment_counts=invoice_payment_counts,
        timeline=timeline
    )

@app.route("/patients/<int:patient_id>/notes", methods=["POST"])
@login_required
def update_patient_notes(patient_id):
    patient = Patient.query.get_or_404(patient_id)
    before_snapshot = build_patient_audit_snapshot(patient)
    patient.notes = (request.form.get("notes") or "").strip()
    db.session.commit()
    record_audit_event(
        action="Updated patient notes",
        entity_type="PATIENT",
        entity_id=patient.id,
        ref_code=patient.mobile or patient.name,
        before=before_snapshot,
        after=build_patient_audit_snapshot(patient)
    )
    flash("Patient notes updated successfully.", "success")
    return redirect(url_for("patient_profile", patient_id=patient.id))

@app.route("/patients/<int:patient_id>/update", methods=["POST"])
@login_required
def update_patient_profile(patient_id):
    patient = Patient.query.get_or_404(patient_id)
    if not can_manage_patient_records():
        flash("Access denied", "danger")
        return redirect(url_for("patient_profile", patient_id=patient.id))

    before_snapshot = build_patient_audit_snapshot(patient)
    old_name = patient.name
    old_mobile = patient.mobile

    name = (request.form.get("name") or "").strip()
    mobile = normalize_patient_mobile(request.form.get("mobile"))
    gender = (request.form.get("gender") or "").strip().upper() or None
    age_raw = (request.form.get("age") or "").strip()
    sync_linked_records = is_truthy(request.form.get("sync_linked_records"))

    if not name:
        flash("Patient name is required.", "danger")
        return redirect(url_for("patient_profile", patient_id=patient.id))

    age_value = None
    if age_raw:
        try:
            age_value = int(age_raw)
        except ValueError:
            flash("Age must be a valid number.", "danger")
            return redirect(url_for("patient_profile", patient_id=patient.id))

    existing_mobile = None
    if mobile:
        existing_mobile = Patient.query.filter(
            Patient.mobile == mobile,
            Patient.id != patient.id
        ).first()
    if existing_mobile:
        flash("This mobile number is already linked to another patient profile.", "danger")
        return redirect(url_for("patient_profile", patient_id=patient.id))

    patient.name = name
    patient.mobile = mobile or None
    patient.gender = gender
    patient.age = age_value

    sync_result = {
        "appointment_relinked": 0,
        "appointment_synced": 0,
        "invoice_relinked": 0,
        "invoice_synced": 0
    }
    if sync_linked_records:
        sync_result = relink_patient_records(
            patient,
            matching_names=[old_name, patient.name],
            matching_mobiles=[old_mobile, patient.mobile],
            sync_display_fields=True
        )

    db.session.commit()
    record_audit_event(
        action="Updated patient profile",
        entity_type="PATIENT",
        entity_id=patient.id,
        ref_code=patient.mobile or patient.name,
        before=before_snapshot,
        after=build_patient_audit_snapshot(patient),
        extra={
            "sync_linked_records": sync_linked_records,
            **sync_result
        }
    )
    flash("Patient profile updated successfully.", "success")
    return redirect(url_for("patient_profile", patient_id=patient.id))

@app.route("/patients/<int:patient_id>/relink", methods=["POST"])
@login_required
def relink_patient_profile(patient_id):
    patient = Patient.query.get_or_404(patient_id)
    if not can_manage_patient_records():
        flash("Access denied", "danger")
        return redirect(url_for("patient_profile", patient_id=patient.id))

    before_snapshot = build_patient_audit_snapshot(patient)
    sync_display_fields = not str(request.form.get("sync_display_fields") or "").strip().lower() == "0"
    relink_result = relink_patient_records(
        patient,
        matching_names=[patient.name],
        matching_mobiles=[patient.mobile],
        sync_display_fields=sync_display_fields
    )
    db.session.commit()
    record_audit_event(
        action="Relinked patient records",
        entity_type="PATIENT",
        entity_id=patient.id,
        ref_code=patient.mobile or patient.name,
        before=before_snapshot,
        after=build_patient_audit_snapshot(patient),
        extra={
            "sync_display_fields": sync_display_fields,
            **relink_result
        }
    )
    flash(
        "Linked records refreshed. "
        f"Appointments relinked: {relink_result['appointment_relinked']}, "
        f"Invoices relinked: {relink_result['invoice_relinked']}.",
        "success"
    )
    return redirect(url_for("patient_profile", patient_id=patient.id))

@app.route("/patients/<int:patient_id>/merge", methods=["POST"])
@login_required
def merge_patient_profile(patient_id):
    target = Patient.query.get_or_404(patient_id)
    if not can_manage_patient_records():
        flash("Access denied", "danger")
        return redirect(url_for("patient_profile", patient_id=target.id))

    source_id = to_int_safe(request.form.get("source_patient_id"), 0)
    source = Patient.query.get_or_404(source_id)
    if source.id == target.id:
        flash("Select a different patient to merge.", "danger")
        return redirect(url_for("patient_profile", patient_id=target.id))

    before_target = build_patient_audit_snapshot(target)
    before_source = build_patient_audit_snapshot(source)
    adopted_source_mobile = None

    if not (target.name or "").strip() and (source.name or "").strip():
        target.name = source.name
    if not (target.gender or "").strip() and (source.gender or "").strip():
        target.gender = source.gender
    if target.age in (None, "") and source.age not in (None, ""):
        target.age = source.age

    if not (target.mobile or "").strip() and (source.mobile or "").strip():
        adopted_source_mobile = source.mobile
        source.mobile = None
        db.session.flush()
        target.mobile = adopted_source_mobile

    merged_note_parts = []
    if (source.notes or "").strip():
        source_note_text = source.notes.strip()
        if source_note_text not in (target.notes or ""):
            merged_note_parts.append(source_note_text)
    if source.mobile and source.mobile != target.mobile:
        merged_note_parts.append(f"Alternate mobile from merged profile: {source.mobile}")
    if merged_note_parts:
        base_notes = (target.notes or "").strip()
        joiner = "\n\n" if base_notes else ""
        merged_note_text = "\n\n".join(merged_note_parts)
        target.notes = f"{base_notes}{joiner}{merged_note_text}".strip()

    merge_result = relink_patient_records(
        target,
        matching_names=[target.name, before_target.get("name"), source.name],
        matching_mobiles=[target.mobile, before_target.get("mobile"), source.mobile, adopted_source_mobile],
        source_patient_ids=[source.id],
        sync_display_fields=True
    )

    db.session.delete(source)
    db.session.commit()
    record_audit_event(
        action="Merged patient records",
        entity_type="PATIENT",
        entity_id=target.id,
        ref_code=target.mobile or target.name,
        before={
            "target": before_target,
            "source": before_source
        },
        after=build_patient_audit_snapshot(target),
        extra=merge_result
    )
    flash(
        f"Merged patient #{source_id} into profile #{target.id}. "
        f"Appointments relinked: {merge_result['appointment_relinked']}, "
        f"Invoices relinked: {merge_result['invoice_relinked']}.",
        "success"
    )
    return redirect(url_for("patient_profile", patient_id=target.id))

@app.route("/api/global-search")
@login_required
def api_global_search():
    query = (request.args.get("q") or "").strip()
    return jsonify({
        "query": query,
        "groups": build_global_search_results(query)
    })

@app.route("/audit-logs")
@login_required
def audit_logs():
    user = active_user_by_id(session.get("user_id"))
    if not user or (
        user.role != "admin"
        and not user.can_manage_users
        and not getattr(user, "can_view_audit_logs", False)
    ):
        flash("Access denied", "danger")
        return redirect("/")

    search_query = (request.args.get("search") or "").strip()
    entity_filter = (request.args.get("entity") or "").strip().upper()
    action_filter = (request.args.get("action") or "").strip().upper()
    query = AuditLog.query
    if search_query:
        like = f"%{search_query}%"
        query = query.filter(or_(
            AuditLog.user.ilike(like),
            AuditLog.action.ilike(like),
            AuditLog.entity_type.ilike(like),
            AuditLog.ref_code.ilike(like)
        ))
    if entity_filter:
        query = query.filter(AuditLog.entity_type == entity_filter)
    if action_filter == "CREATE":
        query = query.filter(or_(
            AuditLog.action.ilike("%create%"),
            AuditLog.action.ilike("%added%"),
            AuditLog.action.ilike("%booked%"),
            AuditLog.action.ilike("new %")
        ))
    elif action_filter == "UPDATE":
        query = query.filter(or_(
            AuditLog.action.ilike("%update%"),
            AuditLog.action.ilike("%edit%"),
            AuditLog.action.ilike("%change%")
        ))
    elif action_filter == "STATUS":
        query = query.filter(or_(
            AuditLog.action.ilike("%status%"),
            AuditLog.action.ilike("%paid%"),
            AuditLog.action.ilike("%cancel%"),
            AuditLog.action.ilike("%posted%"),
            AuditLog.action.ilike("%restored%")
        ))
    elif action_filter == "DELETE":
        query = query.filter(or_(
            AuditLog.action.ilike("%delete%"),
            AuditLog.action.ilike("%remove%")
        ))

    raw_logs = query.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).limit(250).all()
    logs = []
    entity_values = set()
    action_stats = {
        "CREATE": 0,
        "UPDATE": 0,
        "STATUS": 0,
        "DELETE": 0,
        "OTHER": 0
    }
    for row in raw_logs:
        before_data = parse_json_text(row.before_json)
        after_data = parse_json_text(row.after_json)
        extra_data = parse_json_text(row.extra_json)
        action_category = classify_audit_action(row.action)
        entity_values.add((row.entity_type or "").strip().upper())
        change_rows, change_count = build_audit_change_rows(before_data, after_data)
        action_stats[action_category] = action_stats.get(action_category, 0) + 1
        logs.append({
            "id": row.id,
            "user": row.user,
            "action": row.action,
            "action_category": action_category,
            "entity_type": row.entity_type,
            "entity_id": row.entity_id,
            "ref_code": row.ref_code,
            "before_data": before_data,
            "after_data": after_data,
            "extra_data": extra_data,
            "change_rows": change_rows,
            "change_count": change_count,
            "created_at": row.created_at
        })

    return render_template(
        "audit_logs.html",
        logs=logs,
        search_query=search_query,
        entity_filter=entity_filter,
        action_filter=action_filter,
        action_options=("CREATE", "UPDATE", "STATUS", "DELETE", "OTHER"),
        entity_options=sorted(value for value in entity_values if value),
        audit_stats={
            "visible_logs": len(logs),
            "create_count": action_stats.get("CREATE", 0),
            "update_count": action_stats.get("UPDATE", 0) + action_stats.get("STATUS", 0),
            "delete_count": action_stats.get("DELETE", 0),
            "other_count": action_stats.get("OTHER", 0)
        }
    )

@app.route("/appointments")
@login_required
def appointments():
    return render_appointments_page(
        request.args,
        all_statuses=APPOINTMENT_STATUSES,
        build_appointment_report_payload=build_appointment_report_payload,
        build_appointment_summary=build_appointment_summary,
        build_live_queue_snapshot=build_live_queue_snapshot,
        build_appointment_calendar_days=build_appointment_calendar_days,
        clinic_now=clinic_now,
    )

@app.route("/appointments/queue/live")
@login_required
def appointments_queue_live():
    target_date = parse_date(request.args.get("date")) or clinic_now().date()
    queue_snapshot = build_live_queue_snapshot(target_date)
    return render_template(
        "queue_board.html",
        queue_snapshot=queue_snapshot,
        target_date=target_date
    )

def normalize_patient_mobile(raw_mobile):
    raw = (raw_mobile or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits if digits else ""

def appointment_net_amount(appt):
    fee = to_float_safe(appt.consultation_fee, 0)
    discount = to_float_safe(appt.doctor_discount, 0)
    net = fee - discount
    return round(net if net > 0 else 0, 2)

def build_appointment_summary(appointments):
    counts = {
        "BOOKED": 0,
        "WAITING": 0,
        "CHECKED_IN": 0,
        "IN_CONSULTATION": 0,
        "COMPLETED": 0,
        "CANCELLED": 0
    }
    cash_total = 0.0
    online_total = 0.0
    cash_count = 0
    online_count = 0

    for appt in appointments:
        status = (appt.status or "").strip().upper()
        if status in counts:
            counts[status] += 1

        payment_status = (appt.payment_status or "UNPAID").strip().upper()
        if payment_status != "PAID":
            continue

        net_amount = appointment_net_amount(appt)
        if is_cash_payment_mode(appt.payment_mode):
            cash_total += net_amount
            cash_count += 1
        else:
            online_total += net_amount
            online_count += 1

    cash_total = round(cash_total, 2)
    online_total = round(online_total, 2)
    return {
        "counts": counts,
        "revenue": round(cash_total + online_total, 2),
        "revenue_breakdown": {
            "cash_total": cash_total,
            "online_total": online_total,
            "cash_count": cash_count,
            "online_count": online_count,
            "total": round(cash_total + online_total, 2)
        }
    }

def build_live_queue_snapshot(target_date):
    selected_date = target_date or clinic_now().date()
    appointments = active_appointment_query().filter(
        Appointment.appointment_date == selected_date
    ).order_by(
        Appointment.token_no.asc(),
        Appointment.appointment_time.asc(),
        Appointment.id.asc()
    ).all()

    lanes = {
        "scheduled": [],
        "waiting": [],
        "consulting": [],
        "completed": [],
        "cancelled": []
    }
    active_queue = []

    for appt in appointments:
        status = (appt.status or "BOOKED").strip().upper()
        item = {
            "id": appt.id,
            "appointment_no": appt.appointment_no,
            "patient_id": appt.patient_id,
            "patient_name": appt.patient_name,
            "mobile": appt.mobile,
            "token_no": appt.token_no,
            "appointment_time": appt.appointment_time,
            "status": status,
            "payment_status": (appt.payment_status or "UNPAID").strip().upper(),
            "payment_mode": (appt.payment_mode or "CASH").strip().upper(),
            "consultation_fee": round(to_float_safe(appt.consultation_fee, 0), 2)
        }

        if status == "BOOKED":
            lanes["scheduled"].append(item)
            active_queue.append(item)
        elif status in {"WAITING", "CHECKED_IN"}:
            lanes["waiting"].append(item)
            active_queue.append(item)
        elif status == "IN_CONSULTATION":
            lanes["consulting"].append(item)
            active_queue.append(item)
        elif status == "COMPLETED":
            lanes["completed"].append(item)
        else:
            lanes["cancelled"].append(item)

    current_serving = lanes["consulting"][0] if lanes["consulting"] else (active_queue[0] if active_queue else None)
    next_tokens = [
        item["token_no"]
        for item in active_queue[:6]
        if item["token_no"] not in (None, "", " ")
    ]
    summary = build_appointment_summary(appointments)

    return {
        "target_date": selected_date,
        "lanes": lanes,
        "summary": summary,
        "current_serving": current_serving,
        "next_tokens": next_tokens,
        "board_stats": {
            "total": len(appointments),
            "active": len(active_queue),
            "scheduled": len(lanes["scheduled"]),
            "waiting": len(lanes["waiting"]),
            "consulting": len(lanes["consulting"]),
            "completed": len(lanes["completed"]),
            "cancelled": len(lanes["cancelled"])
        }
    }

def calculate_appointment_revenue(from_date=None, to_date=None):
    query = active_appointment_query().filter(
        db.func.upper(db.func.coalesce(Appointment.payment_status, "UNPAID")) == "PAID"
    )
    if from_date:
        query = query.filter(Appointment.appointment_date >= from_date)
    if to_date:
        query = query.filter(Appointment.appointment_date <= to_date)
    total = 0.0
    for appt in query.all():
        total += appointment_net_amount(appt)
    return round(total, 2)

def calculate_appointment_revenue_breakdown(from_date=None, to_date=None):
    query = active_appointment_query().filter(
        db.func.upper(db.func.coalesce(Appointment.payment_status, "UNPAID")) == "PAID"
    )
    if from_date:
        query = query.filter(Appointment.appointment_date >= from_date)
    if to_date:
        query = query.filter(Appointment.appointment_date <= to_date)

    cash_total = 0.0
    online_total = 0.0
    cash_count = 0
    online_count = 0

    for appt in query.all():
        net_amount = appointment_net_amount(appt)
        if is_cash_payment_mode(appt.payment_mode):
            cash_total += net_amount
            cash_count += 1
        else:
            online_total += net_amount
            online_count += 1

    return {
        "cash_total": round(cash_total, 2),
        "online_total": round(online_total, 2),
        "cash_count": cash_count,
        "online_count": online_count,
        "total": round(cash_total + online_total, 2)
    }

def get_next_daily_token(appt_date, exclude_appointment_id=None):
    query = db.session.query(db.func.max(Appointment.token_no)).filter(
        falsey_or_null_column_expr(Appointment.is_deleted),
        Appointment.appointment_date == appt_date
    )
    if exclude_appointment_id:
        query = query.filter(Appointment.id != exclude_appointment_id)
    max_token = query.scalar() or 0
    return int(max_token) + 1

def upsert_patient_profile(form_data):
    patient_name = (form_data.get("patient_name") or "").strip()
    mobile = normalize_patient_mobile(form_data.get("mobile"))
    age_raw = (form_data.get("age") or "").strip()
    gender = form_data.get("gender")
    previous_visit_notes = (form_data.get("previous_visit_notes") or "").strip()
    age_val = None
    if age_raw:
        try:
            age_val = int(age_raw)
        except ValueError:
            age_val = None

    patient = None
    if mobile:
        # When mobile is provided, treat it as primary identity.
        patient = Patient.query.filter_by(mobile=mobile).first()
        if not patient:
            patient = Patient(name=patient_name, mobile=mobile)
            db.session.add(patient)
    else:
        if patient_name:
            patient = Patient.query.filter(
                db.func.lower(Patient.name) == patient_name.lower()
            ).order_by(Patient.id.desc()).first()
        if not patient:
            patient = Patient(name=patient_name, mobile=None)
            db.session.add(patient)

    patient.name = patient_name
    if mobile:
        patient.mobile = mobile
    patient.age = age_val
    patient.gender = gender
    if previous_visit_notes:
        patient.notes = previous_visit_notes
    return patient, mobile

def safe_upsert_patient_profile(form_data, current_patient_id=None):
    mobile = normalize_patient_mobile(form_data.get("mobile"))
    try:
        patient, mobile = upsert_patient_profile(form_data)
        db.session.flush()
        return patient, mobile, None
    except SQLAlchemyError as exc:
        db.session.rollback()
        app.logger.exception("Patient profile upsert failed; continuing appointment flow")
        patient = None
        if mobile:
            try:
                patient = Patient.query.filter_by(mobile=mobile).first()
            except Exception:
                patient = None
        if not patient and current_patient_id:
            try:
                patient = Patient.query.get(current_patient_id)
            except Exception:
                patient = None
        return patient, mobile, exc

def ensure_appointment_runtime_schema():
    # Defensive runtime check for deployments where migration block was skipped.
    required = {
        "appointment": [
            ("payment_mode", "TEXT"),
            ("payment_status", "TEXT"),
            ("doctor_discount", "REAL"),
            ("consultation_fee", "REAL"),
            ("token_no", "INTEGER"),
            ("patient_id", "INTEGER"),
            ("age", "INTEGER"),
            ("gender", "TEXT"),
            ("symptoms", "TEXT"),
            ("previous_visit_notes", "TEXT")
        ],
        "patient": [
            ("age", "INTEGER"),
            ("gender", "TEXT"),
            ("notes", "TEXT"),
            ("updated_at", "TIMESTAMP")
        ]
    }
    dialect = (db.session.bind.dialect.name if db.session.bind else "").lower()
    errors = []
    try:
        for table_name, cols in required.items():
            for col_name, col_def in cols:
                try:
                    if dialect == "postgresql":
                        db.session.execute(
                            text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_def}')
                        )
                        db.session.commit()
                        continue

                    insp = inspect(db.engine)
                    existing = {c["name"] for c in insp.get_columns(table_name)}
                    if col_name in existing:
                        continue
                    db.session.execute(
                        text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}')
                    )
                    db.session.commit()
                except Exception as exc:
                    db.session.rollback()
                    err_text = str(getattr(exc, "orig", exc))
                    low = err_text.lower()
                    if "duplicate column" in low or "already exists" in low:
                        continue
                    errors.append(f"{table_name}.{col_name}: {err_text}")
    except Exception:
        db.session.rollback()
        app.logger.exception("Runtime appointment schema check failed")
        return False, "Schema check failed"
    if errors:
        app.logger.error("Runtime appointment schema errors: %s", " | ".join(errors))
        return False, errors[0]
    return True, ""

def build_appointment_calendar_days(appointments, calendar_view, focus_date):
    by_date = {}
    for appt in appointments:
        if not appt.appointment_date:
            continue
        by_date.setdefault(appt.appointment_date, []).append(appt)
    for slots in by_date.values():
        slots.sort(key=lambda x: (x.appointment_time or time(0, 0), x.id))

    days = []
    if calendar_view == "week":
        week_start = focus_date - timedelta(days=focus_date.weekday())
        for i in range(7):
            d = week_start + timedelta(days=i)
            days.append({
                "date": d,
                "label": d.strftime("%a, %d %b"),
                "slots": by_date.get(d, [])
            })
    else:
        d = focus_date
        days.append({
            "date": d,
            "label": d.strftime("%A, %d %B %Y"),
            "slots": by_date.get(d, [])
        })
    return days

def build_appointment_report_payload(args, flash_errors=False, include_search=True):
    today = date.today()
    legacy_date = parse_date(args.get("date"))
    report_type = (args.get("appointment_report_type") or "day").strip().lower()
    day_date_raw = (args.get("appointment_day_date") or "").strip()
    if not day_date_raw and legacy_date:
        day_date_raw = legacy_date.isoformat()
    month = to_int_safe(args.get("appointment_month"), 0)
    year = to_int_safe(args.get("appointment_year"), 0)
    from_date_raw = (args.get("appointment_from_date") or "").strip()
    to_date_raw = (args.get("appointment_to_date") or "").strip()
    quick_filter = (args.get("quick_filter") or "").strip().lower()
    search_query = (args.get("search") or "").strip()
    calendar_view = (args.get("calendar_view") or "day").strip().lower()
    if calendar_view not in ("day", "week"):
        calendar_view = "day"

    query = active_appointment_query()
    report_label = "Today"
    focus_date = today

    if quick_filter == "today":
        query = query.filter(Appointment.appointment_date == today)
        report_label = "Today"
        focus_date = today
    elif quick_filter == "tomorrow":
        target = today + timedelta(days=1)
        query = query.filter(Appointment.appointment_date == target)
        report_label = "Tomorrow"
        focus_date = target
    elif quick_filter == "this_week":
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        query = query.filter(
            Appointment.appointment_date >= week_start,
            Appointment.appointment_date <= week_end
        )
        report_label = f"This Week ({week_start.strftime('%d %b')} - {week_end.strftime('%d %b')})"
        focus_date = week_start
        calendar_view = "week"
    elif quick_filter == "pending":
        query = query.filter(Appointment.status.in_(("BOOKED", "WAITING", "CHECKED_IN", "IN_CONSULTATION")))
        report_label = "Pending Appointments"
    elif quick_filter == "completed":
        query = query.filter(Appointment.status == "COMPLETED")
        report_label = "Completed Appointments"
    elif quick_filter == "cancelled":
        query = query.filter(Appointment.status == "CANCELLED")
        report_label = "Cancelled Appointments"
    else:
        if report_type == "day":
            selected_date = parse_date(day_date_raw) or today
            query = query.filter(Appointment.appointment_date == selected_date)
            report_label = selected_date.strftime("%d-%m-%Y")
            focus_date = selected_date
            day_date_raw = selected_date.isoformat()
        elif report_type == "month":
            if month < 1 or month > 12 or year < 1900:
                if flash_errors:
                    flash("Please select a valid month and year for appointment report.", "danger")
                month = today.month
                year = today.year
            query = query.filter(
                db.extract("month", Appointment.appointment_date) == month,
                db.extract("year", Appointment.appointment_date) == year
            )
            report_label = date(year, month, 1).strftime("%B %Y")
            focus_date = date(year, month, 1)
        elif report_type == "date_range":
            start_date = parse_date(from_date_raw)
            end_date = parse_date(to_date_raw)
            if not start_date or not end_date:
                if flash_errors:
                    flash("Please select both From Date and To Date.", "danger")
                start_date = today
                end_date = today
            if end_date < start_date:
                if flash_errors:
                    flash("To Date must be greater than or equal to From Date.", "danger")
                start_date = today
                end_date = today
            query = query.filter(
                Appointment.appointment_date >= start_date,
                Appointment.appointment_date <= end_date
            )
            report_label = f"{start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"
            focus_date = start_date
            from_date_raw = start_date.isoformat()
            to_date_raw = end_date.isoformat()
        elif report_type == "all":
            report_label = "All Appointments"
            focus_date = today
        else:
            selected_date = today
            query = query.filter(Appointment.appointment_date == selected_date)
            report_type = "day"
            day_date_raw = selected_date.isoformat()
            report_label = selected_date.strftime("%d-%m-%Y")
            focus_date = selected_date

    if include_search and search_query:
        like = f"%{search_query}%"
        query = query.filter(or_(
            Appointment.appointment_no.ilike(like),
            Appointment.patient_name.ilike(like),
            Appointment.mobile.ilike(like)
        ))

    appointments = query.order_by(
        Appointment.appointment_date.asc(),
        Appointment.appointment_time.asc(),
        Appointment.id.asc()
    ).all()

    return {
        "appointments": appointments,
        "report_label": report_label,
        "report_filters": {
            "appointment_report_type": report_type,
            "appointment_day_date": day_date_raw or today.isoformat(),
            "appointment_month": month if month > 0 else "",
            "appointment_year": year if year > 0 else "",
            "appointment_from_date": from_date_raw,
            "appointment_to_date": to_date_raw
        },
        "quick_filter": quick_filter,
        "search_query": search_query,
        "calendar_view": calendar_view,
        "focus_date": focus_date
    }

def build_appointment_form_data(appt=None):
    form_data = {
        "patient_name": "",
        "mobile": "",
        "gender": "OTHER",
        "appointment_date": date.today().isoformat(),
        "appointment_time": "",
        "payment_mode": "CASH",
        "doctor_discount": "0",
        "consultation_fee": APPOINTMENT_CONSULTATION_FEES[0],
        "symptoms": "",
        "previous_visit_notes": "",
        "notes": ""
    }
    if appt:
        consultation_fee = to_float_safe(appt.consultation_fee, 0)
        consultation_fee_value = APPOINTMENT_CONSULTATION_FEES[0]
        for allowed_fee in APPOINTMENT_CONSULTATION_FEES:
            if abs(consultation_fee - float(allowed_fee)) < 0.01:
                consultation_fee_value = allowed_fee
                break
        form_data.update({
            "patient_name": appt.patient_name or "",
            "mobile": appt.mobile or "",
            "gender": (appt.gender or "OTHER").upper(),
            "appointment_date": appt.appointment_date.isoformat() if appt.appointment_date else date.today().isoformat(),
            "appointment_time": appt.appointment_time.strftime("%H:%M") if appt.appointment_time else "",
            "payment_mode": (appt.payment_mode or "CASH").strip().upper(),
            "doctor_discount": str(appt.doctor_discount or 0),
            "consultation_fee": consultation_fee_value,
            "symptoms": appt.symptoms or "",
            "previous_visit_notes": appt.previous_visit_notes or "",
            "notes": appt.notes or ""
        })
    if form_data["payment_mode"] not in APPOINTMENT_PAYMENT_MODES:
        form_data["payment_mode"] = "CASH"
    if form_data["gender"] not in APPOINTMENT_GENDERS:
        form_data["gender"] = "OTHER"
    return form_data

def read_appointment_form_data(form_data):
    form_data["patient_name"] = (request.form.get("patient_name") or "").strip()
    form_data["mobile"] = normalize_patient_mobile(request.form.get("mobile"))
    form_data["gender"] = (request.form.get("gender") or "OTHER").strip().upper()
    form_data["appointment_date"] = (request.form.get("appointment_date") or "").strip()
    form_data["appointment_time"] = (request.form.get("appointment_time") or "").strip()
    form_data["payment_mode"] = (request.form.get("payment_mode") or "CASH").strip().upper()
    form_data["doctor_discount"] = (request.form.get("doctor_discount") or "0").strip()
    form_data["consultation_fee"] = (request.form.get("consultation_fee") or "0").strip()
    form_data["symptoms"] = (request.form.get("symptoms") or "").strip()
    form_data["previous_visit_notes"] = (request.form.get("previous_visit_notes") or "").strip()
    form_data["notes"] = (request.form.get("notes") or "").strip()
    return form_data

def validate_appointment_form(form_data):
    appt_date = parse_date(form_data["appointment_date"])
    appt_time = parse_time_value(form_data["appointment_time"])
    if not form_data["patient_name"] or not appt_date or not appt_time:
        return None, "Patient, appointment date and valid time are required."

    if form_data["mobile"] and not form_data["mobile"].isdigit():
        return None, "Mobile number should contain digits only."

    if form_data["payment_mode"] not in APPOINTMENT_PAYMENT_MODES:
        return None, "Invalid payment mode selected."

    gender = (form_data.get("gender") or "OTHER").upper()
    if gender not in APPOINTMENT_GENDERS:
        return None, "Invalid gender selected."

    try:
        doctor_discount = float(form_data["doctor_discount"] or 0)
    except ValueError:
        return None, "Doctor discount should be a valid number."
    if doctor_discount < 0:
        return None, "Doctor discount cannot be negative."

    consultation_fee_raw = (form_data["consultation_fee"] or "").strip()
    if consultation_fee_raw not in APPOINTMENT_CONSULTATION_FEES:
        return None, "Consultation fee must be 600 or 1000."
    consultation_fee = float(consultation_fee_raw)

    return {
        "appointment_date": appt_date,
        "appointment_time": appt_time,
        "gender": gender,
        "doctor_discount": doctor_discount,
        "consultation_fee": consultation_fee
    }, None

def get_patient_suggestions(limit=200):
    return Patient.query.order_by(Patient.updated_at.desc(), Patient.id.desc()).limit(limit).all()

@app.route("/appointments/add", methods=["GET", "POST"])
@login_required
def add_appointment():
    form_data = build_appointment_form_data()
    patient_suggestions = get_patient_suggestions()

    if request.method == "POST":
        schema_ok, schema_err = ensure_appointment_runtime_schema()
        if not schema_ok:
            flash("Unable to save appointment because database schema update failed.", "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=False
            )
        form_data = read_appointment_form_data(form_data)
        validated, error_msg = validate_appointment_form(form_data)
        if error_msg:
            flash(error_msg, "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=False
            )

        appointment_saved = False
        for attempt in range(2):
            try:
                patient, mobile, _patient_err = safe_upsert_patient_profile(form_data)

                appointment = Appointment(
                    appointment_no=generate_appointment_no(),
                    patient_name=form_data["patient_name"],
                    token_no=get_next_daily_token(validated["appointment_date"]),
                    patient_id=patient.id if patient else None,
                    mobile=mobile,
                    age=None,
                    gender=validated["gender"],
                    doctor_name=APPOINTMENT_DEFAULT_DOCTOR,
                    appointment_date=validated["appointment_date"],
                    appointment_time=validated["appointment_time"],
                    payment_mode=form_data["payment_mode"],
                    payment_status="UNPAID",
                    doctor_discount=validated["doctor_discount"],
                    consultation_fee=validated["consultation_fee"],
                    status="BOOKED",
                    symptoms=form_data["symptoms"],
                    previous_visit_notes=form_data["previous_visit_notes"],
                    notes=form_data["notes"],
                    created_by=session.get("username")
                )
                db.session.add(appointment)
                db.session.commit()
                appointment_saved = True
                break
            except IntegrityError as exc:
                db.session.rollback()
                err_text = str(getattr(exc, "orig", exc)).lower()
                if "appointment_no" in err_text and attempt == 0:
                    # Rare race: regenerate appointment number once.
                    continue
                if "mobile" in err_text:
                    flash("Unable to save appointment. Mobile number is already used in another patient profile.", "danger")
                else:
                    flash("Unable to save appointment due to duplicate data. Please try again.", "danger")
                return render_template(
                    "add_appointment.html",
                    form_data=form_data,
                    payment_modes=APPOINTMENT_PAYMENT_MODES,
                    genders=APPOINTMENT_GENDERS,
                    consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                    patient_suggestions=patient_suggestions,
                    edit_mode=False
                )
            except SQLAlchemyError as exc:
                db.session.rollback()
                # Auto-repair + retry once when DB reports missing columns.
                try:
                    err_text = str(getattr(exc, "orig", exc))
                except Exception:
                    err_text = str(exc)
                low = err_text.lower()
                if ("undefined column" in low or "does not exist" in low) and attempt == 0:
                    schema_retry_ok, _ = ensure_appointment_runtime_schema()
                    if schema_retry_ok:
                        continue
                app.logger.exception("Appointment create failed")
                short_err = (err_text or "database error").replace("\n", " ").strip()
                if len(short_err) > 140:
                    short_err = short_err[:140] + "..."
                flash(f"Unable to save appointment due to a database error. {short_err}", "danger")
                return render_template(
                    "add_appointment.html",
                    form_data=form_data,
                    payment_modes=APPOINTMENT_PAYMENT_MODES,
                    genders=APPOINTMENT_GENDERS,
                    consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                    patient_suggestions=patient_suggestions,
                    edit_mode=False
                )
        if not appointment_saved:
            flash("Unable to save appointment. Please try again.", "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=False
            )

        flash("Appointment booked successfully.", "success")
        created_appointment = active_appointment_query().filter_by(
            appointment_no=appointment.appointment_no
        ).first() if appointment_saved else None
        if created_appointment:
            record_audit_event(
                action="Created appointment",
                entity_type="APPOINTMENT",
                entity_id=created_appointment.id,
                ref_code=created_appointment.appointment_no,
                before=None,
                after=build_appointment_audit_snapshot(created_appointment)
            )
        return redirect(url_for(
            "appointments",
            appointment_report_type="day",
            appointment_day_date=validated["appointment_date"].isoformat()
        ))

    return render_template(
        "add_appointment.html",
        form_data=form_data,
        payment_modes=APPOINTMENT_PAYMENT_MODES,
        genders=APPOINTMENT_GENDERS,
        consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
        patient_suggestions=patient_suggestions,
        edit_mode=False
    )

@app.route("/appointments/<int:id>/edit", methods=["GET", "POST"])
@login_required
def edit_appointment(id):
    appointment = active_appointment_query().filter(Appointment.id == id).first_or_404()
    form_data = build_appointment_form_data(appointment)
    patient_suggestions = get_patient_suggestions()

    if request.method == "POST":
        before_snapshot = build_appointment_audit_snapshot(appointment)
        schema_ok, schema_err = ensure_appointment_runtime_schema()
        if not schema_ok:
            flash("Unable to update appointment because database schema update failed.", "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=True,
                appt_id=appointment.id
            )
        form_data = read_appointment_form_data(form_data)
        validated, error_msg = validate_appointment_form(form_data)
        if error_msg:
            flash(error_msg, "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=True,
                appt_id=appointment.id
            )

        try:
            patient, mobile, patient_err = safe_upsert_patient_profile(
                form_data,
                current_patient_id=appointment.patient_id
            )
            if patient_err:
                # Rollback in safe_upsert may detach loaded row; reload it.
                appointment = active_appointment_query().filter(Appointment.id == id).first_or_404()
            old_date = appointment.appointment_date
            appointment.patient_name = form_data["patient_name"]
            appointment.patient_id = patient.id if patient else appointment.patient_id
            appointment.mobile = mobile
            appointment.gender = validated["gender"]
            appointment.appointment_date = validated["appointment_date"]
            appointment.appointment_time = validated["appointment_time"]
            if old_date != appointment.appointment_date or not appointment.token_no:
                appointment.token_no = get_next_daily_token(
                    appointment.appointment_date,
                    exclude_appointment_id=appointment.id
                )
            appointment.payment_mode = form_data["payment_mode"]
            appointment.doctor_discount = validated["doctor_discount"]
            appointment.consultation_fee = validated["consultation_fee"]
            appointment.symptoms = form_data["symptoms"]
            appointment.previous_visit_notes = form_data["previous_visit_notes"]
            appointment.notes = form_data["notes"]
            if (appointment.payment_status or "").strip().upper() not in APPOINTMENT_PAYMENT_STATUSES:
                appointment.payment_status = "UNPAID"

            db.session.commit()
            record_audit_event(
                action="Updated appointment",
                entity_type="APPOINTMENT",
                entity_id=appointment.id,
                ref_code=appointment.appointment_no,
                before=before_snapshot,
                after=build_appointment_audit_snapshot(appointment)
            )
        except IntegrityError as exc:
            db.session.rollback()
            err_text = str(getattr(exc, "orig", exc)).lower()
            if "mobile" in err_text:
                flash("Unable to update appointment. Mobile number is already used in another patient profile.", "danger")
            else:
                flash("Unable to update appointment due to duplicate data. Please check entries.", "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=True,
                appt_id=appointment.id
            )
        except SQLAlchemyError as exc:
            db.session.rollback()
            try:
                err_text = str(getattr(exc, "orig", exc))
            except Exception:
                err_text = str(exc)
            low = err_text.lower()
            if "undefined column" in low or "does not exist" in low:
                ensure_appointment_runtime_schema()
            app.logger.exception("Appointment update failed")
            short_err = (err_text or "database error").replace("\n", " ").strip()
            if len(short_err) > 140:
                short_err = short_err[:140] + "..."
            flash(f"Unable to update appointment due to a database error. {short_err}", "danger")
            return render_template(
                "add_appointment.html",
                form_data=form_data,
                payment_modes=APPOINTMENT_PAYMENT_MODES,
                genders=APPOINTMENT_GENDERS,
                consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
                patient_suggestions=patient_suggestions,
                edit_mode=True,
                appt_id=appointment.id
            )

        flash("Appointment updated successfully.", "success")
        return redirect(url_for(
            "appointments",
            appointment_report_type="day",
            appointment_day_date=validated["appointment_date"].isoformat()
        ))

    return render_template(
        "add_appointment.html",
        form_data=form_data,
        payment_modes=APPOINTMENT_PAYMENT_MODES,
        genders=APPOINTMENT_GENDERS,
        consultation_fee_options=APPOINTMENT_CONSULTATION_FEES,
        patient_suggestions=patient_suggestions,
        edit_mode=True,
        appt_id=appointment.id
    )

@app.route("/appointments/<int:id>/status", methods=["POST"])
@login_required
def update_appointment_status(id):
    appointment = active_appointment_query().filter(Appointment.id == id).first_or_404()
    before_snapshot = build_appointment_audit_snapshot(appointment)
    new_status = (request.form.get("status") or "").strip().upper()

    if new_status not in APPOINTMENT_STATUSES:
        flash("Invalid appointment status.", "danger")
        return redirect(request.referrer or url_for("appointments"))

    current_status = (appointment.status or "BOOKED").strip().upper()
    if new_status != current_status:
        allowed = APPOINTMENT_STATUS_FLOW.get(current_status, set())
        if new_status not in allowed:
            flash(f"Invalid transition: {current_status.replace('_', ' ')} to {new_status.replace('_', ' ')}", "danger")
            return redirect(request.referrer or url_for("appointments"))

    if new_status == "COMPLETED" and (appointment.payment_status or "").strip().upper() != "PAID":
        flash("Mark payment as PAID before completing appointment.", "danger")
        return redirect(request.referrer or url_for("appointments"))

    if new_status == "CANCELLED" and (appointment.payment_status or "").strip().upper() == "PAID":
        flash("Paid appointment cannot be cancelled directly. Handle refund first.", "danger")
        return redirect(request.referrer or url_for("appointments"))

    now = datetime.utcnow()
    appointment.status = new_status
    if new_status in {"WAITING", "CHECKED_IN", "IN_CONSULTATION"}:
        appointment.checked_in_at = appointment.checked_in_at or now
    if new_status == "COMPLETED":
        appointment.completed_at = now
        appointment.checked_in_at = appointment.checked_in_at or now
    elif new_status == "CANCELLED":
        appointment.cancelled_at = now
        appointment.payment_status = "UNPAID"
        appointment.doctor_discount = 0
        appointment.consultation_fee = 0

    db.session.commit()
    record_audit_event(
        action="Updated appointment status",
        entity_type="APPOINTMENT",
        entity_id=appointment.id,
        ref_code=appointment.appointment_no,
        before=before_snapshot,
        after=build_appointment_audit_snapshot(appointment)
    )
    flash("Appointment status updated.", "success")
    return redirect(
        request.referrer or url_for(
            "appointments",
            appointment_report_type="day",
            appointment_day_date=(appointment.appointment_date.isoformat() if appointment.appointment_date else date.today().isoformat())
        )
    )

@app.route("/appointments/<int:id>/payment/paid", methods=["POST"])
@login_required
def mark_appointment_paid(id):
    return handle_mark_appointment_paid(
        id,
        build_appointment_audit_snapshot=build_appointment_audit_snapshot,
        record_audit_event=record_audit_event,
        validate_mark_paid_transition=validate_mark_paid_transition,
    )

@app.route("/appointments/delete/<int:id>", methods=["POST"])
@login_required
def delete_appointment(id):
    appointment = active_appointment_query().filter(Appointment.id == id).first_or_404()
    before_snapshot = build_appointment_audit_snapshot(appointment)
    selected_date = appointment.appointment_date.isoformat() if appointment.appointment_date else date.today().isoformat()
    appointment.is_deleted = True
    appointment.deleted_at = datetime.utcnow()
    appointment.deleted_by = session.get("username")
    db.session.commit()
    record_audit_event(
        action="Archived appointment",
        entity_type="APPOINTMENT",
        entity_id=before_snapshot.get("id") if before_snapshot else None,
        ref_code=before_snapshot.get("appointment_no", "") if before_snapshot else "",
        before=before_snapshot,
        after=None
    )
    flash("Appointment archived successfully.", "success")
    return redirect(request.referrer or url_for("appointments", appointment_report_type="day", appointment_day_date=selected_date))

@app.route("/reports", methods=["GET", "POST"])
@login_required
def reports():
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_reports:
        flash("Access denied", "danger")
        return redirect("/")
    report_validation_error = None
    if request.method == "POST":
        report_validation_error = validate_report_request(request.form, parse_date=parse_date)

    return render_reports_page(
        request_method=request.method,
        form_data=request.form if request.method == "POST" else request.args,
        prevalidated_error=report_validation_error,
        clinic_now=clinic_now,
        parse_date=parse_date,
        to_int_safe=to_int_safe,
        local_date_range_to_storage_bounds=local_date_range_to_storage_bounds,
        normalize_patient_mobile=normalize_patient_mobile,
        or_=or_,
        build_patient_medicine_usage_report=build_patient_medicine_usage_report,
        build_profit_report_summary=build_profit_report_summary,
        build_medicine_report_data=build_medicine_report_data,
    )


@app.route("/reports/export")
@login_required
def export_excel():
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_reports:
        flash("Access denied", "danger")
        return redirect("/")
    import pandas as pd
    from flask import send_file
    from io import BytesIO

    now = clinic_now()
    delivery_mode = (request.args.get("delivery") or "").strip().lower()
    if delivery_mode == "background":
        queued_job = queue_report_export_job(
            app,
            {
                "queued_at": datetime.utcnow().isoformat(),
                "queued_by": session.get("username"),
                "route": "/reports/export",
                "query": {key: value for key, value in request.args.items()},
            }
        )
        if is_async_request():
            return jsonify({
                "ok": True,
                "job_id": queued_job["job_id"],
                "message": "Report export queued for background processing."
            }), 202
        flash(f"Report export queued in background. Job ID: {queued_job['job_id']}", "success")
        return redirect(request.referrer or url_for("reports"))
    db_dialect = db.engine.dialect.name if db.engine else "unknown"
    scope = (request.args.get("scope") or "filtered").strip().lower()
    if scope not in ("filtered", "all"):
        scope = "filtered"

    def build_invoice_rows(rows):
        return [{
            "Invoice ID": i.id,
            "Invoice No": i.invoice_no,
            "Patient Name": i.customer,
            "Mobile": i.mobile,
            "Doctor": i.doctor,
            "Gender": i.gender,
            "Subtotal": i.subtotal,
            "Discount": i.discount,
            "CGST": i.cgst,
            "SGST": i.sgst,
            "Total": i.total,
            "Payment Mode": i.payment_mode,
            "Created By": i.created_by,
            "Created At": i.created_at.strftime("%d-%m-%Y %I:%M %p") if i.created_at else ""
        } for i in rows]

    def build_invoice_rows_legacy(rows):
        return [{
            "Invoice No": i.invoice_no,
            "Patient Name": i.customer,
            "Mobile": i.mobile,
            "Date": i.created_at.strftime("%d-%m-%Y") if i.created_at else "",
            "Total (ex GST)": i.subtotal,
            "Payment Mode": i.payment_mode,
            "User": i.created_by
        } for i in rows]

    def build_invoice_item_rows(rows):
        return [{
            "Item ID": it.id,
            "Invoice ID": it.invoice_id,
            "Name": it.name,
            "Batch": it.batch,
            "Expiry": it.expiry,
            "Qty": it.qty,
            "Price": it.price,
            "Amount": it.amount,
            "Discount %": it.discount_percent,
            "Discount Amount": it.discount_amount,
            "Net Amount": it.net_amount,
            "Cost Price": it.cost_price,
            "Cost Amount": it.cost_amount
        } for it in rows]

    if scope == "filtered":
        report_type = (request.args.get("report_type") or "").strip().lower()
        month = to_int_safe(request.args.get("month"), 0)
        year = to_int_safe(request.args.get("year"), 0)
        from_date = (request.args.get("from_date") or "").strip()
        to_date = (request.args.get("to_date") or "").strip()
        patient = (request.args.get("patient") or "").strip()
        mobile_raw = (request.args.get("mobile") or "").strip()
        search_query = (request.args.get("search_query") or "").strip()
        medicine_query = (request.args.get("medicine_query") or "").strip()
        top_n = request.args.get("top_n")

        query = Invoice.query
        applied_filter = "all"
        if report_type == "daily":
            today = clinic_now().date()
            start_bound, end_bound = local_date_range_to_storage_bounds(today, today)
            query = query.filter(Invoice.created_at >= start_bound, Invoice.created_at < end_bound)
            applied_filter = f"daily ({today.isoformat()})"
        elif report_type == "monthly":
            if month >= 1 and month <= 12 and year >= 1900:
                start_bound, end_bound = local_month_to_storage_bounds(year, month)
                query = query.filter(
                    Invoice.created_at >= start_bound,
                    Invoice.created_at < end_bound
                )
                applied_filter = f"monthly ({year}-{month:02d})"
            else:
                applied_filter = "monthly (invalid month/year, exported all)"
        elif report_type == "custom":
            from_date_value = parse_date(from_date)
            to_date_value = parse_date(to_date)
            if from_date_value and to_date_value and to_date_value >= from_date_value:
                from_dt, to_dt = local_date_range_to_storage_bounds(from_date_value, to_date_value)
                query = query.filter(Invoice.created_at >= from_dt, Invoice.created_at < to_dt)
                applied_filter = f"custom ({from_date} to {to_date})"
            else:
                applied_filter = "custom (invalid dates, exported all)"
        elif report_type == "patient":
            if patient:
                query = query.filter(Invoice.customer.ilike(f"%{patient}%"))
                applied_filter = f"patient ({patient})"
            else:
                applied_filter = "patient (blank, exported all)"
        elif report_type == "mobile":
            if mobile_raw:
                mobile_digits = normalize_patient_mobile(mobile_raw)
                normalized_mobile = db.func.replace(
                    db.func.replace(
                        db.func.replace(
                            db.func.replace(
                                db.func.replace(
                                    db.func.replace(db.func.coalesce(Invoice.mobile, ""), " ", ""),
                                    "-", ""
                                ),
                                "+", ""
                            ),
                            "(", ""
                        ),
                        ")", ""
                    ),
                    ".", ""
                )
                if mobile_digits:
                    query = query.filter(
                        or_(
                            normalized_mobile.like(f"%{mobile_digits}%"),
                            Invoice.mobile.ilike(f"%{mobile_raw}%")
                        )
                    )
                else:
                    query = query.filter(Invoice.mobile.ilike(f"%{mobile_raw}%"))
                applied_filter = f"mobile ({mobile_raw})"
            else:
                applied_filter = "mobile (blank, exported all)"
        elif report_type == "profit":
            profit_summary, profit_error = build_profit_report_summary(from_date, to_date)
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                if profit_summary:
                    pd.DataFrame([{
                        "From Date": profit_summary["from_date"],
                        "To Date": profit_summary["to_date"],
                        "Sales (ex GST)": profit_summary["sales_total"],
                        "Returns (ex GST)": profit_summary["returns_total"],
                        "Net Sales": profit_summary["net_sales"],
                        "COGS": profit_summary["cogs"],
                        "Return COGS": profit_summary["return_cogs"],
                        "Net COGS": profit_summary["net_cogs"],
                        "Gross Profit": profit_summary["gross_profit"],
                        "Gross Profit %": profit_summary["gross_profit_percentage"]
                    }]).to_excel(writer, sheet_name="ProfitSummary", index=False)
                else:
                    pd.DataFrame([{"Error": profit_error or "Unable to build profit report."}]).to_excel(
                        writer,
                        sheet_name="ProfitSummary",
                        index=False
                    )
            output.seek(0)
            filename = f"Profit_Report_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
            return send_file(
                output,
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                max_age=0
            )
        elif report_type == "medicine":
            medicine_report, medicine_errors = build_medicine_report_data(
                from_date,
                to_date,
                medicine_query=medicine_query,
                top_n=top_n
            )
            totals = medicine_report["medicine_totals"] or {
                "count": 0,
                "purchase_qty": 0,
                "free_qty": 0,
                "inward_qty": 0,
                "sold_qty": 0,
                "return_qty": 0,
                "net_sold_qty": 0,
                "current_stock": 0,
                "inward_purchase_value": 0.0,
                "purchase_amount": 0.0,
                "return_purchase_amount": 0.0,
                "net_purchase_amount": 0.0,
                "gross_sales_value": 0.0,
                "return_sales_value": 0.0,
                "sales_value": 0.0,
                "net_profit": 0.0,
                "period_days": 0
            }
            summary_rows = [{
                "From Date": from_date,
                "To Date": to_date,
                "Medicine Filter": medicine_query,
                "Top N": medicine_report["top_n"],
                "Medicines": totals["count"],
                "Purchase Qty": totals["purchase_qty"],
                "Free Qty": totals["free_qty"],
                "Inward Qty": totals["inward_qty"],
                "Sold Qty": totals["sold_qty"],
                "Return Qty": totals["return_qty"],
                "Net Sold Qty": totals["net_sold_qty"],
                "Current Stock": totals["current_stock"],
                "Inward Purchase Value": totals["inward_purchase_value"],
                "Purchase Amount": totals["net_purchase_amount"],
                "Sales Amount": totals["sales_value"],
                "Net Profit": totals["net_profit"],
                "Period Days": totals["period_days"],
                "Errors": " | ".join(medicine_errors)
            }]
            movement_rows = [{
                "Medicine": row["medicine"],
                "Purchased": row["purchase_qty"],
                "Free": row["free_qty"],
                "Inward": row["inward_qty"],
                "Sold": row["sold_qty"],
                "Returned": row["return_qty"],
                "Net Sold": row["net_sold_qty"],
                "Current Stock": row["current_stock"],
                "Inward Purchase Value": row["inward_purchase_value"],
                "Purchase Amount": row["net_purchase_amount"],
                "Sales Amount": row["sales_value"],
                "Net Profit": row["net_profit"],
                "Profit %": row["profit_percent"],
                "Avg Daily Sale": row["avg_daily_sale"]
            } for row in medicine_report["medicine_summary"]]
            fast_rows = [{
                "Rank": idx + 1,
                "Medicine": row["medicine"],
                "Net Sold": row["net_sold_qty"],
                "Avg Daily Sale": row["avg_daily_sale"],
                "Current Stock": row["current_stock"],
                "Sales Amount": row["sales_value"],
                "Net Profit": row["net_profit"]
            } for idx, row in enumerate(medicine_report["fast_movers"])]

            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
                pd.DataFrame(movement_rows).to_excel(writer, sheet_name="MedicineMovement", index=False)
                pd.DataFrame(fast_rows).to_excel(writer, sheet_name="FastMovers", index=False)
            output.seek(0)
            filename = f"Medicine_Report_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
            return send_file(
                output,
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                max_age=0
            )
        elif report_type == "patient_medicine":
            patients, detail_rows, usage_summary, _usage_error = build_patient_medicine_usage_report(
                from_date,
                to_date,
                search_query=search_query
            )

            patient_rows = [{
                "Patient Name": row["patient_name"],
                "Mobile": row["mobile"],
                "Invoice Count": row["invoice_count"],
                "Purchase Count": row["purchase_count"],
                "Distinct Medicines": row["distinct_medicines"],
                "Total Qty": row["total_qty"],
                "Invoice Nos": row["invoice_nos"]
            } for row in patients]

            medicine_rows = [{
                "Patient Name": row["patient_name"],
                "Mobile": row["mobile"],
                "Medicine": row["medicine"],
                "Purchase Count": row["purchase_count"],
                "Total Qty": row["total_qty"],
                "Last Purchase Date": row["last_purchase_date"],
                "Invoice Nos": row["invoice_nos"]
            } for row in detail_rows]

            summary_rows = [{
                "From Date": usage_summary["from_date"] if usage_summary else from_date,
                "To Date": usage_summary["to_date"] if usage_summary else to_date,
                "Search Filter": usage_summary["search_filter"] if usage_summary else (request.args.get("search_query") or ""),
                "Patients": usage_summary["patient_count"] if usage_summary else 0,
                "Patient-Medicine Rows": usage_summary["patient_medicine_count"] if usage_summary else 0,
                "Purchase Count": usage_summary["purchase_count"] if usage_summary else 0,
                "Total Qty": usage_summary["total_qty"] if usage_summary else 0
            }]

            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
                pd.DataFrame(patient_rows).to_excel(writer, sheet_name="Patients", index=False)
                pd.DataFrame(medicine_rows).to_excel(writer, sheet_name="PatientMedicines", index=False)
            output.seek(0)

            filename = f"Patient_Medicine_Usage_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
            return send_file(
                output,
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                max_age=0
            )
        elif report_type:
            applied_filter = f"{report_type} (not invoice list type, exported all)"

        invoices = query.order_by(Invoice.created_at.desc(), Invoice.id.desc()).all()
        invoice_ids = [i.id for i in invoices]
        invoice_items = []
        if invoice_ids:
            invoice_items = InvoiceItem.query.filter(
                InvoiceItem.invoice_id.in_(invoice_ids)
            ).order_by(InvoiceItem.invoice_id.asc(), InvoiceItem.id.asc()).all()

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(build_invoice_rows_legacy(invoices)).to_excel(writer, sheet_name="Reports", index=False)
        output.seek(0)

        filename = f"Pharmacy_Reports_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            max_age=0
        )

    invoices = Invoice.query.order_by(Invoice.created_at.desc(), Invoice.id.desc()).all()
    invoice_items = InvoiceItem.query.order_by(InvoiceItem.id.asc()).all()
    appointments = active_appointment_query().order_by(
        Appointment.appointment_date.desc(),
        Appointment.appointment_time.desc(),
        Appointment.id.desc()
    ).all()
    patients = Patient.query.order_by(Patient.updated_at.desc(), Patient.id.desc()).all()
    medicines = Medicine.query.order_by(Medicine.name.asc(), Medicine.batch.asc(), Medicine.id.asc()).all()
    returns = Return.query.order_by(Return.created_at.desc(), Return.id.desc()).all()
    return_items = ReturnItem.query.order_by(ReturnItem.id.asc()).all()

    appointment_rows = [{
        "Appointment ID": a.id,
        "Appointment No": a.appointment_no,
        "Token No": a.token_no,
        "Patient ID": a.patient_id,
        "Patient Name": a.patient_name,
        "Mobile": a.mobile,
        "Age": a.age,
        "Gender": a.gender,
        "Doctor": a.doctor_name,
        "Appointment Date": a.appointment_date.strftime("%d-%m-%Y") if a.appointment_date else "",
        "Appointment Time": a.appointment_time.strftime("%I:%M %p") if a.appointment_time else "",
        "Payment Mode": a.payment_mode,
        "Payment Status": a.payment_status,
        "Doctor Discount": a.doctor_discount,
        "Consultation Fee": a.consultation_fee,
        "Status": a.status,
        "Symptoms": a.symptoms,
        "Previous Visit Notes": a.previous_visit_notes,
        "Notes": a.notes,
        "Created By": a.created_by,
        "Created At": a.created_at.strftime("%d-%m-%Y %I:%M %p") if a.created_at else ""
    } for a in appointments]

    patient_rows = [{
        "Patient ID": p.id,
        "Name": p.name,
        "Mobile": p.mobile,
        "Age": p.age,
        "Gender": p.gender,
        "Notes": p.notes,
        "Created At": p.created_at.strftime("%d-%m-%Y %I:%M %p") if p.created_at else "",
        "Updated At": p.updated_at.strftime("%d-%m-%Y %I:%M %p") if p.updated_at else ""
    } for p in patients]

    medicine_rows = [{
        "Medicine ID": m.id,
        "Name": m.name,
        "Batch": m.batch,
        "Expiry": m.expiry,
        "Composition": m.composition,
        "Company": m.company,
        "Pack Type": m.pack_type,
        "Pack Qty": m.pack_qty,
        "MRP": m.mrp,
        "Qty": m.qty,
        "Discount %": m.discount_percent,
        "Created At": m.created_at.strftime("%d-%m-%Y %I:%M %p") if m.created_at else ""
    } for m in medicines]

    return_rows = [{
        "Return ID": r.id,
        "Return No": r.return_no,
        "Invoice ID": r.invoice_id,
        "Invoice No": r.invoice_no,
        "Customer": r.customer,
        "Mobile": r.mobile,
        "Total Refund": r.total_refund,
        "CGST": r.cgst,
        "SGST": r.sgst,
        "Payment Mode": r.payment_mode,
        "Is Cancelled": r.is_cancelled,
        "Cancelled By": r.cancelled_by,
        "Cancelled At": r.cancelled_at.strftime("%d-%m-%Y %I:%M %p") if r.cancelled_at else "",
        "Created By": r.created_by,
        "Created At": r.created_at.strftime("%d-%m-%Y %I:%M %p") if r.created_at else ""
    } for r in returns]

    return_item_rows = [{
        "Return Item ID": ri.id,
        "Return ID": ri.return_id,
        "Invoice Item ID": ri.invoice_item_id,
        "Medicine ID": ri.medicine_id,
        "Medicine Name": ri.medicine_name,
        "Batch": ri.batch,
        "Expiry": ri.expiry,
        "Qty": ri.qty,
        "Price": ri.price,
        "Amount": ri.amount,
        "Net Amount": ri.net_amount,
        "Cost Amount": ri.cost_amount
    } for ri in return_items]

    meta_rows = [
        {"Metric": "Generated At", "Value": now.strftime("%d-%m-%Y %I:%M:%S %p")},
        {"Metric": "Database Dialect", "Value": db_dialect},
        {"Metric": "Scope", "Value": "all"},
        {"Metric": "Invoices", "Value": len(invoices)},
        {"Metric": "Invoice Items", "Value": len(invoice_items)},
        {"Metric": "Appointments", "Value": len(appointments)},
        {"Metric": "Patients", "Value": len(patients)},
        {"Metric": "Medicines", "Value": len(medicines)},
        {"Metric": "Returns", "Value": len(returns)},
        {"Metric": "Return Items", "Value": len(return_items)}
    ]

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(meta_rows).to_excel(writer, sheet_name="Meta", index=False)
        pd.DataFrame(build_invoice_rows(invoices)).to_excel(writer, sheet_name="Invoices", index=False)
        pd.DataFrame(build_invoice_item_rows(invoice_items)).to_excel(writer, sheet_name="InvoiceItems", index=False)
        pd.DataFrame(appointment_rows).to_excel(writer, sheet_name="Appointments", index=False)
        pd.DataFrame(patient_rows).to_excel(writer, sheet_name="Patients", index=False)
        pd.DataFrame(medicine_rows).to_excel(writer, sheet_name="Medicines", index=False)
        pd.DataFrame(return_rows).to_excel(writer, sheet_name="Returns", index=False)
        pd.DataFrame(return_item_rows).to_excel(writer, sheet_name="ReturnItems", index=False)

    output.seek(0)
    filename = f"Pharmacy_Reports_Full_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        max_age=0
    )

@app.route("/reports/appointments/export")
@login_required
def export_appointments_excel():
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_reports:
        flash("Access denied", "danger")
        return redirect("/")

    import pandas as pd
    from flask import send_file
    from io import BytesIO

    payload = build_appointment_report_payload(request.args, flash_errors=False)
    appointments = payload["appointments"]
    report_type = payload["report_filters"]["appointment_report_type"]
    filter_label = payload["report_label"].replace(" ", "_")

    rows = []
    for appt in appointments:
        consultation_fee = to_float_safe(appt.consultation_fee, 0)
        doctor_discount = to_float_safe(appt.doctor_discount, 0)
        net_fee = consultation_fee - doctor_discount
        if net_fee < 0:
            net_fee = 0
        payment_status = (
            appt.payment_status
            or ("PAID" if (appt.payment_mode or "").strip().upper() == "PAID" else "UNPAID")
        )
        rows.append({
            "Appointment No": appt.appointment_no or "",
            "Token No": appt.token_no or "",
            "Date": appt.appointment_date.strftime("%d-%m-%Y") if appt.appointment_date else "",
            "Time": appt.appointment_time.strftime("%I:%M %p") if appt.appointment_time else "",
            "Patient Name": appt.patient_name or "",
            "Mobile": appt.mobile or "",
            "Gender": appt.gender or "",
            "Status": appt.status or "",
            "Payment Mode": appt.payment_mode or "",
            "Payment Status": str(payment_status).upper(),
            "Consultation Fee": round(consultation_fee, 2),
            "Doctor Discount": round(doctor_discount, 2),
            "Net Fee": round(net_fee, 2),
            "Created By": appt.created_by or ""
        })

    columns = [
        "Appointment No",
        "Token No",
        "Date",
        "Time",
        "Patient Name",
        "Mobile",
        "Gender",
        "Status",
        "Payment Mode",
        "Payment Status",
        "Consultation Fee",
        "Doctor Discount",
        "Net Fee",
        "Created By"
    ]
    df = pd.DataFrame(rows, columns=columns)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Appointments", index=False)
    output.seek(0)

    safe_label = filter_label.replace("/", "-").replace(" ", "_")
    filename = f"Appointment_Report_{report_type}_{safe_label}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.route("/reports/appointments/print")
@login_required
def print_appointments_report():
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_reports:
        flash("Access denied", "danger")
        return redirect("/")

    payload = build_appointment_report_payload(request.args, flash_errors=False)
    appointments = payload["appointments"]
    total_net = round(sum(appointment_net_amount(a) for a in appointments), 2)
    mode = (request.args.get("mode") or "print").strip().lower()
    if mode not in ("print", "pdf"):
        mode = "print"
    return render_template(
        "appointment_report_print.html",
        appointments=appointments,
        report_label=payload["report_label"],
        total_net=total_net,
        mode=mode,
        generated_at=datetime.now()
    )
@app.route("/users")
@login_required
@admin_required
def users():
    users = active_user_query().order_by(User.username.asc()).all()
    for listed_user in users:
        if listed_user.role == "admin":
            listed_user.access_profile = "admin"
        elif not listed_user.access_profile:
            listed_user.access_profile = derive_access_profile(
                {field_name: getattr(listed_user, field_name, False) for field_name in USER_PERMISSION_FIELDS}
            )
    return render_template(
        "users.html",
        users=users,
        permission_fields=USER_PERMISSION_FIELDS
    )
@app.route("/users/edit/<int:user_id>", methods=["GET", "POST"])
@login_required
@admin_required
def edit_user(user_id):
    user = active_user_query().filter(User.id == user_id).first_or_404()

    if request.method == "POST":
        payload = validate_user_form(request.form, is_create=False)
        if payload["errors"]:
            for error in payload["errors"]:
                flash(error, "danger")
            return render_template(
                "user_form.html",
                user=user,
                form_title="Edit User",
                submit_label="Save Changes",
                profile_options=ACCESS_PROFILE_PRESETS
            )
        duplicate = active_user_query().filter(
            db.func.lower(User.username) == payload["username"].lower(),
            User.id != user.id
        ).first()
        if duplicate:
            flash("Username already exists.", "danger")
            return render_template(
                "user_form.html",
                user=user,
                form_title="Edit User",
                submit_label="Save Changes",
                profile_options=ACCESS_PROFILE_PRESETS
            )
        user.username = payload["username"]
        user.role = payload["role"]
        user.access_profile = payload["access_profile"]
        for field_name, enabled in payload["permissions"].items():
            setattr(user, field_name, bool(enabled))
        user.session_version = int(user.session_version or 0) + 1

        db.session.commit()
        flash("User updated successfully", "success")
        return redirect("/users")

    if not user.access_profile:
        user.access_profile = derive_access_profile({field: getattr(user, field, False) for field in USER_PERMISSION_FIELDS})
    return render_template(
        "user_form.html",
        user=user,
        form_title="Edit User",
        submit_label="Save Changes",
        profile_options=ACCESS_PROFILE_PRESETS
    )
@app.route("/users/add", methods=["GET", "POST"])
@login_required
@admin_required
def add_user():
    if request.method == "POST":
        payload = validate_user_form(request.form, is_create=True)
        if payload["errors"]:
            for error in payload["errors"]:
                flash(error, "danger")
            return render_template(
                "user_form.html",
                user=None,
                form_title="Add User",
                submit_label="Create User",
                profile_options=ACCESS_PROFILE_PRESETS
            )
        if active_user_query().filter(db.func.lower(User.username) == payload["username"].lower()).first():
            flash("Username already exists.", "danger")
            return render_template(
                "user_form.html",
                user=None,
                form_title="Add User",
                submit_label="Create User",
                profile_options=ACCESS_PROFILE_PRESETS
            )
        user = User(
            username=payload["username"],
            role=payload["role"],
            access_profile=payload["access_profile"],
            **payload["permissions"]
        )
        user.set_password(payload["password"])
        
        db.session.add(user)
        db.session.commit()
        flash("User created", "success")
        return redirect("/users")

    return render_template(
        "user_form.html",
        user=None,
        form_title="Add User",
        submit_label="Create User",
        profile_options=ACCESS_PROFILE_PRESETS
    )
@app.route("/users/change-password/<int:user_id>", methods=["GET","POST"])
@login_required
@admin_required
def change_user_password(user_id):
    user = active_user_query().filter(User.id == user_id).first_or_404()

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
        user.session_version = int(user.session_version or 0) + 1
        db.session.commit()
        flash("Password updated")
        return redirect("/users")

    return render_template(
        "change_password.html",
        user=user,
        require_current_password=False,
        page_title=f"Change Password - {user.username}"
    )
@app.route("/users/delete/<int:user_id>")
@login_required
@admin_required
def delete_user(user_id):
    user = active_user_query().filter(User.id == user_id).first_or_404()
    if user.username == "admin":
        flash("Admin user cannot be deleted", "danger")
        return redirect("/users")

    user.is_active = False
    user.deleted_at = datetime.utcnow()
    user.deleted_by = session.get("username")
    user.session_version = int(user.session_version or 0) + 1
    db.session.commit()
    flash("User archived successfully", "success")
    return redirect("/users")

@app.route("/medicines/export")
@login_required
def export_medicines():
    user = active_user_by_id(session.get("user_id"))
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
    from io import BytesIO

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

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="medicine_list.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
@app.route("/export-low-stock")
@login_required
def export_low_stock():
    import pandas as pd
    from flask import send_file
    from io import BytesIO

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

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="low_stock_medicines.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

from datetime import datetime, time

@app.route("/stock-history")
@login_required
def stock_history():
    user = active_user_by_id(session.get("user_id"))
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
    user = active_user_by_id(session.get("user_id"))
    if not user:
        flash("Access denied", "danger")
        return redirect("/")
    if user.role != "admin" and not user.can_view_stock_history:
        flash("Access denied", "danger")
        return redirect("/")
    import pandas as pd
    from flask import send_file
    from io import BytesIO

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
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="stock_history.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
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
