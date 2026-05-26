from __future__ import annotations

import json
import os
import shutil
import zipfile
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.sql.sqltypes import Boolean, Date, DateTime, Integer, Numeric, Time

try:
    from ..models import (
        Appointment,
        AuditLog,
        HoldBill,
        Invoice,
        InvoiceItem,
        LoginSecurityEvent,
        Medicine,
        Patient,
        Return,
        ReturnItem,
        SalesAllocation,
        StockHistory,
        User,
        Vendor,
        VendorLedgerEntry,
        VendorNote,
        VendorNoteAllocation,
        VendorNoteItem,
        VendorPurchase,
        VendorPurchaseItem,
        db,
    )
except ImportError:  # pragma: no cover - script/local fallback
    from models import (
        Appointment,
        AuditLog,
        HoldBill,
        Invoice,
        InvoiceItem,
        LoginSecurityEvent,
        Medicine,
        Patient,
        Return,
        ReturnItem,
        SalesAllocation,
        StockHistory,
        User,
        Vendor,
        VendorLedgerEntry,
        VendorNote,
        VendorNoteAllocation,
        VendorNoteItem,
        VendorPurchase,
        VendorPurchaseItem,
        db,
    )


BACKUP_MODEL_ORDER = [
    User,
    Patient,
    Medicine,
    Vendor,
    HoldBill,
    Invoice,
    Return,
    Appointment,
    VendorPurchase,
    InvoiceItem,
    ReturnItem,
    VendorPurchaseItem,
    VendorNote,
    VendorNoteItem,
    VendorNoteAllocation,
    VendorLedgerEntry,
    SalesAllocation,
    StockHistory,
    AuditLog,
    LoginSecurityEvent,
]

HEAVY_QUERY_INDEXES = [
    ("ix_invoice_created_at", "invoice", ["created_at"]),
    ("ix_invoice_payment_mode", "invoice", ["payment_mode"]),
    ("ix_invoice_customer_mobile", "invoice", ["customer", "mobile"]),
    ("ix_invoice_item_invoice_id", "invoice_item", ["invoice_id"]),
    ("ix_invoice_item_name_batch", "invoice_item", ["name", "batch"]),
    ("ix_return_bill_invoice_created", "return_bill", ["invoice_id", "created_at"]),
    ("ix_return_item_return_id", "return_item", ["return_id"]),
    ("ix_return_item_invoice_item_id", "return_item", ["invoice_item_id"]),
    ("ix_medicine_barcode", "medicine", ["barcode"]),
    ("ix_medicine_name_batch", "medicine", ["name", "batch"]),
    ("ix_stock_history_created_at", "stock_history", ["created_at"]),
    ("ix_stock_history_ref_lookup", "stock_history", ["ref_table", "ref_id"]),
    ("ix_audit_log_user_created", "audit_log", ["user", "created_at"]),
    ("ix_audit_log_action_created", "audit_log", ["action", "created_at"]),
    ("ix_vendor_purchase_vendor_date", "vendor_purchase", ["vendor_id", "purchase_date"]),
    ("ix_vendor_purchase_invoice_no", "vendor_purchase", ["invoice_no"]),
    ("ix_vendor_purchase_item_purchase_id", "vendor_purchase_item", ["purchase_id"]),
    ("ix_vendor_purchase_item_medicine_batch", "vendor_purchase_item", ["medicine_name", "batch"]),
    ("ix_vendor_notes_status_date", "vendor_notes", ["status", "note_date"]),
    ("ix_login_security_created_at", "login_security_event", ["created_at"]),
    ("ix_login_security_username_created", "login_security_event", ["username", "created_at"]),
    ("ix_login_security_suspicious_created", "login_security_event", ["is_suspicious", "created_at"]),
]


def ensure_runtime_indexes():
    for index_name, table_name, columns in HEAVY_QUERY_INDEXES:
        cols_sql = ", ".join(f'"{column}"' for column in columns)
        try:
            db.session.execute(
                text(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({cols_sql})')
            )
            db.session.commit()
        except Exception:
            db.session.rollback()


def get_backup_root(app):
    configured_root = (
        app.config.get("BACKUP_ROOT")
        or os.environ.get("APP_BACKUP_ROOT")
        or os.path.join(app.instance_path, "backups")
    )
    root = os.path.abspath(str(configured_root))
    Path(root).mkdir(parents=True, exist_ok=True)
    return root


def _serialize_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _deserialize_value(value, column):
    if value is None:
        return None
    col_type = getattr(column, "type", None)
    if isinstance(col_type, DateTime):
        return datetime.fromisoformat(value)
    if isinstance(col_type, Date):
        return date.fromisoformat(value)
    if isinstance(col_type, Time):
        return time.fromisoformat(value)
    if isinstance(col_type, Numeric):
        return Decimal(str(value))
    if isinstance(col_type, Integer):
        return int(value)
    if isinstance(col_type, Boolean):
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}
    return value


def _dump_model_rows(model):
    columns = list(model.__table__.columns)
    rows = model.query.order_by(model.id.asc()).all() if hasattr(model, "id") else model.query.all()
    serialized = []
    for row in rows:
        row_payload = {}
        for column in columns:
            row_payload[column.name] = _serialize_value(getattr(row, column.name))
        serialized.append(row_payload)
    return serialized


def _write_zip_from_dir(source_dir, target_path):
    with zipfile.ZipFile(target_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if not os.path.isdir(source_dir):
            return
        for root, _dirs, files in os.walk(source_dir):
            for filename in files:
                file_path = os.path.join(root, filename)
                arcname = os.path.relpath(file_path, source_dir)
                zf.write(file_path, arcname)


def _reset_directory(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _prune_old_snapshots(backups_root, keep_count):
    snapshot_dirs = []
    for name in os.listdir(backups_root):
        path = os.path.join(backups_root, name)
        if os.path.isdir(path):
            snapshot_dirs.append((os.path.getmtime(path), path))
    snapshot_dirs.sort(reverse=True)
    for _mtime, path in snapshot_dirs[keep_count:]:
        shutil.rmtree(path, ignore_errors=True)


def build_restore_commands(app, snapshot_name="latest"):
    project_dir = app.config.get("APP_BASE_DIR") or os.getcwd()
    python_path = os.path.join(project_dir, "venv", "bin", "python")
    return {
        "dry_run": f"{python_path} {project_dir}/scripts/restore_backup.py --snapshot {snapshot_name} --dry-run",
        "apply": f"{python_path} {project_dir}/scripts/restore_backup.py --snapshot {snapshot_name} --apply",
    }


def build_backup_snapshot(app, *, upload_dirs, keep_count=14, include_uploads=True):
    backups_root = get_backup_root(app)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snapshot_name = f"snapshot_{stamp}"
    snapshot_dir = os.path.join(backups_root, snapshot_name)
    os.makedirs(snapshot_dir, exist_ok=True)

    table_payload = {}
    table_counts = {}
    for model in BACKUP_MODEL_ORDER:
        rows = _dump_model_rows(model)
        table_payload[model.__tablename__] = rows
        table_counts[model.__tablename__] = len(rows)

    database_dump_path = os.path.join(snapshot_dir, "database.json")
    with open(database_dump_path, "w", encoding="utf-8") as handle:
        json.dump({"tables": table_payload}, handle, ensure_ascii=True, indent=2)

    upload_archives = {}
    if include_uploads:
        for key, path_value in upload_dirs.items():
            archive_name = f"{key}.zip"
            archive_path = os.path.join(snapshot_dir, archive_name)
            _write_zip_from_dir(path_value, archive_path)
            upload_archives[key] = archive_name

    commands = build_restore_commands(app, snapshot_name)
    manifest = {
        "snapshot_name": snapshot_name,
        "created_at": datetime.utcnow().isoformat(),
        "environment": app.config.get("APP_ENV", "local"),
        "database_backend": db.session.bind.dialect.name if db.session.bind else "unknown",
        "table_counts": table_counts,
        "upload_archives": upload_archives,
        "restore_commands": commands,
    }
    manifest_path = os.path.join(snapshot_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=True, indent=2)

    restore_plan_path = os.path.join(snapshot_dir, "restore_plan.txt")
    with open(restore_plan_path, "w", encoding="utf-8") as handle:
        handle.write("Backup Restore Plan\n")
        handle.write("===================\n\n")
        handle.write(f"Snapshot: {snapshot_name}\n")
        handle.write(f"Created At: {manifest['created_at']}\n\n")
        handle.write("Dry Run:\n")
        handle.write(f"  {commands['dry_run']}\n\n")
        handle.write("Apply Restore:\n")
        handle.write(f"  {commands['apply']}\n")

    _prune_old_snapshots(backups_root, max(3, int(keep_count or 14)))
    return {
        "snapshot_name": snapshot_name,
        "snapshot_dir": snapshot_dir,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "restore_plan_path": restore_plan_path,
    }


def list_backup_snapshots(app, *, limit=10):
    backups_root = get_backup_root(app)
    snapshots = []
    for name in os.listdir(backups_root):
        snapshot_dir = os.path.join(backups_root, name)
        manifest_path = os.path.join(snapshot_dir, "manifest.json")
        if not os.path.isdir(snapshot_dir) or not os.path.exists(manifest_path):
            continue
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
        except Exception:
            continue
        snapshots.append(
            {
                "snapshot_name": manifest.get("snapshot_name", name),
                "created_at": manifest.get("created_at", ""),
                "table_counts": manifest.get("table_counts", {}),
                "upload_archives": manifest.get("upload_archives", {}),
                "path": snapshot_dir,
            }
        )
    snapshots.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return snapshots[:limit]


def get_backup_summary(app):
    backup_root = get_backup_root(app)
    snapshots = list_backup_snapshots(app, limit=1)
    latest = snapshots[0] if snapshots else None
    return {
        "snapshot_count": len(list_backup_snapshots(app, limit=500)),
        "latest_snapshot": latest["snapshot_name"] if latest else "",
        "latest_created_at": latest["created_at"] if latest else "",
        "root_path": backup_root,
    }


def restore_backup_snapshot(app, snapshot_name="latest", *, include_uploads=True, dry_run=False, allow_production=False):
    if app.config.get("APP_ENV") == "production" and not allow_production and not dry_run:
        raise RuntimeError("Restore apply is blocked in production without explicit allow_production=True.")

    snapshots = list_backup_snapshots(app, limit=500)
    if not snapshots:
        raise FileNotFoundError("No backup snapshots were found.")
    target = snapshots[0] if snapshot_name == "latest" else next(
        (item for item in snapshots if item["snapshot_name"] == snapshot_name),
        None,
    )
    if not target:
        raise FileNotFoundError(f"Backup snapshot '{snapshot_name}' was not found.")

    snapshot_dir = target["path"]
    manifest_path = os.path.join(snapshot_dir, "manifest.json")
    database_dump_path = os.path.join(snapshot_dir, "database.json")
    if not os.path.exists(manifest_path) or not os.path.exists(database_dump_path):
        raise FileNotFoundError("Snapshot is incomplete. Missing manifest.json or database.json.")

    with open(manifest_path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    with open(database_dump_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    table_payload = payload.get("tables") or {}
    if dry_run:
        return {
            "snapshot_name": target["snapshot_name"],
            "table_counts": manifest.get("table_counts", {}),
            "upload_archives": manifest.get("upload_archives", {}),
            "dry_run": True,
        }

    for model in reversed(BACKUP_MODEL_ORDER):
        db.session.execute(model.__table__.delete())
    db.session.commit()

    for model in BACKUP_MODEL_ORDER:
        rows = table_payload.get(model.__tablename__, [])
        if not rows:
            continue
        prepared_rows = []
        for row in rows:
            prepared = {}
            for column in model.__table__.columns:
                if column.name in row:
                    prepared[column.name] = _deserialize_value(row[column.name], column)
            prepared_rows.append(prepared)
        db.session.execute(model.__table__.insert(), prepared_rows)
    db.session.commit()

    if db.session.bind and db.session.bind.dialect.name == "postgresql":
        for model in BACKUP_MODEL_ORDER:
            if not hasattr(model, "id"):
                continue
            table_name = model.__tablename__
            db.session.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('\"{table_name}\"', 'id'), "
                    f"COALESCE((SELECT MAX(id) FROM \"{table_name}\"), 1), true)"
                )
            )
        db.session.commit()

    if include_uploads:
        for key, archive_name in (manifest.get("upload_archives") or {}).items():
            archive_path = os.path.join(snapshot_dir, archive_name)
            target_dir = app.config.get("INFRA_UPLOAD_DIRECTORIES", {}).get(key)
            if not archive_path or not target_dir or not os.path.exists(archive_path):
                continue
            _reset_directory(target_dir)
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(target_dir)

    return {
        "snapshot_name": target["snapshot_name"],
        "table_counts": manifest.get("table_counts", {}),
        "upload_archives": manifest.get("upload_archives", {}),
        "dry_run": False,
    }
