from __future__ import annotations

import json
import os
import shutil
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from models import Appointment, Medicine
from services.infra_safety import build_backup_snapshot


class BackgroundJobCoordinator:
    def __init__(self, app):
        self.app = app
        self.jobs = {}
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def register(self, key, interval_seconds, callback):
        with self._lock:
            self.jobs[key] = {
                "interval_seconds": max(60, int(interval_seconds)),
                "callback": callback,
                "last_run_ts": None,
                "last_run_at": None,
                "last_status": "idle",
                "last_error": "",
            }

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, name="billingwebapp-jobs", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def snapshot(self):
        with self._lock:
            return {
                key: {
                    "interval_seconds": value["interval_seconds"],
                    "last_run_at": value["last_run_at"],
                    "last_status": value["last_status"],
                    "last_error": value["last_error"],
                }
                for key, value in self.jobs.items()
            }

    def _loop(self):
        while not self._stop_event.wait(15):
            now_ts = time.time()
            with self._lock:
                job_items = list(self.jobs.items())
            for key, job in job_items:
                last_run = job["last_run_ts"]
                should_run = last_run is None or (now_ts - last_run) >= job["interval_seconds"]
                if not should_run:
                    continue
                self._run_job(key, job)

    def _run_job(self, key, job):
        try:
            with self.app.app_context():
                job["callback"](self.app)
            job["last_status"] = "ok"
            job["last_error"] = ""
        except Exception as exc:  # pragma: no cover - defensive logging
            self.app.logger.exception("Background job %s failed", key)
            job["last_status"] = "error"
            job["last_error"] = str(exc)
        finally:
            job["last_run_ts"] = time.time()
            job["last_run_at"] = datetime.utcnow().isoformat()


def _ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def queue_report_export_job(app, payload):
    report_jobs_dir = _ensure_dir(os.path.join(app.instance_path, "report_jobs", "pending"))
    job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    target_path = os.path.join(report_jobs_dir, f"{job_id}.json")
    with open(target_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
    return {"job_id": job_id, "path": target_path}


def _process_report_export_queue(app):
    pending_dir = _ensure_dir(os.path.join(app.instance_path, "report_jobs", "pending"))
    ready_dir = _ensure_dir(os.path.join(app.instance_path, "report_jobs", "ready"))
    for filename in os.listdir(pending_dir):
        if not filename.endswith(".json"):
            continue
        source_path = os.path.join(pending_dir, filename)
        target_path = os.path.join(ready_dir, filename)
        if os.path.exists(target_path):
            os.remove(source_path)
            continue
        shutil.move(source_path, target_path)


def _write_whatsapp_reminder_outbox(app):
    tomorrow = datetime.utcnow().date() + timedelta(days=1)
    appointments = Appointment.query.filter(
        Appointment.appointment_date == tomorrow,
        Appointment.status != "CANCELLED",
        Appointment.deleted_at.is_(None),
    ).order_by(Appointment.appointment_time.asc(), Appointment.id.asc()).all()
    reminder_rows = []
    for appointment in appointments:
        if not (appointment.mobile or "").strip():
            continue
        reminder_rows.append({
            "appointment_no": appointment.appointment_no,
            "patient_name": appointment.patient_name,
            "mobile": appointment.mobile,
            "appointment_date": appointment.appointment_date.isoformat() if appointment.appointment_date else "",
            "appointment_time": appointment.appointment_time.strftime("%H:%M") if appointment.appointment_time else "",
            "doctor_name": appointment.doctor_name,
        })
    outbox_dir = _ensure_dir(os.path.join(app.instance_path, "outbox"))
    target_path = os.path.join(outbox_dir, "whatsapp_reminders.json")
    with open(target_path, "w", encoding="utf-8") as handle:
        json.dump({"generated_at": datetime.utcnow().isoformat(), "appointments": reminder_rows}, handle, ensure_ascii=True, indent=2)


def _write_expiry_alert_snapshot(app):
    today = datetime.utcnow().date()
    alert_rows = []
    for medicine in Medicine.query.order_by(Medicine.name.asc(), Medicine.batch.asc()).all():
        expiry_raw = (medicine.expiry or "").strip()
        try:
            expiry_date = datetime.strptime(expiry_raw, "%Y-%m-%d").date()
        except ValueError:
            try:
                expiry_date = datetime.strptime(expiry_raw, "%d/%m/%Y").date()
            except ValueError:
                continue
        days_left = (expiry_date - today).days
        if days_left > 30:
            continue
        alert_rows.append({
            "medicine": medicine.name,
            "batch": medicine.batch,
            "expiry": expiry_raw,
            "stock": medicine.qty or 0,
            "days_left": days_left,
        })
    alerts_dir = _ensure_dir(os.path.join(app.instance_path, "alerts"))
    target_path = os.path.join(alerts_dir, "expiry_alerts.json")
    with open(target_path, "w", encoding="utf-8") as handle:
        json.dump({"generated_at": datetime.utcnow().isoformat(), "items": alert_rows}, handle, ensure_ascii=True, indent=2)


def _backup_database_file(app):
    build_backup_snapshot(
        app,
        upload_dirs=app.config.get("INFRA_UPLOAD_DIRECTORIES", {}),
        keep_count=int(os.environ.get("BACKUP_KEEP_COUNT", "14") or 14),
        include_uploads=True,
    )


def init_background_jobs(app, *, enabled):
    coordinator = BackgroundJobCoordinator(app)
    app.extensions["background_jobs"] = coordinator
    if not enabled or app.config.get("TESTING"):
        return coordinator

    coordinator.register("report_exports", int(os.environ.get("REPORT_EXPORT_JOB_SECONDS", "120") or 120), _process_report_export_queue)
    coordinator.register("whatsapp_reminders", int(os.environ.get("WHATSAPP_REMINDER_JOB_SECONDS", "1800") or 1800), _write_whatsapp_reminder_outbox)
    coordinator.register("database_backup", int(os.environ.get("BACKUP_JOB_SECONDS", "86400") or 86400), _backup_database_file)
    coordinator.register("expiry_alerts", int(os.environ.get("EXPIRY_ALERT_JOB_SECONDS", "3600") or 3600), _write_expiry_alert_snapshot)
    coordinator.start()
    return coordinator
