## Railway Deploy Guide

This app now supports a safer release flow with health checks, backup snapshots, restore drills, persistent uploads, and PostgreSQL-first production settings.

### 1. Recommended Environments

- `local`: SQLite or Postgres for development
- `staging`: Railway service linked to a staging branch and separate PostgreSQL database
- `production`: Railway service linked to `main` with production PostgreSQL and persistent storage

Set `APP_ENV` explicitly:

```text
APP_ENV=staging
APP_ENV=production
```

### 2. Production Environment Variables

Add these in Railway:

```text
SECRET_KEY=<strong-random-secret>
DATABASE_URL=<auto-from-railway-postgres>
APP_ENV=production
APP_TIMEZONE=Asia/Kolkata
ENABLE_BACKGROUND_JOBS=1
APP_STORAGE_ROOT=/data/uploads
APP_PRIVATE_STORAGE_ROOT=/data/private
APP_BACKUP_ROOT=/data/backups
SESSION_IDLE_MINUTES=90
SESSION_ABSOLUTE_HOURS=24
SUSPICIOUS_LOGIN_THRESHOLD=5
SUSPICIOUS_LOGIN_WINDOW_MINUTES=15
SENTRY_DSN=<optional>
SLOW_REQUEST_MS=1200
SLOW_QUERY_MS=250
```

### Public patient portals

The online lab-report portal and QR appointment page are public routes, so
configure these before sharing them with patients:

```text
# Exact HTTPS origin printed inside the reception QR code.
PUBLIC_BOOKING_BASE_URL=https://your-clinic-domain.example

# Real OTP delivery. Development mode only writes a code to server logs.
PUBLIC_PORTAL_OTP_MODE=twilio_whatsapp
TWILIO_ACCOUNT_SID=<real-account-sid>
TWILIO_AUTH_TOKEN=<real-auth-token>
TWILIO_WHATSAPP_FROM=whatsapp:+<approved-sender>
```

`APP_PRIVATE_STORAGE_ROOT` must be on the persistent volume and must **not**
be served by a web/static route. Lab-report PDFs are stored there and are
available only through the OTP-protected download endpoint.

The ₹1,000 priority-booking screen intentionally does not create a paid
appointment from browser data. Before enabling that option for patients,
connect a payment gateway with signed server-side webhook verification.

### 3. PostgreSQL-First Strategy

- Production should always use Railway PostgreSQL.
- Local SQLite is fine for development, but release validation should run against Postgres before production rollout.
- Formal migrations are wired through `Flask-Migrate` / `Alembic`.

Migration workflow:

```bash
cd /Users/punit/Desktop/billingwebapp/billingwebapp
./venv/bin/flask db init      # only once if migrations folder is absent
./venv/bin/flask db migrate -m "describe schema change"
./venv/bin/flask db upgrade
```

### 4. Persistent Upload Storage

Mount a Railway volume and point both uploads and backups to it:

```text
APP_STORAGE_ROOT=/data/uploads
APP_BACKUP_ROOT=/data/backups
```

This keeps vendor attachments, vendor bill uploads, and backup snapshots safe across redeploys.

### 5. Start Command

Railway can use:

```text
web: gunicorn app:app --bind 0.0.0.0:$PORT
```

### 6. Health Checks

After deploy, verify:

- `/healthz` for application health
- `/readyz` for readiness probes
- `/system-center` for admin diagnostics, backup status, and suspicious login review

### 7. Staging Before Production

Recommended release sequence:

1. Deploy feature branch to staging.
2. Run smoke tests.
3. Open `/healthz`, `/readyz`, and `/system-center`.
4. Run a manual backup snapshot from System Center.
5. Run a restore drill from System Center.
6. Promote the same code to production.

### 8. Smoke Tests Before Release

Run locally before pushing:

```bash
cd /Users/punit/Desktop/billingwebapp
billingwebapp/venv/bin/python -m unittest discover -s billingwebapp/tests
```

### 9. Manual Backup and Restore

Create a snapshot from the admin UI:

- `System Center -> Run Snapshot Now`

Restore drill from CLI:

```bash
/Users/punit/Desktop/billingwebapp/billingwebapp/venv/bin/python \
  /Users/punit/Desktop/billingwebapp/billingwebapp/scripts/restore_backup.py \
  --snapshot latest --dry-run
```

Apply a restore:

```bash
/Users/punit/Desktop/billingwebapp/billingwebapp/venv/bin/python \
  /Users/punit/Desktop/billingwebapp/billingwebapp/scripts/restore_backup.py \
  --snapshot latest --apply
```

Use `--allow-production` only in a planned recovery window.

### 10. Nightly Backup Behavior

When `ENABLE_BACKGROUND_JOBS=1`, the app schedules:

- report export queue processing
- WhatsApp reminder outbox generation
- nightly backup snapshots
- expiry alert snapshot generation

Backup retention is controlled by:

```text
BACKUP_KEEP_COUNT=14
BACKUP_JOB_SECONDS=86400
```

### 11. Release Checklist

- Staging deploy successful
- `/healthz` and `/readyz` both pass
- System Center shows storage available
- Latest backup snapshot created
- Restore drill passed
- Smoke tests passed
- PostgreSQL target confirmed
