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

# Choose one real SMS OTP provider (not WhatsApp).
# Option A: MSG91 normal SMS
PUBLIC_PORTAL_OTP_MODE=msg91_sms
MSG91_AUTH_KEY=<your-msg91-auth-key>
MSG91_TEMPLATE_ID=<your-approved-msg91-flow-template-id>

# The variable name inside the MSG91 template.  For ##otp##, keep this as otp.
MSG91_OTP_VARIABLE=otp

# Only set this when the MSG91 Flow is configured as "FromAPI".
# MSG91_SENDER_ID=<your-approved-sender-id>

# Option B: 2Factor normal SMS
# Use this instead of the MSG91 variables above when your 2Factor OTP
# template and Sender ID are approved in the 2Factor dashboard.
PUBLIC_PORTAL_OTP_MODE=twofactor_sms
TWOFACTOR_API_KEY=<your-2factor-api-key>
```

To enable SMS OTP, create an Indian transactional SMS Flow in MSG91 and map
its DLT-approved template.  For example, if the template contains
`##otp##`, the app sends the generated six-digit code in that variable.  The
template's brand/sender and wording must be approved in MSG91; do not put an
OTP code, Auth Key, or any real secret in Git or in a chat message.  Add the
required values for your chosen provider in Railway **Variables**, then
redeploy the service. For 2Factor's approved custom-OTP template containing
`XXXX`, the app sends its locally generated six-digit code through 2Factor's
documented SMS OTP API; no API key, OTP, or secret belongs in Git or chat.
Both the appointment and lab-report portals use the same SMS OTP
configuration.

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
