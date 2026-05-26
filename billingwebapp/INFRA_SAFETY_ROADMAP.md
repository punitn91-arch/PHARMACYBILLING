# Infra and Safety Roadmap

This roadmap turns the current pharmacy app into a production-safe, recoverable, and operationally reliable system without breaking billing, stock, reporting, or appointment flows.

## Current Baseline

Already live in the codebase:

- Public health endpoints: `/healthz` and `/readyz`
- Background job coordinator for report export queueing, reminder outbox generation, SQLite snapshots, and expiry alerts
- Optional Sentry bootstrap
- Request logging and slow-query logging
- Session invalidation via `session_version`
- PostgreSQL deployment notes in Railway docs
- Some high-value indexes already present on patient, appointment, and vendor-note tables

Key current files:

- [app.py](/Users/punit/Desktop/billingwebapp/billingwebapp/app.py:1)
- [models.py](/Users/punit/Desktop/billingwebapp/billingwebapp/models.py:1)
- [background_jobs.py](/Users/punit/Desktop/billingwebapp/billingwebapp/services/background_jobs.py:1)
- [monitoring.py](/Users/punit/Desktop/billingwebapp/billingwebapp/services/monitoring.py:1)
- [RAILWAY_DEPLOY.md](/Users/punit/Desktop/billingwebapp/billingwebapp/RAILWAY_DEPLOY.md:1)
- [PRO_UPGRADE_STATUS.md](/Users/punit/Desktop/billingwebapp/billingwebapp/PRO_UPGRADE_STATUS.md:1)

## Guiding Rules

- Do not ship infra changes that can silently corrupt billing, stock, or reporting data.
- Prefer additive rollout over big-bang refactors.
- Every production-safety feature must have a verification path.
- Every destructive operation must have auditability or restore coverage.
- SQLite local development may continue, but production architecture should be PostgreSQL-first.

## Phase 1: Production Baseline

Goal:
Move production assumptions to PostgreSQL and persistent uploads before deeper safety work.

Deliverables:

- Standardize production DB on PostgreSQL
- Keep SQLite only for local development and fallback scripts
- Move upload persistence to a mounted volume or object storage strategy
- Centralize storage paths and deployment-sensitive config
- Remove production dependence on ephemeral filesystem assumptions

Primary files to update:

- [app.py](/Users/punit/Desktop/billingwebapp/billingwebapp/app.py:136)
- [RAILWAY_DEPLOY.md](/Users/punit/Desktop/billingwebapp/billingwebapp/RAILWAY_DEPLOY.md:23)
- `billingwebapp/services/storage.py` (new)
- `billingwebapp/services/config.py` (new, optional)
- `billingwebapp/tests/test_system_smoke.py`

Work items:

- Normalize `DATABASE_URL` handling and verify PostgreSQL-ready startup
- Move upload path construction into a shared storage helper
- Define separate paths for vendor uploads, bill attachments, exports, backups, and restore artifacts
- Add storage writability checks to readiness checks
- Add documentation for local SQLite vs production PostgreSQL behavior

Exit criteria:

- Production deployment uses PostgreSQL
- Upload files survive redeploys
- `/readyz` fails if required storage is not writable
- Local development still runs cleanly with SQLite

## Phase 2: Nightly Backups and One-Click Restore

Goal:
Ensure data can be recovered quickly and safely after operator error, deploy issues, or infrastructure problems.

Deliverables:

- Nightly automated DB backups
- Retention policy for old backups
- Upload snapshot strategy
- One-command restore flow
- Restore drill checklist and validation script

Primary files to update:

- [background_jobs.py](/Users/punit/Desktop/billingwebapp/billingwebapp/services/background_jobs.py:1)
- `billingwebapp/services/restore.py` (new)
- `billingwebapp/scripts/restore_backup.py` (new)
- [system_center.html](/Users/punit/Desktop/billingwebapp/billingwebapp/templates/system_center.html:1)
- [app.py](/Users/punit/Desktop/billingwebapp/billingwebapp/app.py:2346)
- `billingwebapp/tests/test_backup_restore.py` (new)

Work items:

- Extend backup jobs beyond SQLite file copy
- For PostgreSQL production, add dump strategy and backup metadata
- Include upload snapshot manifest in backup output
- Store backup status, timestamp, and artifact list in a machine-readable manifest
- Add restore script with modes:
  - latest backup
  - backup by timestamp
  - dry-run validation only
- Add post-restore smoke checks:
  - DB connectivity
  - critical table counts
  - sample invoice lookup
  - upload path integrity

Exit criteria:

- Backup manifest is generated automatically
- Restore command can rebuild a working environment from the latest backup
- Restore drill is documented and repeatable
- Failed backup or restore events are surfaced in logs or admin diagnostics

## Phase 3: Staging Environment and Release Safety

Goal:
Create a safe path to test deploys before production.

Deliverables:

- Dedicated staging environment
- Separate staging PostgreSQL DB
- Separate staging uploads/volume
- Branch-based release workflow
- Release checklist and rollback checklist

Primary files to update:

- [RAILWAY_DEPLOY.md](/Users/punit/Desktop/billingwebapp/billingwebapp/RAILWAY_DEPLOY.md:1)
- [PRO_UPGRADE_STATUS.md](/Users/punit/Desktop/billingwebapp/billingwebapp/PRO_UPGRADE_STATUS.md:1)
- `.env.example` (new or expanded)
- `billingwebapp/scripts/release_check.py` (new)
- `billingwebapp/tests/test_release_smoke.py` (new)

Work items:

- Define `APP_ENV=local|staging|production`
- Keep staging config separate from production secrets
- Add pre-release smoke checklist:
  - login
  - POS billing create
  - return bill
  - appointment add/update/payment
  - report export
  - upload test
- Add rollback instructions:
  - revert release
  - restore backup if required
  - verify health and readiness

Exit criteria:

- Every production deploy is first exercised in staging
- Release checklist exists and is used
- Rollback steps are documented and fast

## Phase 4: Database Performance and Index Strategy

Goal:
Improve report and search speed using measured indexes instead of guesswork.

Deliverables:

- Targeted indexes on heavy filters and joins
- Migration-backed index creation
- Slow-query review process

Primary files to update:

- [models.py](/Users/punit/Desktop/billingwebapp/billingwebapp/models.py:1)
- `billingwebapp/migrations/` (new revisions)
- [monitoring.py](/Users/punit/Desktop/billingwebapp/billingwebapp/services/monitoring.py:1)
- `billingwebapp/tests/test_report_performance_shapes.py` (new)

High-priority index candidates:

- `invoice.created_at`
- `invoice.payment_mode`
- `invoice.created_by`
- `invoice.customer`
- `medicine.barcode`
- `medicine.name + batch`
- `invoice_item.invoice_id`
- `invoice_item.name`
- `return_bill.invoice_id`
- `stock_history.medicine_id + created_at`
- `audit_log.created_at`
- `audit_log.user`
- `audit_log.action`

Work items:

- Review current slow-query logs
- Add indexes only for real query shapes
- Use migrations for every new index
- Avoid duplicate or redundant indexes

Exit criteria:

- Heavy report/search pages show lower latency
- Slow-query logs reduce on known hot paths
- Index changes are tracked in migrations, not runtime hacks

## Phase 5: Soft Delete for Important Records

Goal:
Prevent accidental permanent data loss while preserving clean operational screens.

Deliverables:

- Soft delete model fields on high-risk entities
- Admin restore flow
- Query defaults that hide deleted data unless explicitly requested
- Audit log entries for delete and restore

Primary files to update:

- [models.py](/Users/punit/Desktop/billingwebapp/billingwebapp/models.py:1)
- [app.py](/Users/punit/Desktop/billingwebapp/billingwebapp/app.py:4174)
- `billingwebapp/services/query_filters.py` (new)
- `billingwebapp/templates/` restore/admin pages
- `billingwebapp/tests/test_soft_delete_flows.py` (new)

Priority entities:

- Invoice
- Appointment
- Vendor
- VendorPurchase
- VendorNote
- User

Recommended fields:

- `is_deleted`
- `deleted_at`
- `deleted_by`

Important rule:

- Medicines may continue to use archive/inactive semantics instead of a separate soft-delete layer if that stays consistent with stock logic.

Exit criteria:

- Important records are restorable
- Default UI lists hide deleted rows
- Admin can review and restore deleted items safely

## Phase 6: Session Expiry and Suspicious Login Monitoring

Goal:
Tighten user-account safety and improve audit visibility.

Deliverables:

- Idle session timeout
- Absolute session lifetime
- Failed login logging
- Suspicious login tracking
- Admin review screen for auth events

Primary files to update:

- [app.py](/Users/punit/Desktop/billingwebapp/billingwebapp/app.py:2305)
- [models.py](/Users/punit/Desktop/billingwebapp/billingwebapp/models.py:1)
- `billingwebapp/services/auth_monitoring.py` (new)
- `billingwebapp/templates/audit_logs.html`
- `billingwebapp/templates/system_center.html`
- `billingwebapp/tests/test_auth_security.py` (new)

Events to track:

- login success
- login failure
- logout everywhere
- password change
- access profile change
- unusual repeated failures
- login from new IP or device fingerprint, if captured

Recommended session controls:

- idle timeout, for example 30 minutes
- absolute max session lifetime, for example 8 to 12 hours
- forced re-login after password reset or profile change

Exit criteria:

- Auth events are visible to admins
- Idle sessions expire automatically
- Repeated failed logins are detectable

## Cross-Cutting Engineering Work

These should run alongside the phases above:

- Continue moving remaining route logic out of [app.py](/Users/punit/Desktop/billingwebapp/billingwebapp/app.py:1)
- Replace runtime schema patching with formal migrations
- Add test coverage for new infra paths
- Add admin diagnostics visibility in System Center
- Keep logs structured and machine-readable

## Proposed Order

Recommended order for safe rollout:

1. Phase 1: Production Baseline
2. Phase 2: Backups and Restore
3. Phase 3: Staging and Release Safety
4. Phase 4: Database Performance and Indexes
5. Phase 5: Soft Delete
6. Phase 6: Session and Login Monitoring

## Suggested Timeline

Conservative rollout:

- Week 1: PostgreSQL baseline and persistent uploads
- Week 2: Backup manifests and restore tooling
- Week 3: Staging environment and release checklist
- Week 4: Index review and migrations
- Week 5: Soft delete rollout
- Week 6: Session expiry and suspicious login logging

## Validation Matrix

Every phase should finish with:

- local smoke test
- staging validation
- migration verification
- rollback note
- admin-facing operational note

Critical smoke flows:

- login and logout
- POS billing create/save
- return bill create/cancel
- appointment booking and payment update
- medicine search and barcode search
- vendor purchase with upload attachment
- report view and export
- health and readiness endpoints

## Environment Variables To Standardize

- `APP_ENV`
- `SECRET_KEY`
- `DATABASE_URL`
- `ENABLE_BACKGROUND_JOBS`
- `SENTRY_DSN`
- `SENTRY_TRACES_SAMPLE_RATE`
- `SLOW_REQUEST_MS`
- `SLOW_QUERY_MS`
- `SESSION_IDLE_MINUTES`
- `SESSION_MAX_HOURS`
- `BACKUP_JOB_SECONDS`
- `REPORT_EXPORT_JOB_SECONDS`
- `WHATSAPP_REMINDER_JOB_SECONDS`
- `EXPIRY_ALERT_JOB_SECONDS`
- `UPLOAD_ROOT`
- `BACKUP_ROOT`

## Definition of Done

The infra and safety layer is considered complete only when:

- production runs on PostgreSQL
- uploads persist across redeploys
- backups are automatic and restorable
- staging exists and is used before release
- hot queries are indexed through migrations
- important records are restorable
- sessions expire safely
- suspicious auth activity is auditable

