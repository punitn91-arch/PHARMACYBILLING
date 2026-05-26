# Pro Upgrade Status

This document tracks what is already live in the app, what was upgraded in the latest foundation pass, and what the next high-value phases should be.

## Already Live

- Billing, returns, stock history, vendor purchases, vendor notes, and reports
- Appointment management with payment status and queue board
- Patient directory and patient profile with linked invoice and appointment history
- Global search across patient, medicine, invoice, and appointment records
- Audit log tracking with before/after snapshots
- Role-based user access for reports, inventory, invoice actions, and user management
- Medicine barcode field and searchable medicine master
- Excel exports for pharmacy and appointment reporting

## Added In This Upgrade Pass

- `/healthz` public health endpoint for deployment monitoring
- `/readyz` readiness endpoint for load balancers and platform checks
- Admin-only `/system-center` page for runtime, database, storage, and release diagnostics
- Manual backup snapshots and restore drills exposed in System Center
- Snapshot-based backup service for database rows plus upload archives
- Restore CLI script: `scripts/restore_backup.py`
- Secure session defaults and configurable upload-size guardrail
- Session idle timeout and absolute session expiry
- Suspicious login tracking for failed attempts, disabled-user access, and new IP/device logins
- Automated smoke tests for health endpoints, system center, and global search
- Route/service helper split started with `routes/` and `services/` modules for billing, appointments, reports, and vendors
- Optional `Flask-Migrate` / `Alembic` wiring added with migration bootstrap notes
- Central validation layer added for user management, billing, returns, reports, and appointment payment transitions
- Request logging, slow-query logging, and optional Sentry bootstrap added
- Background job coordinator added for report export queueing, reminder outbox generation, backup snapshots, and expiry alerts
- Granular access presets added: report-only, billing-only, stock manager, purchase manager, and admin audit access
- Soft-delete protection added for users, appointments, held bills, and vendors
- Runtime indexes added for heavy invoice, report, audit, stock, and security-login lookups
- Environment-driven persistent storage support for uploads and backup snapshots
- Flow-level regression tests added for billing, returns, appointment payments, report math, and FIFO stock deduction

## Next Pro Phases

### Phase 2: Operations and Release Safety

- Production backup alerting and retention monitoring
- Restore apply runbook with owner sign-off checklist
- Background-job dashboard alerts and notification hooks
- Detailed rollout plan: `INFRA_SAFETY_ROADMAP.md`

### Phase 3: Patient Finance and CRM

- Patient credit ledger and due collection flow
- Visit-follow-up workflow and refill reminders
- Better patient financial summary inside profile pages

### Phase 4: Automation and Analytics

- WhatsApp reminder automation
- Dashboard trend charts and management analytics
- Reorder forecasting and dead-stock intelligence

### Phase 5: Engineering Maturity

- Continue moving remaining route bodies out of monolithic `app.py`
- Convert runtime schema patching fully over to formal migrations
- Add permission regression tests for non-admin operational roles
