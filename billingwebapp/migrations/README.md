## Database migrations

This directory is the Alembic / Flask-Migrate migration history for the
production AI call-receptionist work. It is already initialized; **do not run
`flask db init` again**.

The reviewed chain is intentionally additive:

- `20260823_01_voice_call_foundation` adds the minimal, privacy-preserving
  call and provider-event tables.
- `20260823_02_clinic_schedule_configuration` adds reusable clinic locations,
  clinician records, weekly schedules, and date exceptions.
- `20260831_03_ai_api_foundation` adds hashed external client credentials,
  short-lived opaque access tokens, privacy-minimized request auditing, and a
  reusable idempotency store for later appointment/notification operations.
- `20260831_04_ai_domain_operations` extends the reusable clinic, doctor,
  appointment and report tables and adds verification sessions, secure
  document tokens, slot locks/blocks, waitlists, reception/knowledge,
  callbacks, complaints and notification outbox tables.
- `20260831_05_ai_legacy_schema_reconciliation` reconciles this application's
  historical automatic schema bootstrap with the tables above.
- `20260901_06_clinic_profile_timezone_compat` keeps the legacy
  `clinic_profile.timezone` column and the finalized `timezone_name` column
  synchronized for installations created before the timezone field was
  finalized.
- `20260905_07_ai_clinic_lab_upgrade` adds admin-managed public doctor detail
  (sub-specialties, languages, conditions treated, services offered), lab
  test search aliases/fasting/turnaround fields, and a schedule-rule
  `individual_time_slots` flag so one shared availability engine can serve
  either an ARRIVAL_WINDOW capacity pool or FIXED_SLOT individually bookable
  times. It also backfills existing clinic-wide (no-clinician) schedule rows
  to ARRIVAL_WINDOW, matching that table's own documented contract; every
  per-doctor row keeps today's FIXED_SLOT behaviour.
- `20261009_13_tenant_foundation` through `20261009_16_super_admin_subscriptions`
  add tenant ownership, tenant-scoped identifiers, and manual subscription
  history.
- `20261009_17_platform_super_admin_rls` and
  `20261009_18_rls_tenant_settings_subscriptions` restrict global platform
  administrator records, tenant settings, and subscription history under
  PostgreSQL row-level security.
- `20261009_19_tenant_ownership_foreign_keys` adds any missing
  `tenant_id`-to-`tenant.id` foreign keys across tenant-owned tables.
- `20261010_20_invoice_business_identity_snapshot` preserves tenant business
  identity on issued invoice records.
- `20261010_21_vendor_purchase_tenant_number_unique` scopes internal purchase
  number uniqueness to the owning tenant.
- `20261010_22_tenant_scoped_identity_uniqueness` removes selected global
  identity uniqueness and installs tenant-scoped indexes.
- `20261010_23_ai_tenant_identity_lookup` adds narrow PostgreSQL RLS policies
  for resolving AI clients and access tokens before tenant context exists.
- `20261010_24_tenant_scoped_vendor_note_numbers` scopes vendor-note number
  uniqueness to the owning tenant while preserving note and allocation rows.

They are guarded bridge revisions because this legacy application still uses
`db.create_all()` and runtime `ensure_column(...)` guards during start-up.

For a staging or production deployment, run the following from the folder that
contains `app.py` after taking a database backup:

```bash
source venv/bin/activate
export FLASK_APP=app.py
flask db upgrade
flask db current
```

`flask db current` must report `20261010_24 (head)` after the upgrade.

The bridge revision safely marks an existing database as upgraded if the two
voice-call tables were already created by the legacy start-up path. It never
drops or rewrites old clinic data.

If an old pre-release `voice_call_event` table contains rows with a raw-body
hash, the upgrade intentionally stops instead of disguising that historical
metadata as the new keyed event fingerprint. Decide its retention or deletion
policy with an authorized administrator before retrying the migration.

Until the old automatic schema bootstrap is retired, make new schema changes
with an explicit, reviewed migration rather than relying blindly on
`flask db migrate --autogenerate`: importing `app.py` can otherwise create a
table before Alembic compares metadata. A full historical migration baseline
and removal of the legacy runtime schema mutations remain a separate hardening
task before broad production rollout.
