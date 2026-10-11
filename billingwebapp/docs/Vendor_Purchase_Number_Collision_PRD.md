---
title: "Product Requirements Document"
subtitle: "Fix #1 — Tenant-Wise Vendor Purchase Number Collision"
author: "Pharmacy Billing SaaS | Technical Implementation Specification"
date: "10 October 2026"
geometry: margin=0.82in
fontsize: 10pt
mainfont: "DejaVu Sans"
monofont: "DejaVu Sans Mono"
colorlinks: true
linkcolor: "22558A"
header-includes:
  - \usepackage{fancyhdr}
  - \pagestyle{fancy}
  - \fancyhf{}
  - \fancyhead[L]{\small Pharmacy Billing SaaS · PRD 01}
  - \fancyhead[R]{\small Purchase Number Collision}
  - \fancyfoot[C]{\small \thepage}
  - \renewcommand{\headrulewidth}{0.3pt}
  - \setlength{\headheight}{14pt}
  - \usepackage{titlesec}
  - \titlespacing*{\section}{0pt}{1.0em}{0.35em}
  - \titlespacing*{\subsection}{0pt}{0.8em}{0.25em}
---

**Document ID:** PRD-PUR-001  
**Version:** 1.0  
**Status:** Ready for targeted implementation, subject to verification against the active runtime database  
**Priority:** P0 / Blocking tenant purchase workflow  
**Target stack:** Existing Python + Flask + SQLAlchemy + Alembic, SQLite development and PostgreSQL production  
**Source:** Deep audit of `project(6).zip` and the reported UI error; this PRD is not a claim that any fix has already been applied.

## 1. Product outcome

All pharmacies must continue using **one shared Vendor Purchase implementation**. Each tenant can create a purchase with its own internally generated purchase number even if a different tenant has previously used the same number. Within one tenant, duplicate internal purchase numbers must remain prohibited.

**Non-negotiable:** Do not rewrite the purchase engine, stock logic, GST calculations, UI, API payloads or any existing business formula. Make only the tenant-safe schema and numbering corrections actually required.

## 2. Observed issue and current evidence

The application showed: `Purchase number collision detected. Please retry once.`

The ZIP audit found that:

- `billingwebapp/instance/pharmacy.db` has a legacy global uniqueness rule equivalent to `UNIQUE(purchase_no)` on `vendor_purchase`.
- The application creates internal purchase numbers with tenant-aware logic; an earlier tenant already has a value such as `PB-00001`.
- A second tenant can legitimately generate `PB-00001`; the global constraint can reject it even though `tenant_id` differs.
- `billingwebapp/migrations/versions/20261009_15_pharmacy_tenant_business_isolation.py` contains a global-constraint removal helper that can miss **unnamed** SQLite uniqueness constraints. Do not assume a migration has taken effect merely because its file exists.

**Evidence limitation:** The exact failed HTTP POST and its SQL exception were not captured in the offline audit. The implementer must verify the active database and reproduce or trace the failure before making changes. The user may be running a different database than the packaged SQLite snapshot.

## 3. In scope / out of scope

| In scope | Explicitly out of scope |
|---|---|
| Confirm the real internal purchase-number field and physical constraints | Rebuild Vendor Purchase, Billing or Stock services |
| Correct tenant-scoped uniqueness for the internal number | Rename, reset or renumber historical purchases |
| Apply a safe, minimal Alembic migration where needed | Redesign purchase form, receipt, invoice or reports |
| Protect concurrent internal-number allocation if needed | Change medicine prices, GST, discounts, returns or due/payment rules |
| Ensure atomic rollback and specific collision error handling | Fix unrelated `user.username`, `patient.mobile`, subscription or branding issues |
| Prove two-tenant purchase functional parity | Introduce Node.js, Prisma, online payments, ZIP output or extra infrastructure |

Existing supplier/external bill or invoice numbers are **not automatically the same thing** as an application-generated `purchase_no`. Inspect the current schema and business rules before changing constraints on any supplier document identifier.

## 4. Expected product behavior

| Scenario | Required result |
|---|---|
| Tenant A issues internal `PB-00001` | Purchase commits successfully |
| Tenant B independently issues `PB-00001` | Purchase commits successfully |
| Tenant A attempts another purchase with its existing `PB-00001` | Duplicate is rejected; no partial write |
| Tenant A issues its next valid internal number | Purchase commits; existing format preserved |
| Two counters issue purchases concurrently for one tenant | No duplicate committed numbers; safe bounded conflict handling |
| Saving fails for another reason | Actual cause preserved; no misleading “number collision” error |
| Tenant B views its purchase history | Only its own purchases, using the existing UI and route |

Tenant identity must come from authenticated server-side context. A browser-supplied tenant ID must never select ownership or affect uniqueness checks.

## 5. Technical requirements

### FR-01 — Identify the exact numbering contract

- Trace the existing `VendorPurchase` SQLAlchemy model, internal number generator, purchase save route, and Alembic history.
- Confirm the physical table and column (`vendor_purchase.purchase_no` is the audited candidate), supplier bill number field, numbering format, and whether the identifier is generated or entered.
- Inspect SQLite using `PRAGMA table_info`, `PRAGMA index_list`, `PRAGMA index_info` and table DDL; inspect PostgreSQL catalog constraints/indexes where a test instance exists.
- Verify whether any current migration already creates `UNIQUE (tenant_id, purchase_no)` and whether a surviving global constraint makes it ineffective.
- Determine which database file/URL the running app actually uses. Do not assume the ZIP database is the live one.

### FR-02 — Database-enforced tenant uniqueness

Where tenant-scoped numbering is the established intended rule, require the equivalent of:

```sql
UNIQUE (tenant_id, purchase_no)
```

Use the **actual** existing column and appropriate null-handling consistent with the business rules. This requirement does **not** authorize indiscriminately dropping other unique rules or any primary key. Keep foreign keys and RLS/tenant ownership intact.

### FR-03 — Safe legacy schema migration

1. Before DDL, capture active engine/URL **without printing credentials**, migration revision, table DDL, exact indexes/constraints, affected row counts, orphaned tenant ownership, and duplicate values grouped by `(tenant_id, purchase_no)`.
2. Require a verified restorable backup or an isolated copy before any schema-rebuild or destructive change. Stop and explain the blocker otherwise.
3. If migration already exists, repair/extend the migration path without duplicate revision identifiers or breaking databases already upgraded. A corrective migration may be safer than editing an applied historical revision; choose based on actual revision state.
4. **SQLite:** Unnamed `UNIQUE(purchase_no)` commonly belongs to table DDL rather than a droppable named index. Use supported Alembic batch reconstruction or another deliberately tested schema-copy strategy. Preserve rows, indexes, FKs and triggers. Validate SQLite foreign key behavior and transaction boundaries.
5. **PostgreSQL:** Discover the exact constraint/index and drop only the legacy global uniqueness enforcement when safely warranted; create or verify tenant-composite uniqueness. Respect transactional DDL and deployment lock implications.
6. Check for true duplicates **within a tenant** before adding the composite constraint; do not silently delete, merge, or renumber records.
7. Do not assign null-tenant legacy records to an arbitrary tenant. Resolve from reliable ownership evidence or stop and report.
8. After migration, inspect **physical schema**, data row counts and schema revision; do not rely on the existence of Alembic source files.
9. Make deployment reruns safe: the migration should not destroy data or fail unexpectedly on already-correct schemas.

### FR-04 — Concurrency and save transaction correctness

- Retain the existing internal-number generator if it is correct; change it only to address a confirmed defect.
- Ensure database constraints remain the final authority, even if application-level number checks exist.
- If allocation currently uses `MAX(...) + 1`, verify concurrent behavior and apply the smallest safe synchronization technique supported by the active database (e.g. tenant-scoped row lock/atomic sequence under PostgreSQL) only when needed.
- Never implement unbounded retry loops. Retry only a confirmed, relevant uniqueness conflict; use bounded attempts and reallocate a number correctly.
- Handle SQLAlchemy `IntegrityError` with proper session rollback. Do not mistake a supplier bill error, missing foreign key, or another integrity failure for purchase-number collision.
- Keep Vendor Purchase header, items, stock additions, payment/due entries and associated business changes atomic. A failure must not leave partial stock or partially saved purchases.

### FR-05 — Tenant data isolation and existing UI parity

- Ensure purchase creation, retrieval and history are scoped to the authenticated tenant through the existing tenant context and PostgreSQL RLS where configured.
- Do not fork purchase routes or introduce per-pharmacy versions of the engine.
- Preserve the existing purchase-number presentation, Vendor Bill History, form validation and API response contract.
- Keep existing purchase calculations, medicine/batch handling, GST, discounts and print formats unchanged.

## 6. Implementation sequence (minimal and safe)

1. **Confirm:** Identify the active database and collect the exact global/composite uniqueness state, relevant migration revision and failing database exception.
2. **Preflight:** Check data ownership, duplicate groups and valid backup; document any migration blocker.
3. **Correct:** Apply only the necessary `vendor_purchase` numbering constraint/migration and narrowly related conflict handling.
4. **Verify schema:** Prove the global constraint is absent where inappropriate and composite tenant uniqueness is enforced.
5. **Regression tests:** Use an isolated copy of a **legacy** schema, then verify two-tenant and concurrent purchase workflows. Run PostgreSQL-specific tests only with an available isolated PostgreSQL database.
6. **Report:** Record the real root cause, changed files, schema before/after, migrations, tests and any unverified environments.

Avoid a complete codebase audit, unrelated migration cleanup, or repetitive full-suite tests.

## 7. Acceptance tests

| ID | Verification | Acceptance condition |
|---|---|---|
| AT-01 | Existing Tenant A purchase | Saves and appears in Vendor Bill History |
| AT-02 | Tenant B same internal number as Tenant A | Saves without cross-tenant collision |
| AT-03 | Same-tenant duplicate internal number | Database rejects duplicate; clean application response |
| AT-04 | Concurrent same-tenant purchases | Two successful purchases have distinct valid numbers; otherwise safe retry/rollback |
| AT-05 | Failed purchase rollback | No partial purchase/item/stock/payment write |
| AT-06 | Invoice / supplier bill distinction | Unrelated external supplier-number rules unchanged |
| AT-07 | Legacy SQLite migration replay | Legacy unnamed UNIQUE removed correctly, row counts and FKs preserved |
| AT-08 | PostgreSQL migration (if test DB available) | Correct composite uniqueness and tenant RLS behavior |
| AT-09 | Purchase calculation regression | Medicine stock, batch, discounts, GST and totals match baseline |
| AT-10 | Tenant isolation | Tenant A cannot read/change Tenant B purchases |
| AT-11 | Failure diagnosis | Non-numbering errors are not presented as purchase collisions |
| AT-12 | Live app configuration | Migration and verification performed against identified intended environment, not a wrong duplicate DB |

**Test policy:** Never run destructive schema experiments against production data. If a test database or dependency is unavailable, label the result **NOT VERIFIED**, not PASS.

## 8. Definition of done

Implementation is complete only when:

- The underlying collision cause in the **active** environment is confirmed and corrected.
- Both tenants can save valid purchases even if they use the same internal purchase number.
- One tenant cannot commit the same internal purchase number twice.
- Legacy data and formatting are retained; no unrelated business behavior changes.
- No partial purchases/stock adjustments occur on error.
- Physical schema and migration revision are verified after upgrade.
- Focused tests support the result, with explicit limitations for unavailable environments.

## 9. Required coding-agent final report

Provide a short report containing:

1. Actual root cause and evidence (including active DB identity without secrets).
2. Exact files changed and why; identify pre-existing uncommitted changes separately.
3. Physical constraint(s) **before → after** and migration revision.
4. Verification that existing rows and history were preserved.
5. Tenant A / Tenant B save results; same-tenant duplicate and concurrency results.
6. Tests run: **PASS / FAIL / NOT VERIFIED** with commands.
7. Any remaining blocker or manual deployment/backup step.

**Execution instruction:** Read this PRD once, inspect only relevant purchase/numbering/migration files, implement the safe minimal correction directly in the VS Code workspace, run targeted tests, and do not stop at analysis. No ZIP, online payments, UI redesign, broad code refactor or unrelated fixes.
