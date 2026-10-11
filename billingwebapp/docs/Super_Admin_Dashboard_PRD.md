# Product Requirements Document (PRD)
## Super Admin Dashboard Redesign — Pharmacy Billing Multi-Tenant SaaS

**Version:** 1.0  
**Date:** 11 October 2026  
**Status:** Ready for implementation  
**Target:** Existing application codebase (edit in place, not a new project)  
**Visual reference:** `saas-tenant-dashboard-reference.webp` (supplied reference screenshot)

---

## 1. Objective

Redesign the **existing Super Admin dashboard** to closely match the supplied reference image's clean, premium SaaS visual style. Keep the current application stack, backend services, database, authentication, subscriptions, and tenant-isolation logic intact. Deliver a **real, functional dashboard**, not a static mockup.

**Implementation priorities, in order:**
1. Preserve existing behavior, tenant security, and data correctness.
2. Match the reference layout, spacing, typography, color scheme, tables, and interactions.
3. Connect every visible control to existing authorized functionality.
4. Keep changes targeted, maintainable, and performant.

## 2. Scope

### In scope
- Super Admin dashboard page and its directly associated styling/scripts.
- Sidebar and header refinements within the Super Admin area.
- Four real-data KPI cards.
- Pharmacy/tenant management table, search, filters, sorting, and pagination.
- Connections to existing Add Pharmacy, Edit Tenant, Manage Users, and Access Toggle operations.
- Minimum backend/API additions **only if existing routes cannot support a required function**.
- Focused validation of dashboard and its security boundaries.

### Out of scope
- Rebuilding the multi-tenant architecture or creating a second admin application.
- Changing pharmacy POS, billing, invoices, GST, purchases, stock, or returns.
- Rewriting login, sessions, role permissions, tenant isolation, or subscription engines.
- Online payments, payment gateways, new subscription billing automation.
- New reporting/audit modules, unnecessary dependencies, mock tenants, fake metrics, and ZIP exports.

## 3. User and Access

**Primary user:** Authenticated platform-level **Super Admin**.

- Dashboard and privileged APIs must require existing Super Admin authorization.
- Ordinary pharmacy/tenant users must be denied access.
- Super Admin aggregates may span tenants **only through explicitly authorized code paths**; tenant-facing endpoints must retain strict isolation.
- Never rely on UI hiding or disabled buttons as an authorization mechanism.

## 4. Visual and Interaction Requirements

**Reference fidelity:** Closely reproduce the screenshot's left white sidebar, slim top bar, light lavender-gray work area, prominent four-card KPI row, rounded search/filter area, clean tenant table, contextual actions, and purple–pink highlighted elements. Do not reproduce the source brand/logo or sample data.

### 4.1 Sidebar
- Left-aligned white sidebar with a branded area at top.
- Reuse existing navigation destinations; preferred labels where available: **Dashboard, Pharmacies/Tenants, Subscriptions, Plans, Users, Reports, Settings**.
- Active navigation item uses a purple-to-pink gradient and white text/icon.
- Minimal outline icons, consistent spacing, visible focus states.
- Collapsible on narrower displays. Do not create empty or fake pages solely to fill the sidebar.

### 4.2 Top bar
- Current page title / breadcrumbs on the left.
- Notifications (only if supported), authenticated Super Admin name/avatar, and existing profile/logout menu on the right.
- No hardcoded name, email, or user image.

### 4.3 KPI cards (four across on desktop)

| Card | Definition / behavior | Visual treatment |
|---|---|---|
| **Total Pharmacies** | Count registered tenant/pharmacy records consistent with existing deletion/archive rules. | White card, blue icon and figure. |
| **Active Subscriptions** | Count subscriptions/tenants considered active by the current subscription business rules; use existing canonical definition. | White card, green icon and figure. |
| **Total Users** | Count tenant user accounts according to existing app user-model rules; avoid inadvertently counting platform-only Super Admin accounts. | Purple-to-pink gradient, white icon and figure. |
| **Expiring Subscriptions** | Active subscriptions whose expiry date is within the next **30 days**, respecting existing date/timezone semantics. | White card, pink icon and figure. |

- Values must come from the real database; no fixed numeric placeholders in production.
- Show a clear loading/empty/error state; avoid presenting errors as misleading zero counts.
- Show trend percentage badges **only when valid comparable historical data already exists**; otherwise omit them.

### 4.4 Tenant / pharmacy table

Columns (adapt names to the current schema, with no invented fields):
1. Pharmacy / Tenant name and avatar/logo or initials
2. Tenant ID
3. Subscription Plan
4. User Count
5. Subscription End Date
6. Subscription Status
7. System Access Toggle
8. Actions (Edit Tenant / Manage Users)

- Alternating very light row backgrounds, rounded corners when practical, compact action icons and clear row hover/focus states.
- Keep a readable table at desktop widths; support horizontal scroll or an accessible compact layout on mobile.
- Suggested subscription badges: **Active** (green), **Expiring Soon** (amber), **Expired** (red), **Suspended** (gray). Use backend's existing canonical status rules and maintain the distinction between **subscription status** and **manual system access**.
- Dates and numeric counts must be formatted consistently with the existing app.

### 4.5 Controls and actions
- **Search** by supported existing fields (ideally pharmacy name, tenant ID, owner name/email), with safe query handling.
- **Advanced Filters** flyout/popover modeled on the image: plan, status, access, expiry window, and sort criteria **only for fields supported by the current schema**.
- **Sort** by name and creation date where supported.
- **Pagination** with clear result count and controls; server-side for growing datasets.
- **Add Pharmacy** button launches/reuses the existing tenant-provisioning workflow.
- **Edit Tenant** reuses the existing authorized edit screen/form.
- **Manage Users** menu reuses existing user-management flows, and never accesses another tenant's data unintentionally.
- **System Access** toggle uses the current enable/suspend mechanism; requires backend validation, a confirmation before disabling, success/failure feedback, and safe state rollback on errors. It must never delete tenant data or silently rewrite subscription history.
- Preserve filter/search/sort state when changing pages wherever practical.

### 4.6 Suggested design tokens

These are starting points; fine-tune using the screenshot and the existing CSS system.

| Token | Value |
|---|---|
| Page background | `#F7F7FC` |
| Surface / sidebar | `#FFFFFF` |
| Primary purple | `#64249A` |
| Accent pink | `#DB3C9D` |
| Main gradient | `linear-gradient(120deg, #54218B, #C02C9A)` |
| Blue KPI | `#4388FA` |
| Green KPI | `#27C785` |
| Pink KPI | `#E64D91` |
| Primary text | `#272438` |
| Secondary text | `#77748A` |
| Corner radius | Typically `10–14px` |
| Motion | Subtle `150–200ms`, honor reduced-motion settings |

Use existing typography and icon packages where possible; avoid unnecessary new assets/dependencies. Ensure contrast and keyboard focus are usable.

## 5. Technical Architecture & Data Requirements

### 5.1 First inspect the existing code
Before editing, identify:
- Super Admin routes and templates/components.
- Existing CSS, JavaScript, design tokens, and frontend stack.
- Tenant, User, Subscription, and Subscription Plan models.
- Admin permissions, CSRF protection, tenant context, status-change services, and pagination/query utilities.
- Current app-specific definitions of active, expired, suspended, and tenant user counts.

**Reuse the actual stack present in the repository.** If it is Flask/SQLAlchemy, extend its existing route/model/service/template patterns rather than adding a parallel frontend or API layer.

### 5.2 Backend and API integration
- Prefer existing dashboard/query endpoints and tenant-management actions.
- Create or adjust only the smallest missing endpoints needed for real functionality.
- Use server-side authorization on **every** read/write endpoint.
- Validate inputs, constrain sorting to allowlisted fields, apply server-side limits, and use ORM parameter binding.
- Maintain CSRF protection for state-changing actions, with the application's existing conventions.
- Return user-safe error messages; never expose sensitive tenant records, tracebacks, or credentials.
- Use the current service layer for provisioning, user edits, subscription updates, and access changes; do not duplicate business logic in view/controller code.

### 5.3 Data and database
- Compute KPI counts from authoritative persisted records using efficient aggregate queries.
- Respect existing soft-deletion, tenant membership, subscription lifecycle, and timezone rules.
- Scope tenant-specific actions to the target tenant and verify the caller's platform role.
- Avoid N+1 queries in table rows; use existing joins/eager loading/count subqueries as appropriate.
- Server-side filter, sort, and paginate before returning tenant rows.
- Avoid schema changes/migrations unless a concrete blocking gap is found; document the reason before making one.
- Do not change invoice numbering, billing records, or tenant-owned business data.

### 5.4 Frontend behavior
- Connect every visible actionable control to a real handler and valid destination.
- Use existing page rendering and AJAX/Fetch conventions; do not introduce React/Vue/etc. solely for this redesign.
- Implement loading, no-results, errors, disabled states, and success confirmation.
- Keep controls keyboard usable; label form controls and action icons accessibly.
- Desktop-first layout, with tablet and mobile adaptations.

### 5.5 Security and performance
- Enforce platform Super Admin role checks on KPIs, tenant listings, and mutations.
- Ensure ordinary tenants cannot call privileged endpoints even with crafted requests.
- Preserve tenant isolation everywhere else.
- Prevent unauthorized access toggle changes, CSRF, SQL injection, and insecure object access.
- Avoid overly broad data fetching, repeated unnecessary requests, and new dependencies.
- Reuse existing audit events for access changes where available.

## 6. Implementation Sequence

1. **Targeted audit:** Inspect only relevant dashboard, tenant, subscription, user, auth, template, and styling code.
2. **UI implementation:** Redesign the existing dashboard in place; match the supplied screenshot.
3. **Data integration:** Hook KPIs, table, search/filter/sort/pagination, and row actions into authorized existing services.
4. **Focused verification:** Run the relevant dashboard tests and a limited set of security/regression checks; fix issues introduced by the work.
5. **Report:** Summarize modified files, reused components, functional changes, checks actually run/results, and any unresolved limitations.

**Do not produce a ZIP. Apply code changes directly inside the existing VS Code project.**

## 7. Acceptance Criteria (Definition of Done)

- [ ] Screenshot-inspired layout is visually close on desktop: sidebar, header, four KPI cards, search/filter strip, tenant table, and contextual actions.
- [ ] UI is responsive and accessible enough to use with keyboard and smaller screens.
- [ ] All four KPIs use correct live database aggregates, not demo values.
- [ ] Tenant rows show accurate existing data without data leakage.
- [ ] Search, filters, sorting, and pagination work together.
- [ ] Add Pharmacy, Edit, and Manage Users lead to working existing workflows.
- [ ] System Access Toggle persists via existing secure business logic, confirms disabling, and handles errors correctly.
- [ ] Subscription status and access toggle remain distinct concepts.
- [ ] Non-Super-Admin access to the page and APIs is denied server-side.
- [ ] No regressions in billing, GST, POS, purchases, inventory, authentication, subscriptions, or tenant isolation caused by the changes.
- [ ] No unnecessary new framework, duplicate services, unrelated refactors, fake data, or ZIP output.
- [ ] Relevant tests are run once as a focused final pass; final summary reports real outcomes without claiming unrun tests passed.

## 8. Agent Execution Instruction

**THINK 10X — AUDIT → REUSE → REDESIGN → INTEGRATE → VERIFY.**

Read this PRD and the supplied reference screenshot, inspect the relevant existing code, and implement the redesign directly in the current repository. Do not stop at planning, do not build a new SaaS system, and do not change unrelated functioning modules. If a required feature has no existing backend capability, make the minimum secure addition and identify it in the final report.
