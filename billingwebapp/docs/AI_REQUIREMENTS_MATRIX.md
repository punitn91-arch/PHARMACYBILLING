# AI integration requirements matrix

Status reflects the Billing App implementation at migration head `20260831_05`.
“Covered” means implemented directly or preserved/reused from the existing app.

| # | Status | Implementation |
|---:|---|---|
| 1 | Covered | Repository conventions, existing models, auth, routes, migrations and tests were inspected; changes are additive. |
| 2 | Covered | External assistant receives HTTPS API access only; no database credential or direct model access. |
| 3 | Covered | Domain routes are split under `routes/ai_*`; business rules live under `services/ai_*`. |
| 4 | Covered | Central `/api/v1/ai` prefix. |
| 5 | Covered | Hashed client secrets and short-lived hashed opaque access tokens. |
| 6 | Covered | All 12 specified least-privilege scopes are server-enforced. |
| 7 | Covered | Active client, token expiry/version, IP allowlist and scope checks run server-side. |
| 8 | Covered | Request/call/session correlation IDs are validated, generated and audited. |
| 9 | Covered | Structured `ClinicProfile` plus structured location data. |
| 10 | Covered | Existing `ClinicLocation` extended for multi-location public information. |
| 11 | Covered | Admin-managed clinic weekly rules; no permanently hard-coded Sunday rule. |
| 12 | Covered | Date exceptions override recurring schedules. |
| 13 | Covered | Existing clinician model extended with approved public fields and fees. |
| 14 | Covered | Doctor/location weekly schedule, slot duration and per-slot capacity. |
| 15 | Covered | Doctor leave, closure and custom-hours date exceptions. |
| 16 | Covered | `/clinic/status` resolves live one/all-location state and next date. |
| 17 | Covered | `/clinic/info` returns approved fields only (`/clinic` compatibility alias retained). |
| 18 | Covered | Active public doctor list; account/private data excluded. |
| 19 | Covered | Effective doctor schedule API applies clinic and doctor rules/overrides. |
| 20 | Covered | Availability API returns effective times, reason/message and next available date. |
| 21 | Covered | Authoritative deterministic slot engine handles rules, overrides, blocks, bookings and cancellations. |
| 22 | Covered | Slot API supports date/time/period filters and returns bookable capacity only. |
| 23 | Covered | Verified existing or OTP-created patient can book; source/call/session/request context stored. |
| 24 | Covered | Deterministic slot lock, row lock and commit-time capacity recheck. |
| 25 | Covered | Consequential operations use hashed idempotency records and request fingerprints. |
| 26 | Covered | Appointment lookup is limited to the verified patient. |
| 27 | Covered | Transactional reschedule records previous date/time, timestamp, source and audit context. |
| 28 | Covered | Cancellation preserves history and records reason/timestamp/source. |
| 29 | Covered | Admin-configured late grace/reschedule/staff-notification policy and late-arrival API. |
| 30 | Covered | Optional patient-owned waitlist with source/call context. |
| 31 | Covered | Enumeration-safe identification intent; no bulk patient endpoint. |
| 32 | Covered | Indian mobile variants normalize consistently; new identities use canonical form. |
| 33 | Covered | Secure random hashed OTP, expiry, attempts, cooldown, one-time use and throttling. |
| 34 | Covered | Temporary hashed server-owned patient session bound to client/patient/phone/call/session. |
| 35 | Covered | Existing lab order/report records reused; no duplicate clinical report store. |
| 36 | Covered | Existing report states map to stable minimal API states. |
| 37 | Covered | Verified, owned, filtered and paginated report status API without interpretation. |
| 38 | Covered | Owned ready reports receive random, hashed, expiring, revocable one-time tokens. |
| 39 | Covered | Token endpoint validates state and streams only containment-checked private files. |
| 40 | Covered | Notification delivery and report delivery states tracked without false delivered claims. |
| 41 | Covered | Admin-managed approved non-clinical knowledge entries. |
| 42 | Covered | Filtered, limited, paginated active knowledge API. |
| 43 | Covered | Separate weekly reception schedules and date overrides. |
| 44 | Covered | Reception API returns live state, next opening and approved transfer destination. |
| 45 | Covered | Idempotent callback intake supports category, priority, summary, assignment and lifecycle. |
| 46 | Covered | Admin callback view supports assignment, statuses, notes and status/priority/date filters. |
| 47 | Covered | Complaint intake supports patient/caller context, category, priority, status and timestamps. |
| 48 | Covered | Admin complaint management includes reference, summary, priority, assignment, notes and filters. |
| 49 | Covered | Provider-neutral OTP/template notification service boundary; provider absence fails closed. |
| 50 | Covered | Verified fixed-purpose appointment/location/report routes plus controlled template endpoint. |
| 51 | Covered | Admin-approved templates and a strict variable allowlist; arbitrary bodies unsupported. |
| 52 | Covered | `/locations/{id}/send` uses verified patient number and fixed location template. |
| 53 | Covered | Request audit includes client/action/resource/correlation/outcome/error/time. |
| 54 | Covered | No payload/OTP/credential logging; phone masked; document path token sanitized. |
| 55 | Covered | One success/error envelope with request ID. |
| 56 | Covered | Stable authentication, verification, clinic, appointment, report and operation error codes. |
| 57 | Covered | Strict IDs, strings, enums, dates, times, ranges, ownership and association validation. |
| 58 | Covered | Endpoint-specific limits protect auth, identify, OTP, slots and reports. |
| 59 | Covered | No patient directory and identical identify response shape for known/unknown numbers. |
| 60 | Covered | Responses omit medical history, prescriptions, addresses and report interpretation. |
| 61 | Covered | HTTPS-ready rotating tokens plus optional IP/CIDR restriction and strict proxy trust setting. |
| 62 | Covered | No wildcard or browser CORS header. |
| 63 | Covered | Admin can create, scope, restrict, disable and rotate clients; secret shown once. |
| 64 | Covered | Admin manages profile, locations, doctors, schedules, overrides, blocks, reception, fees and KB. |
| 65 | Covered | Shared schedule resolver is used across clinic, doctor, next-date and slot APIs. |
| 66 | Covered | Reusable next-available-date logic applies closures/leaves/location rules. |
| 67 | Covered | Configurable IANA timezone with ISO-8601 offset slot output. |
| 68 | Covered | Additive Alembic revisions; no production rebuild/delete flow. |
| 69 | Covered | Targeted indexes cover auth, schedules, appointments, verification, reports and operations. |
| 70 | Covered | AI mutations store `source=AI_CALL`; legacy appointments default to `ADMIN`. |
| 71 | Covered | Generic external call/session/request context stored where consequential. |
| 72 | Covered | Deterministic backend makes verification, availability and ownership decisions. |
| 73 | Covered | No diagnosis, medication, dosage or treatment endpoint exists. |
| 74 | Covered | Endpoint/workflow/error documentation in integration guide. |
| 75 | Covered | OpenAPI 3.0.3 contract contains all 30 implemented paths. |
| 76 | Covered | Dependency-free reference client demonstrates auth, status, slots, OTP, booking and reports. |
| 77 | Covered | Automated auth, schedule, booking, identity, OTP, report, callback, complaint and security tests. |
| 78 | Covered | Full existing suite is required and run before handoff. |
| 79 | Covered | Structured request logs include correlation/client/endpoint/status/latency/error without bodies. |
| 80 | Covered | Existing `/healthz` and `/readyz` preserved. |
| 81 | Covered | `.env.example` documents feature, security, OTP, URL and provider settings without secrets. |
| 82 | Covered | Independent off-by-default flags for API, OTP, reports, callbacks, complaints and notifications. |
| 83 | Covered | Existing Flask initialization, database URL and start/deployment structure preserved. |
| 84 | Covered | Bounded queries, day-specific counts, batched lab-order lookup and streamed files. |
| 85 | Covered | Doctors, appointments, reports and knowledge are limited/paginated. |
| 86 | Covered | Existing admin navigation includes the styled AI Integration section. |
| 87 | Covered | Overview shows feature status, active clients, failures, requests and operational queues. |
| 88 | Covered | Lightweight audit metadata only; full request payloads are not stored. |
| 89 | Covered | Standard safe errors, transaction rollback and provider/storage/auth failure handling. |
| 90 | Covered | Booking, mutations, OTP and secure-link operations use explicit commit/rollback boundaries. |
| 91 | Covered | Notification idempotency is client/key/fingerprint enforced. |
| 92 | Covered | Access control, IDOR, enumeration, leakage, brute force, races and messaging reviewed/tested. |
| 93 | Covered | All requested code, UI, migrations, tests, contract, config and setup deliverables are present. |
| 94 | Covered | `AI_INTEGRATION_GUIDE.md` is the separate assistant handoff guide. |
| 95 | Covered | Billing/clinic authority and Call Assistant telephony/conversation responsibilities stay separate. |
| 96 | Covered | Implementation completed phase-by-phase using existing app conventions. |
