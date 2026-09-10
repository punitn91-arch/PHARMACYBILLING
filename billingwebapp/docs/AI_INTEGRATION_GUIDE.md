# Clinic AI Integration guide

This Billing App is the source of truth. The separate Call Assistant must use
this HTTPS API only; it must never receive database credentials, storage paths,
API-client secret hashes, or direct model access.

## Base URL and authentication

Production base URL:

`https://<billing-app-host>/api/v1/ai`

An administrator creates an API client under **System → AI Integration** and
copies its one-time secret into the Call Assistant's secret manager. Exchange
that credential at `POST /auth/token`. Tokens are opaque, short-lived, hashed
at rest and restricted by client scopes and optional IP/CIDR allowlists.

```http
POST /api/v1/ai/auth/token
Content-Type: application/json

{"client_id":"ai_...","client_secret":"<secret>","scope":"clinic:read doctor:read"}
```

```json
{"success":true,"data":{"access_token":"<opaque>","token_type":"Bearer","expires_in":900,"scope":"clinic:read doctor:read"},"request_id":"..."}
```

Every authenticated request uses `Authorization: Bearer <token>`. Send generic
`X-Request-ID`, `X-Call-ID` and `X-Session-ID` values for traceability. Never
put phone numbers, OTPs or report tokens in those IDs.

## Endpoint and scope summary

| Capability | Endpoints | Scope |
|---|---|---|
| Clinic info/status | `GET /clinic/info`, `/clinic/status`, `/locations`, `/knowledge`, `/reception/status` | `clinic:read` |
| Doctors | `GET /doctors`, `/doctors/{id}/schedule`, `/availability` | `doctor:read` |
| Slot and lookup | `GET /appointments/slots`, `/appointments` | `appointment:read` |
| Book/waitlist | `POST /appointments`, `/appointments/waitlist` | `appointment:create` |
| Cancel/reschedule/late | `POST /appointments/{no}/cancel`, `/reschedule`, `/late-arrival` | `appointment:update` |
| Identify/OTP | `POST /patients/identify`, `/verification/otp/send`, `/verify` | `patient:identify`, `patient:verify` |
| Reports | `GET /reports/status`, `POST /reports/{id}/secure-link` | `report:read` |
| Report delivery | `POST /reports/{id}/send` | `report:send` |
| Callback/complaint | `POST /callbacks`, `/complaints` | `callback:create`, `complaint:create` |
| Approved messaging | `POST /notifications`, `/notifications/appointment`, `/locations/{id}/send` | `notification:send` |

The exact request contract is [openapi-ai-v1.yaml](openapi-ai-v1.yaml).

For OmniDimension specifically, do not put a raw AI client secret or a
short-lived patient session into its Custom API configuration. Use the narrow
[OmniDimension gateway](OMNIDIM_GATEWAY.md) instead.

## Patient verification workflow

1. Call `POST /patients/identify` with the caller mobile. For a genuinely new
   patient also send their confirmed name. Existing and unknown lookups have
   the same public response shape to prevent enumeration.
2. Call `POST /verification/otp/send` using `identification_ref`.
3. Ask the caller for the OTP and call `/verification/otp/verify`.
4. Keep the returned `patient_session_token` in call-session memory only and
   send it as `X-Patient-Session` on patient-owned operations.
5. The temporary session is bound server-side to the API client, patient,
   verified phone, call/session context and expiry. Caller-supplied patient IDs
   are never accepted as authorization.

Production responses never contain the OTP. Development OTP output exists only
under the test/development adapter.

## Appointment workflow

1. Read clinic and doctor effective schedules.
2. Fetch `/appointments/slots?doctor_id=1&location_id=1&date=2026-09-02`.
   Optional `period`, `start_time` and `end_time` filters are supported. Only
   currently bookable slots are returned.
3. Complete OTP verification.
4. Book with an unpredictable `Idempotency-Key` (8–128 safe characters):

```http
POST /api/v1/ai/appointments
Authorization: Bearer <token>
X-Patient-Session: <temporary-token>
Idempotency-Key: call-abc-booking-001
Content-Type: application/json

{"doctor_id":1,"location_id":1,"start_at":"2026-09-02T10:00:00+05:30","reason":"Follow-up"}
```

The server revalidates clinic schedule, doctor schedule, overrides, blocks,
capacity and ownership inside a transaction. A deterministic slot-lock row and
commit-time capacity check protect the final slot. Retries return the original
result; reuse of a key with different data returns `IDEMPOTENCY_CONFLICT`.

Cancellation preserves history as `CANCELLED`. Reschedule validates the new
slot transactionally. The clinic info response contains configured late-arrival
policy; the assistant must not invent a policy.

## Report and secure-document workflow

`GET /reports/status` requires `X-Patient-Session` and returns title, normalized
status, date and delivery state only—never diagnostic interpretation. It
supports `report_id`, `test_name`, `date_from`, `date_to`, `limit` and `offset`.

`POST /reports/{id}/secure-link` verifies ownership and readiness, then returns
a short-lived one-time URL. The URL contains a random token whose hash alone is
stored. The download endpoint validates expiry, revocation, ownership binding,
download count and private storage containment. Its audit path is sanitized so
the token is never logged.

`POST /reports/{id}/send` creates the link and sends only the admin-approved
`REPORT_LINK` template. It is idempotent and records `SENT`; it does not claim
provider delivery confirmation.

## Responses and errors

All JSON responses use one envelope:

```json
{"success":false,"error":{"code":"APPOINTMENT_UNAVAILABLE","message":"Requested slot is not available"},"request_id":"..."}
```

Common codes include `UNAUTHORIZED`, `FORBIDDEN`, `INVALID_REQUEST`,
`RATE_LIMITED`, `VERIFICATION_REQUIRED`, `OTP_INVALID`, `OTP_EXPIRED`,
`CLINIC_CLOSED`, `DOCTOR_UNAVAILABLE`, `APPOINTMENT_NOT_FOUND`,
`APPOINTMENT_UNAVAILABLE`, `REPORT_NOT_READY`, `REPORT_ACCESS_DENIED`,
`IDEMPOTENCY_CONFLICT`, `NOTIFICATION_FAILED` and `INTERNAL_ERROR`. Do not retry
validation/ownership failures. Retry transient 5xx failures with bounded
backoff and the same idempotency key.

## Deployment checklist

1. Back up the database and run `flask db upgrade`; expected head is
   `20260910_09`.
2. Configure strong `SECRET_KEY`, `AI_AUDIT_FINGERPRINT_SECRET` and HTTPS
   `APPLICATION_BASE_URL`.
3. Keep all AI feature flags off while staging client, locations, schedules,
   overrides, reception hours, knowledge and message templates.
4. Configure a reviewed notification adapter before production messaging.
   The current boundary fails closed when no provider is configured.
5. Create a least-privilege client, set stable egress CIDRs where possible,
   then enable features in staging before production.
6. Monitor AI Integration audits, failures, callbacks and complaints. Rotate a
   client secret to revoke all older tokens immediately.

The Billing App remains responsible for identity, authorization, schedules,
appointments, reports, business rules and audit. The separate Call Assistant
remains responsible for telephony, speech, conversation and deciding which
approved endpoint to call.
