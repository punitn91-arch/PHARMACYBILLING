# AI Clinic Call Receptionist: Repository-Based Plan

## Audit summary

This Flask application already has valuable building blocks that the call
assistant must reuse rather than replace:

- `Patient`, `Appointment`, `LabOrder`, `LabReport`, and `AuditLog` models.
- OTP-protected public appointment reservations with a per-day capacity lock.
- Hashed, expiring, rate-limited OTP challenges and existing MSG91/2Factor
  delivery adapters.
- A protected patient report portal and private report storage.
- A Flask-independent Hindi/Hinglish conversation core in
  `services/voice_receptionist.py` plus a local Mac test launcher.
- Railway/Gunicorn deployment guidance, health checks, and existing tests.

The local launcher is only a microphone test client. It must never be exposed
as an incoming-call process: it has no signed webhook handling, durable call
state, replay protection, or real telephony transport.

## Implementation status — 2026-08-23

The first two foundations are now present in the codebase:

- A provider-neutral call-event boundary with HMAC/replay checking,
  idempotent event storage, monotonic call state, and minimal retention.
  Its generic endpoint is intentionally local/staging only; it is disabled in
  production and does not claim compatibility with a carrier.
- `VoiceCall` and `VoiceCallEvent` tables use a caller fingerprint rather than
  a full number. Raw audio, raw webhook bodies, transcripts, OTPs, report
  data, and plain raw-body hashes are not persisted by this foundation.
- Explicit Alembic bridge migrations now create the call tables and the new
  clinic-location / clinician / schedule tables. Existing legacy automatic
  schema bootstrapping remains a documented limitation to retire later.
- A Flask-independent schedule resolver now supports FCFS arrival windows,
  closures, holidays, leave, and overrides without ever exposing a private
  leave reason or inventing individual time slots.

The schedule models are not yet enabled for public bookings because the same
resolved rule must be enforced in *both* the phone path and the existing QR/
OTP booking path. That integration, the admin configuration UI, and a chosen
provider adapter are the next reviewed milestones.

## Current constraints that shape the design

- The public booking policy is first-come, first-served. It reserves daily
  capacity and gives an arrival window; it does **not** offer individual
  doctor time slots. The call assistant must not promise a time slot until a
  structured scheduling policy is configured.
- `Appointment.doctor_name` is currently a string. There is no structured
  Doctor, clinic schedule, hospital schedule, leave, holiday, or branch data.
- The application has no inbound telephony provider, signed voice webhook,
  durable voice-session model, callback queue, complaint queue, or approved
  non-clinical knowledge base.
- The existing `PortalOtpChallenge` is the source of truth for verification.
  Caller ID and speech transcripts are never proof of identity.

## Target architecture

```text
Caller → existing clinic SIM → configured call forwarding/telephony provider
       → signed HTTPS telephony webhook → call-session service
       → narrowly typed clinic tools → existing Flask services/database
       → provider speech/playback and existing approved SMS OTP delivery
```

The caller continues to dial the clinic's existing SIM number. The provider
destination is an internal routing detail. A human handoff must use a separate
staff number so forwarding cannot create a call loop.

## Implementation phases

1. **Safety and foundation**
   - Add provider-neutral telephony types, signature/replay validation, and
     an allow-listed clinic tool policy.
   - Add persistent call/session and webhook-event storage through formal
     migrations. Store minimal metadata only; no OTP, raw audio, or full
     transcript by default.
   - Add production preflight checks for a strong secret, PostgreSQL,
     persistent private storage, HTTPS, and real OTP delivery.

2. **Live clinic data**
   - Add admin-managed clinic/reception hours, doctors, locations, schedule
     rules, holidays, leaves, and temporary overrides.
   - Keep the present FCFS booking behaviour until the clinic explicitly
     enables a safe slot policy.
   - Add an approved non-clinical knowledge base, callback requests, and
     complaints.

3. **Controlled call actions**
   - Wrap only approved existing operations: clinic status, live availability,
     fee, normal booking reservation/confirmation, verified report status,
     secure report link delivery, callback, complaint, and human handoff.
   - Enforce OTP, confirmation, ownership, idempotency, and audit checks in
     Python/database code rather than trusting an AI model.

4. **Telephony integration**
   - Add the chosen provider adapter behind a small interface for inbound
     webhook parsing, response rendering, transfer, and call status events.
   - Begin with turn-based gather/DTMF webhooks. Real-time audio streaming and
     barge-in belong in a separate asynchronous worker/service, not the Flask
     Gunicorn process.
   - Add an AI/voice provider adapter after explicit caller disclosure and
     consent. Never send booking-stage audio, OTPs, reports, or clinical
     information to a cloud speech service without an approved policy.

5. **Admin, observability, and release**
   - Add an authenticated AI Reception dashboard for calls, schedule changes,
     callbacks, complaints, knowledge-base entries, and safe analytics.
   - Add webhook, retry, authorization, OTP, schedule, duplicate-booking,
     emergency, and provider-failure tests.
   - Deploy first to Railway staging with a separate test call route; then
     configure the clinic SIM's forwarding and production secrets.

## Non-negotiable safety rules

- No direct database access or arbitrary SQL/Python from an AI model.
- Never treat caller ID or speech as patient verification.
- Never speak, log, or store OTP plaintext.
- Do not read report contents or provide diagnosis/medicine advice by voice.
- Verify a provider signature before processing any public telephony webhook;
  deduplicate retries using a persisted event ID.
- Default recordings and transcripts to off. If enabled, require disclosure,
  restricted access, retention limits, and deletion controls.

## External prerequisite for live calls

Before a real call can reach the app, choose a telephony provider that supports
the clinic's forwarding scenario and signed HTTPS webhooks. The provider's
account, inbound route, webhook secret, and a public HTTPS staging URL are
required; no credential belongs in source code or this document.
