# OmniDimension voice gateway

This adapter is the only Billing App surface intended for OmniDimension's
**Custom API** actions. It is deliberately narrower than `/api/v1/ai`:

- It accepts one long random gateway key in `X-Clinic-Gateway-Key`.
- It exposes public clinic information, timings and live appointment slots.
- It creates staff-reviewed appointment requests, callbacks and complaints.
- It never accepts or returns an internal AI client secret, an OTP, a patient
  session token, report status, report contents or a report download URL.

## Why report lookup and direct booking are not voice-tool actions

The Billing App requires OTP proof of possession before viewing a patient's
report or confirming a patient-owned appointment. OmniDimension's Custom API
uses static headers, so it cannot safely retain a short-lived verified patient
session without exposing it to the model/provider. The gateway instead returns
the clinic's existing OTP-protected web links and can create a staff callback
request when the caller needs help.

## Railway variables

Set these in **Railway → web service → Variables**. Do not put the secret in
source control, a chat, an agent prompt or a public knowledge-base document.

```text
APPLICATION_BASE_URL=https://appbilling.up.railway.app
OMNIDIM_GATEWAY_ENABLED=true
OMNIDIM_GATEWAY_SECRET=<new random value, at least 32 bytes>
OMNIDIM_GATEWAY_RATE_LIMIT_PER_MINUTE=60
AI_CALLBACKS_ENABLED=true
AI_COMPLAINTS_ENABLED=true
```

`AI_API_ENABLED`, `AI_OTP_ENABLED`, `AI_SECURE_REPORT_DELIVERY_ENABLED` and
`AI_NOTIFICATIONS_ENABLED` can remain `false` for this gateway. They control
the separate internal API and must not be enabled only to make OmniDimension
work.

Generate the gateway secret locally, then paste it only into Railway and the
OmniDimension header field:

```bash
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
```

After changing a variable, let Railway deploy/restart. Rotate the secret in
both places immediately if it is ever exposed.

## OmniDimension Custom API settings

For every action, add this header:

```text
X-Clinic-Gateway-Key: <the same OMNIDIM_GATEWAY_SECRET>
```

Use the exact endpoint and method below. The base URL is:

```text
https://appbilling.up.railway.app/api/v1/omnidim
```

| Omni action | Method and URL | What it does |
|---|---|---|
| Clinic information | `GET /clinic-info` | Public clinic contacts, services and locations. |
| Doctor list | `GET /doctors` | Public active doctor information. |
| Doctor timing | `GET /timings?doctor_name={doctor_name}&location_name={location_name}&date={date}` | Effective timing/closure for `YYYY-MM-DD`. `location_name` is optional if that doctor has a default clinic. |
| Available slots | `GET /appointment-slots?doctor_name={doctor_name}&location_name={location_name}&date={date}` | Live available appointment slots only. |
| Booking portal | `GET /appointment-booking-link` | The existing secure OTP booking link. |
| Lab-report portal | `GET /lab-report-access` | The existing secure OTP report portal link. |
| Appointment request | `POST /appointment-requests` | Creates a staff callback request; it does **not** claim a booked appointment. |
| Callback | `POST /callbacks` | Creates a staff callback request. |
| Complaint | `POST /complaints` | Opens a complaint for staff follow-up. |

For `POST` actions, configure a JSON request body and ensure the agent calls
the action only after the caller explicitly says yes. `request_key` must be a
new, stable value for a retry of the same action; the same value with different
content is rejected. It must be 8–128 characters using letters, digits, `.`,
`_`, `:` or `-`.

```json
{
  "request_key": "call-123-callback-1",
  "confirmed": true,
  "mobile": "9876543210",
  "caller_name": "Patient name if confirmed",
  "reason": "Caller requested a return call about medication timing",
  "category": "GENERAL",
  "priority": "NORMAL",
  "preferred_at": "2026-09-10T16:00:00+05:30",
  "call_id": "optional-safe-provider-call-id",
  "session_id": "optional-safe-provider-session-id"
}
```

Complaint body fields are `request_key`, `confirmed`, `mobile`, `category`,
`summary`, optional `details`, optional `caller_name`, optional `priority`,
`call_id` and `session_id`.

Appointment-request body fields are `request_key`, `confirmed`, `mobile`,
`reason`, optional `caller_name`, `doctor_name`, `location_name`,
`preferred_date` (`YYYY-MM-DD`), `preferred_time` (`HH:MM`), `preferred_at`,
`priority`, `call_id` and `session_id`.

## Agent rules

Tell the voice agent to:

1. Use timings and slots only to answer availability questions; never invent a
   slot or promise that a staff request is a confirmed appointment.
2. Call a POST action only after explicit caller confirmation.
3. Never ask the caller to speak an OTP, never include an OTP in a tool call,
   and never read lab results over the call.
4. For reports, provide the secure portal route or offer a callback; do not
   ask for a report ID or try to determine whether a report exists.
5. Treat the returned `request_ref`, `callback_ref` and `complaint_ref` as
   support references only.

## Test before going live

With the Railway variable set, test a read-only action first:

```bash
curl -sS \
  -H "X-Clinic-Gateway-Key: <secret>" \
  https://appbilling.up.railway.app/api/v1/omnidim/clinic-info
```

Expect `success: true`. A `503` means the gateway flag/secret or public URL is
not configured; a `401` means the header does not match. Do not test a POST
action with a real patient number until the agent's confirmation prompt has
been reviewed.
