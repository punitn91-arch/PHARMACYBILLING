# AI Integration API foundation

The external Clinic Call Assistant must use the versioned HTTPS API and must
never connect to the clinic database. The API is disabled by default.

## Safe activation sequence

1. Back up the production database.
2. Run `flask db upgrade` and verify revision `20260831_05`.
3. Set a strong `SECRET_KEY` and a separate
   `AI_AUDIT_FINGERPRINT_SECRET`.
4. Keep `AI_API_ENABLED=false` while creating the first client in
   **System → AI Integration**.
5. Copy the generated secret once into the Call Assistant's secret manager.
6. Assign only required scopes and, where stable egress IPs are available,
   configure an IP/CIDR allowlist.
7. Enable `AI_API_ENABLED=true` in staging, verify token exchange and
   `/api/v1/ai/integration/status`, then repeat the controlled rollout in
   production.

## Authentication

Exchange a client credential using HTTP Basic authentication or a JSON/form
body at `POST /api/v1/ai/auth/token`. The returned bearer token is opaque,
short-lived, and shown only in the response. Database rows contain hashes only.

Client secret rotation immediately revokes all tokens created with an older
secret. Disabling a client revokes its active tokens as well.

## Correlation and privacy

The API accepts `X-Request-ID`, `X-Call-ID`, and `X-Session-ID` using a strict
identifier character set and length. Request audit records contain endpoint,
status, latency, correlation IDs and a keyed IP fingerprint. Request bodies,
credentials, OTPs, report tokens, transcripts and medical data are not stored
in API request audit rows.

The complete machine-readable contract is `docs/openapi-ai-v1.yaml`. The
end-to-end setup and Call Assistant workflow are documented in
`docs/AI_INTEGRATION_GUIDE.md`.
