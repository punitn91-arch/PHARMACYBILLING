"""Security boundary for machine-to-machine AI API authentication.

The external assistant receives an opaque token. Only hashes of client secrets
and access tokens are persisted, keeping a database leak from immediately
becoming an API credential leak.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import ipaddress
import json
import secrets

from werkzeug.security import check_password_hash, generate_password_hash

try:
    from ..models import AIAPIClient, AIAccessToken
except ImportError:  # pragma: no cover - direct ``python app.py`` execution
    from models import AIAPIClient, AIAccessToken


ALLOWED_AI_SCOPES = frozenset(
    {
        "clinic:read",
        "doctor:read",
        "appointment:read",
        "appointment:create",
        "appointment:update",
        "patient:identify",
        "patient:verify",
        "report:read",
        "report:send",
        "lab:read",
        "callback:create",
        "complaint:create",
        "notification:send",
    }
)

DEFAULT_CLIENT_SCOPES = frozenset({"clinic:read", "doctor:read"})
_DUMMY_SECRET_HASH = generate_password_hash(
    "not-a-real-ai-api-client-secret",
    method="pbkdf2:sha256",
)


class AIAuthenticationError(Exception):
    """Safe authentication/authorization failure suitable for an API response."""

    def __init__(self, code, message, status_code):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = int(status_code)


@dataclass(frozen=True)
class AIPrincipal:
    api_client_id: int
    client_id: str
    client_name: str
    scopes: frozenset
    access_token_id: int

    def has_scope(self, required_scope):
        return required_scope in self.scopes


def _load_string_set(raw_value, allowed_values=None):
    try:
        values = json.loads(raw_value or "[]")
    except (TypeError, ValueError):
        values = []
    if not isinstance(values, list):
        return set()
    normalized = {
        str(value).strip()
        for value in values
        if isinstance(value, str) and str(value).strip()
    }
    if allowed_values is not None:
        normalized.intersection_update(allowed_values)
    return normalized


def serialize_scopes(scopes):
    normalized = normalize_scopes(scopes)
    return json.dumps(sorted(normalized), separators=(",", ":"))


def deserialize_scopes(raw_value):
    return _load_string_set(raw_value, ALLOWED_AI_SCOPES)


def normalize_scopes(scopes):
    if isinstance(scopes, str):
        scopes = scopes.replace(",", " ").split()
    if scopes is None:
        scopes = []
    normalized = {
        str(scope).strip()
        for scope in scopes
        if isinstance(scope, str) and str(scope).strip()
    }
    unknown = normalized.difference(ALLOWED_AI_SCOPES)
    if unknown:
        raise ValueError("Unsupported AI API scope: {}".format(sorted(unknown)[0]))
    return normalized


def serialize_allowed_ips(allowed_ips):
    normalized = normalize_allowed_ips(allowed_ips)
    return json.dumps(normalized, separators=(",", ":"))


def deserialize_allowed_ips(raw_value):
    values = _load_string_set(raw_value)
    valid = []
    for value in values:
        try:
            valid.append(str(ipaddress.ip_network(value, strict=False)))
        except ValueError:
            continue
    return sorted(set(valid))


def normalize_allowed_ips(allowed_ips):
    if isinstance(allowed_ips, str):
        allowed_ips = allowed_ips.replace(",", " ").split()
    if allowed_ips is None:
        allowed_ips = []
    normalized = []
    for raw_value in allowed_ips:
        value = str(raw_value or "").strip()
        if not value:
            continue
        try:
            normalized.append(str(ipaddress.ip_network(value, strict=False)))
        except ValueError as exc:
            raise ValueError("Invalid IP address or CIDR range: {}".format(value)) from exc
    return sorted(set(normalized))


def is_ip_allowed(remote_addr, allowed_ips):
    networks = normalize_allowed_ips(allowed_ips)
    if not networks:
        return True
    try:
        address = ipaddress.ip_address(str(remote_addr or "").strip())
    except ValueError:
        return False
    return any(address in ipaddress.ip_network(network) for network in networks)


def hash_access_token(raw_token):
    return hashlib.sha256((raw_token or "").encode("utf-8")).hexdigest()


def generate_client_credentials():
    return "ai_{}".format(secrets.token_hex(12)), "ais_{}".format(secrets.token_urlsafe(32))


def set_client_secret(client, raw_secret):
    secret = str(raw_secret or "")
    if len(secret) < 32:
        raise ValueError("AI API client secret must be at least 32 characters")
    client.secret_hash = generate_password_hash(secret, method="pbkdf2:sha256")


def verify_client_secret(client, raw_secret):
    candidate_hash = client.secret_hash if client is not None else _DUMMY_SECRET_HASH
    try:
        return check_password_hash(candidate_hash, str(raw_secret or ""))
    except (TypeError, ValueError):
        return False


def create_api_client(
    db_session,
    *,
    name,
    scopes=None,
    allowed_ips=None,
    created_by=None,
):
    clean_name = str(name or "").strip()
    if not clean_name or len(clean_name) > 120:
        raise ValueError("Client name is required and must be at most 120 characters")
    normalized_scopes = normalize_scopes(scopes or DEFAULT_CLIENT_SCOPES)
    if not normalized_scopes:
        raise ValueError("At least one API scope is required")
    normalized_ips = normalize_allowed_ips(allowed_ips)
    client_id, raw_secret = generate_client_credentials()
    client = AIAPIClient(
        name=clean_name,
        client_id=client_id,
        allowed_scopes_json=serialize_scopes(normalized_scopes),
        allowed_ips_json=serialize_allowed_ips(normalized_ips),
        is_active=True,
        secret_version=1,
        created_by=created_by,
        updated_by=created_by,
    )
    set_client_secret(client, raw_secret)
    db_session.add(client)
    db_session.flush()
    return client, raw_secret


def rotate_api_client_secret(db_session, client, *, updated_by=None, now=None):
    now = now or datetime.utcnow()
    raw_secret = "ais_{}".format(secrets.token_urlsafe(32))
    set_client_secret(client, raw_secret)
    client.secret_version = int(client.secret_version or 0) + 1
    client.updated_by = updated_by
    client.updated_at = now
    AIAccessToken.query.filter(
        AIAccessToken.api_client_id == client.id,
        AIAccessToken.revoked_at.is_(None),
    ).update({AIAccessToken.revoked_at: now}, synchronize_session=False)
    db_session.flush()
    return raw_secret


def issue_access_token(
    db_session,
    *,
    client_identifier,
    client_secret,
    requested_scopes=None,
    remote_addr=None,
    ttl_seconds=900,
    now=None,
):
    now = now or datetime.utcnow()
    client_identifier = str(client_identifier or "").strip()
    client = AIAPIClient.query.filter_by(client_id=client_identifier).first()
    secret_valid = verify_client_secret(client, client_secret)
    if client is None or not secret_valid or not bool(client.is_active):
        raise AIAuthenticationError("UNAUTHORIZED", "Invalid service credentials", 401)
    allowed_ips = deserialize_allowed_ips(client.allowed_ips_json)
    if not is_ip_allowed(remote_addr, allowed_ips):
        raise AIAuthenticationError("UNAUTHORIZED", "Invalid service credentials", 401)

    allowed_scopes = deserialize_scopes(client.allowed_scopes_json)
    try:
        requested = normalize_scopes(requested_scopes or allowed_scopes)
    except ValueError:
        raise AIAuthenticationError("FORBIDDEN", "Requested scope is not permitted", 403)
    if not requested or not requested.issubset(allowed_scopes):
        raise AIAuthenticationError("FORBIDDEN", "Requested scope is not permitted", 403)

    ttl = max(60, min(int(ttl_seconds or 900), 3600))
    raw_token = "ait_{}".format(secrets.token_urlsafe(48))
    token = AIAccessToken(
        api_client_id=client.id,
        token_hash=hash_access_token(raw_token),
        scopes_json=serialize_scopes(requested),
        secret_version=int(client.secret_version or 1),
        expires_at=now + timedelta(seconds=ttl),
        created_at=now,
    )
    client.last_used_at = now
    db_session.add(token)
    db_session.flush()
    return raw_token, token, client, ttl


def authenticate_bearer_token(db_session, authorization_header, *, remote_addr=None, now=None):
    now = now or datetime.utcnow()
    scheme, separator, raw_token = str(authorization_header or "").partition(" ")
    if separator != " " or scheme.lower() != "bearer" or not raw_token.strip():
        raise AIAuthenticationError("UNAUTHORIZED", "Bearer token is required", 401)

    token = AIAccessToken.query.filter_by(token_hash=hash_access_token(raw_token.strip())).first()
    if token is None or token.revoked_at is not None or token.expires_at <= now:
        raise AIAuthenticationError("UNAUTHORIZED", "Invalid or expired access token", 401)
    client = db_session.get(AIAPIClient, token.api_client_id)
    if (
        client is None
        or not bool(client.is_active)
        or int(token.secret_version or 0) != int(client.secret_version or 0)
    ):
        raise AIAuthenticationError("UNAUTHORIZED", "Invalid or expired access token", 401)
    if not is_ip_allowed(remote_addr, deserialize_allowed_ips(client.allowed_ips_json)):
        raise AIAuthenticationError("UNAUTHORIZED", "Invalid or expired access token", 401)

    current_scopes = deserialize_scopes(client.allowed_scopes_json)
    token_scopes = deserialize_scopes(token.scopes_json).intersection(current_scopes)
    if not token_scopes:
        raise AIAuthenticationError("FORBIDDEN", "Access token has no active scopes", 403)
    token.last_used_at = now
    client.last_used_at = now
    return AIPrincipal(
        api_client_id=client.id,
        client_id=client.client_id,
        client_name=client.name,
        scopes=frozenset(token_scopes),
        access_token_id=token.id,
    )


def require_scope(principal, required_scope):
    if required_scope not in ALLOWED_AI_SCOPES:
        raise RuntimeError("Unknown server-side AI API scope: {}".format(required_scope))
    if principal is None or not principal.has_scope(required_scope):
        raise AIAuthenticationError("FORBIDDEN", "Insufficient API scope", 403)
