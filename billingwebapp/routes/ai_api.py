"""Versioned, privacy-conscious API boundary for an external call assistant."""

from datetime import datetime, timedelta
from functools import wraps
import hashlib
import hmac
import json
import re
import time
import uuid

from flask import Blueprint, current_app, g, jsonify, request

try:
    from ..models import db, AIAPIRequestAudit
    from ..services.ai_auth_service import (
        AIAuthenticationError,
        authenticate_bearer_token,
        issue_access_token,
        require_scope,
    )
except ImportError:  # pragma: no cover - direct ``python app.py`` execution
    from models import db, AIAPIRequestAudit
    from services.ai_auth_service import (
        AIAuthenticationError,
        authenticate_bearer_token,
        issue_access_token,
        require_scope,
    )


AI_API_VERSION = "v1"
AI_API_PREFIX = "/api/{}/ai".format(AI_API_VERSION)
ai_api_bp = Blueprint("ai_api", __name__, url_prefix=AI_API_PREFIX)

_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
_SAFE_CONTEXT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class AIAPIError(Exception):
    def __init__(self, code, message, status_code=400, details=None):
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)
        self.status_code = int(status_code)
        self.details = details


def _env_flag(value, default=False):
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _remote_addr():
    trust_proxy = _env_flag(current_app.config.get("AI_API_TRUST_PROXY_HEADERS"), False)
    if trust_proxy:
        forwarded_for = (request.headers.get("X-Forwarded-For") or "").split(",", 1)[0].strip()
        if forwarded_for:
            return forwarded_for
    return request.remote_addr or ""


def _safe_header_id(header_name, pattern):
    value = str(request.headers.get(header_name) or "").strip()
    return value if pattern.fullmatch(value) else None


def _request_id():
    return _safe_header_id("X-Request-ID", _SAFE_REQUEST_ID) or str(uuid.uuid4())


def _audit_ip_fingerprint(remote_addr):
    secret = (
        current_app.config.get("AI_AUDIT_FINGERPRINT_SECRET")
        or current_app.secret_key
        or "local-ai-audit-fingerprint"
    )
    return hmac.new(
        str(secret).encode("utf-8"),
        str(remote_addr or "").encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def success_response(data=None, status_code=200):
    response = jsonify(
        {
            "success": True,
            "data": {} if data is None else data,
            "request_id": getattr(g, "ai_request_id", None),
        }
    )
    response.status_code = int(status_code)
    return response


def error_response(code, message, status_code, details=None):
    g.ai_error_code = str(code)
    error = {"code": str(code), "message": str(message)}
    if details and current_app.config.get("TESTING"):
        error["details"] = details
    response = jsonify(
        {
            "success": False,
            "error": error,
            "request_id": getattr(g, "ai_request_id", None),
        }
    )
    response.status_code = int(status_code)
    return response


def require_ai_scope(scope):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            principal = authenticate_bearer_token(
                db.session,
                request.headers.get("Authorization"),
                remote_addr=_remote_addr(),
            )
            g.ai_principal = principal
            require_scope(principal, scope)
            return view(*args, **kwargs)

        return wrapped

    return decorator


def json_object_body():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        raise AIAPIError("INVALID_REQUEST", "A JSON object request body is required", 400)
    return payload


def positive_int(value, field_name):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise AIAPIError("INVALID_REQUEST", "{} must be a positive integer".format(field_name), 400)
    if parsed <= 0:
        raise AIAPIError("INVALID_REQUEST", "{} must be a positive integer".format(field_name), 400)
    return parsed


def nonnegative_int(value, field_name, *, maximum=10000):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise AIAPIError("INVALID_REQUEST", "{} must be a non-negative integer".format(field_name), 400)
    if parsed < 0 or parsed > maximum:
        raise AIAPIError("INVALID_REQUEST", "{} is outside the allowed range".format(field_name), 400)
    return parsed


def enforce_api_rate_limit(limit, *, window_seconds=60):
    principal = getattr(g, "ai_principal", None)
    if principal is None:
        return
    recent_count = AIAPIRequestAudit.query.filter(
        AIAPIRequestAudit.api_client_id == principal.api_client_id,
        AIAPIRequestAudit.endpoint == request.path,
        AIAPIRequestAudit.created_at >= datetime.utcnow() - timedelta(seconds=window_seconds),
    ).count()
    if recent_count >= max(1, int(limit)):
        raise AIAPIError("RATE_LIMITED", "Too many requests", 429)


def set_ai_audit_action(action, *, resource_type=None, resource_id=None):
    """Attach privacy-minimized business context to this request's audit row."""
    g.ai_audit_action = str(action or "")[:80] or None
    g.ai_audit_resource_type = str(resource_type or "")[:50] or None
    g.ai_audit_resource_id = str(resource_id)[:80] if resource_id is not None else None


@ai_api_bp.before_request
def prepare_ai_request_context():
    g.ai_started_at = time.perf_counter()
    g.ai_request_id = _request_id()
    g.ai_call_id = _safe_header_id("X-Call-ID", _SAFE_CONTEXT_ID)
    g.ai_session_id = _safe_header_id("X-Session-ID", _SAFE_CONTEXT_ID)
    g.ai_principal = None
    g.ai_error_code = None
    g.ai_audit_action = None
    g.ai_audit_resource_type = None
    g.ai_audit_resource_id = None
    if not bool(current_app.config.get("AI_API_ENABLED", False)):
        raise AIAPIError(
            "SERVICE_UNAVAILABLE",
            "AI integration API is disabled",
            503,
        )


@ai_api_bp.after_request
def finalize_ai_request(response):
    response.headers["X-Request-ID"] = getattr(g, "ai_request_id", "")
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    # This is a server-to-server API. No Access-Control-Allow-Origin header is
    # emitted, so browsers cannot use it as an open cross-origin data API.
    try:
        elapsed = max(0, int((time.perf_counter() - g.ai_started_at) * 1000))
        principal = getattr(g, "ai_principal", None)
        audited_endpoint = request.path
        if audited_endpoint.startswith(AI_API_PREFIX + "/documents/"):
            audited_endpoint = AI_API_PREFIX + "/documents/{token}"
        audit = AIAPIRequestAudit(
            api_client_id=principal.api_client_id if principal else None,
            request_id=g.ai_request_id,
            call_id=getattr(g, "ai_call_id", None),
            session_id=getattr(g, "ai_session_id", None),
            method=request.method[:10],
            endpoint=audited_endpoint[:255],
            status_code=int(response.status_code),
            outcome="SUCCESS" if response.status_code < 400 else "FAILURE",
            error_code=getattr(g, "ai_error_code", None),
            action=getattr(g, "ai_audit_action", None),
            resource_type=getattr(g, "ai_audit_resource_type", None),
            resource_id=getattr(g, "ai_audit_resource_id", None),
            latency_ms=elapsed,
            client_ip_fingerprint=_audit_ip_fingerprint(_remote_addr()),
        )
        db.session.add(audit)
        db.session.commit()
        current_app.logger.info(
            "ai_api_request %s",
            json.dumps(
                {
                    "request_id": g.ai_request_id,
                    "client_id": principal.client_id if principal else None,
                    "call_id": getattr(g, "ai_call_id", None),
                    "endpoint": audited_endpoint,
                    "method": request.method,
                    "status_code": response.status_code,
                    "latency_ms": elapsed,
                    "error_code": getattr(g, "ai_error_code", None),
                },
                separators=(",", ":"),
            ),
        )
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "AI API audit persistence failed for request_id=%s",
            getattr(g, "ai_request_id", "unknown"),
        )
    return response


@ai_api_bp.errorhandler(AIAuthenticationError)
def handle_authentication_error(exc):
    db.session.rollback()
    response = error_response(exc.code, exc.message, exc.status_code)
    if exc.status_code == 401:
        response.headers["WWW-Authenticate"] = 'Bearer realm="clinic-ai-api"'
    return response


@ai_api_bp.errorhandler(AIAPIError)
def handle_ai_api_error(exc):
    db.session.rollback()
    return error_response(exc.code, exc.message, exc.status_code, exc.details)


@ai_api_bp.errorhandler(404)
def handle_ai_not_found(_exc):
    return error_response("NOT_FOUND", "AI API endpoint was not found", 404)


@ai_api_bp.errorhandler(405)
def handle_ai_method_not_allowed(_exc):
    return error_response("METHOD_NOT_ALLOWED", "HTTP method is not allowed", 405)


@ai_api_bp.errorhandler(Exception)
def handle_ai_unexpected_error(exc):
    db.session.rollback()
    current_app.logger.exception(
        "Unhandled AI API error request_id=%s",
        getattr(g, "ai_request_id", "unknown"),
    )
    return error_response("INTERNAL_ERROR", "The request could not be completed", 500)


def _token_credentials():
    authorization = request.authorization
    if authorization and str(authorization.type or "").lower() == "basic":
        return authorization.username or "", authorization.password or "", None

    payload = request.get_json(silent=True)
    if payload is None and request.form:
        payload = request.form.to_dict(flat=True)
    if not isinstance(payload, dict):
        raise AIAPIError("INVALID_REQUEST", "A JSON or form request body is required", 400)
    client_id = str(payload.get("client_id") or "").strip()
    client_secret = str(payload.get("client_secret") or "")
    requested_scopes = payload.get("scope", payload.get("scopes"))
    return client_id, client_secret, requested_scopes


def _enforce_token_endpoint_rate_limit():
    try:
        limit = max(5, min(int(current_app.config.get("AI_AUTH_RATE_LIMIT_PER_MINUTE", 20)), 100))
    except (TypeError, ValueError):
        limit = 20
    fingerprint = _audit_ip_fingerprint(_remote_addr())
    recent_count = AIAPIRequestAudit.query.filter(
        AIAPIRequestAudit.endpoint == request.path,
        AIAPIRequestAudit.client_ip_fingerprint == fingerprint,
        AIAPIRequestAudit.created_at >= datetime.utcnow() - timedelta(minutes=1),
    ).count()
    if recent_count >= limit:
        raise AIAPIError("RATE_LIMITED", "Too many authentication attempts", 429)


@ai_api_bp.post("/auth/token")
def create_access_token():
    _enforce_token_endpoint_rate_limit()
    client_id, client_secret, requested_scopes = _token_credentials()
    if not client_id or not client_secret:
        raise AIAPIError("INVALID_REQUEST", "Client credentials are required", 400)
    raw_token, token, client, ttl = issue_access_token(
        db.session,
        client_identifier=client_id,
        client_secret=client_secret,
        requested_scopes=requested_scopes,
        remote_addr=_remote_addr(),
        ttl_seconds=current_app.config.get("AI_ACCESS_TOKEN_TTL_SECONDS", 900),
    )
    db.session.commit()
    # Attach only after credentials pass, avoiding client enumeration in audit.
    from types import SimpleNamespace

    g.ai_principal = SimpleNamespace(
        api_client_id=client.id,
        client_id=client.client_id,
    )
    return success_response(
        {
            "access_token": raw_token,
            "token_type": "Bearer",
            "expires_in": ttl,
            "scope": " ".join(sorted(json.loads(token.scopes_json))),
        }
    )


@ai_api_bp.get("/integration/status")
@require_ai_scope("clinic:read")
def integration_status():
    return success_response(
        {
            "api_version": AI_API_VERSION,
            "enabled": True,
            "client": {
                "client_id": g.ai_principal.client_id,
                "name": g.ai_principal.client_name,
                "scopes": sorted(g.ai_principal.scopes),
            },
            "correlation": {
                "call_id": getattr(g, "ai_call_id", None),
                "session_id": getattr(g, "ai_session_id", None),
            },
        }
    )


# Resource routes are split by domain but attached to this single blueprint so
# authentication, feature flags, response envelopes and auditing cannot drift.
try:
    from . import (  # noqa: F401,E402
        ai_clinic_api,
        ai_patient_api,
        ai_appointments_api,
        ai_reports_api,
        ai_operations_api,
        ai_lab_api,
    )
except ImportError:  # pragma: no cover - direct ``python app.py`` execution
    import routes.ai_clinic_api  # noqa: F401,E402
    import routes.ai_patient_api  # noqa: F401,E402
    import routes.ai_appointments_api  # noqa: F401,E402
    import routes.ai_reports_api  # noqa: F401,E402
    import routes.ai_lab_api  # noqa: F401,E402
    import routes.ai_operations_api  # noqa: F401,E402
