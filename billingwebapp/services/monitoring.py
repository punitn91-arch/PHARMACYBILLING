from __future__ import annotations

import json
import logging
import os
import time
from functools import lru_cache

from flask import g, request
from sqlalchemy import event


@lru_cache(maxsize=1)
def _load_sentry_sdk():
    try:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

        return sentry_sdk, FlaskIntegration, SqlalchemyIntegration
    except Exception:
        return None, None, None


def configure_monitoring(app, db, *, env_flag):
    sentry_dsn = (os.environ.get("SENTRY_DSN") or "").strip()
    sentry_sdk, flask_integration_cls, sqlalchemy_integration_cls = _load_sentry_sdk()
    if sentry_dsn and sentry_sdk and flask_integration_cls and sqlalchemy_integration_cls:
        sentry_sdk.init(
            dsn=sentry_dsn,
            traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.05") or 0.05),
            profiles_sample_rate=float(os.environ.get("SENTRY_PROFILES_SAMPLE_RATE", "0.0") or 0.0),
            integrations=[flask_integration_cls(), sqlalchemy_integration_cls()],
            environment=os.environ.get("APP_ENV", "production" if env_flag("RAILWAY_ENVIRONMENT", False) else "local"),
            send_default_pii=False,
        )
        app.logger.info("Sentry monitoring enabled")
    elif sentry_dsn:
        app.logger.warning("Sentry DSN is set but sentry-sdk is not installed.")

    request_log_level = (os.environ.get("REQUEST_LOG_LEVEL") or "INFO").strip().upper()
    slow_request_ms = float(os.environ.get("SLOW_REQUEST_MS", "800") or 800)
    slow_query_ms = float(os.environ.get("SLOW_QUERY_MS", "250") or 250)

    @app.before_request
    def _start_request_timer():
        g.request_started_at = time.perf_counter()
        g.sql_query_durations_ms = []

    @app.after_request
    def _log_request(response):
        started_at = getattr(g, "request_started_at", None)
        if started_at is None:
            return response
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        sql_durations = getattr(g, "sql_query_durations_ms", [])
        payload = {
            "method": request.method,
            "path": request.path,
            "status": response.status_code,
            "duration_ms": duration_ms,
            "sql_queries": len(sql_durations),
            "sql_time_ms": round(sum(sql_durations), 2),
            "user": getattr(getattr(g, "user", None), "username", None),
        }
        message = json.dumps(payload, ensure_ascii=True)
        if duration_ms >= slow_request_ms:
            app.logger.warning("slow_request %s", message)
        else:
            getattr(app.logger, request_log_level.lower(), app.logger.info)("request %s", message)
        return response

    with app.app_context():
        engine = db.engine

        @event.listens_for(engine, "before_cursor_execute")
        def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            context._query_start_time = time.perf_counter()

        @event.listens_for(engine, "after_cursor_execute")
        def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            started_at = getattr(context, "_query_start_time", None)
            if started_at is None:
                return
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            if hasattr(g, "sql_query_durations_ms"):
                g.sql_query_durations_ms.append(duration_ms)
            if duration_ms >= slow_query_ms:
                condensed_sql = " ".join((statement or "").strip().split())
                if len(condensed_sql) > 240:
                    condensed_sql = condensed_sql[:240] + "..."
                app.logger.warning(
                    "slow_query %s",
                    json.dumps(
                        {
                            "duration_ms": duration_ms,
                            "statement": condensed_sql,
                        },
                        ensure_ascii=True,
                    ),
                )

