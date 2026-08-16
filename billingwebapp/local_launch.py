"""Small helpers for a friendly local Flask startup experience.

These helpers are deliberately stdlib-only so both ``python app.py`` entry
points can use them without affecting production hosting.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from urllib.parse import urlparse


_DISABLED_VALUES = {"0", "false", "no", "off"}


def _valid_port(raw_value, default_port=5000):
    try:
        port = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default_port
    return port if 1 <= port <= 65535 else default_port


def is_port_available(port):
    """Return whether Flask can bind the given local TCP port."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("0.0.0.0", int(port)))
    except OSError:
        return False
    return True


def prepare_local_server(default_port=5000, *, is_production=False):
    """Choose a free local port and expose its matching browser URL.

    An explicitly configured ``PORT`` is always respected.  On a developer
    machine, an unavailable default port falls through to the next available
    port so ``python app.py`` remains a one-command start.
    """
    configured_port = (os.environ.get("PORT") or "").strip()
    if configured_port:
        port = _valid_port(configured_port, default_port)
    elif is_production:
        port = default_port
    else:
        port = default_port
        for candidate in range(default_port, min(default_port + 20, 65536)):
            if is_port_available(candidate):
                port = candidate
                break

    os.environ["PORT"] = str(port)
    os.environ.setdefault("APP_LAUNCH_URL", f"http://127.0.0.1:{port}")
    return port


def _browser_autostart_enabled(is_production=False):
    setting = (os.environ.get("AUTO_OPEN_BROWSER") or "1").strip().lower()
    return (
        not is_production
        and setting not in _DISABLED_VALUES
        and os.environ.get("WERKZEUG_RUN_MAIN") != "true"
    )


def _wait_for_local_server(url, timeout_seconds=15):
    parsed = urlparse(url)
    host = parsed.hostname
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return True

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.15)
    return False


def _open_browser(url):
    try:
        if sys.platform == "darwin":
            subprocess.Popen(
                ["open", url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        webbrowser.open_new_tab(url)
    except Exception:
        try:
            webbrowser.open_new_tab(url)
        except Exception:
            pass


def schedule_browser_open(*, is_production=False):
    """Open the local app once it is listening, once per CLI launch."""
    if not _browser_autostart_enabled(is_production):
        return

    launch_url = (
        os.environ.get("APP_LAUNCH_URL")
        or f"http://127.0.0.1:{_valid_port(os.environ.get('PORT'), 5000)}"
    ).strip()

    def wait_then_open():
        if _wait_for_local_server(launch_url):
            _open_browser(launch_url)

    threading.Thread(
        target=wait_then_open,
        name="billingwebapp-browser-launcher",
        daemon=True,
    ).start()
