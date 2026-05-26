from __future__ import annotations

try:
    from ..services.inventory import build_billing_page_context
except ImportError:  # pragma: no cover - script/local fallback
    from services.inventory import build_billing_page_context


def prepare_billing_context(medicines, restored_hold_bill=None):
    payload = build_billing_page_context(medicines)
    payload["medicines"] = medicines
    payload["restored_hold_bill"] = restored_hold_bill
    return payload
