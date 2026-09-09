"""Public-safe lab test catalog endpoints for the AI API.

Reuses the existing lab price-master (``LabTest``) and its admin screen; this
module only adds read endpoints shaped for a caller (search, detail, and
category list) instead of a second lab catalog.
"""

from flask import request

try:
    from ..services.ai_lab_service import (
        get_lab_test,
        lab_test_categories,
        search_lab_tests,
        serialize_lab_test,
    )
    from .ai_api import (
        AIAPIError,
        ai_api_bp,
        nonnegative_int,
        positive_int,
        require_ai_scope,
        success_response,
    )
except ImportError:  # pragma: no cover
    from services.ai_lab_service import (
        get_lab_test,
        lab_test_categories,
        search_lab_tests,
        serialize_lab_test,
    )
    from routes.ai_api import (
        AIAPIError,
        ai_api_bp,
        nonnegative_int,
        positive_int,
        require_ai_scope,
        success_response,
    )


@ai_api_bp.get("/lab/tests/search")
@require_ai_scope("lab:read")
def search_lab_tests_endpoint():
    query = str(request.args.get("q") or "").strip()[:120]
    category = str(request.args.get("category") or "").strip()[:100] or None
    limit = min(50, positive_int(request.args.get("limit", 20), "limit"))
    offset = nonnegative_int(request.args.get("offset", 0), "offset")
    items, total = search_lab_tests(query, category=category, limit=limit, offset=offset)
    return success_response({"items": items, "count": len(items), "total": total, "offset": offset})


@ai_api_bp.get("/lab/tests/<int:test_id>")
@require_ai_scope("lab:read")
def get_lab_test_endpoint(test_id):
    test = get_lab_test(test_id)
    if not test:
        raise AIAPIError("LAB_TEST_NOT_FOUND", "Lab test was not found", 404)
    return success_response(serialize_lab_test(test))


@ai_api_bp.get("/lab/categories")
@require_ai_scope("lab:read")
def get_lab_categories_endpoint():
    return success_response({"items": lab_test_categories()})
