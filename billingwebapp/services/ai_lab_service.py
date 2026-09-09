"""Public-safe lab test catalog queries for the AI API.

Reuses the existing ``LabTest`` price-master table (the same one billing
staff use) instead of a second lab catalog. Price, fasting requirement, and
turnaround remain admin-approved data; this module never infers or guesses a
clinical fact that was not entered by staff.
"""

import json

try:
    from ..models import LabTest
except ImportError:  # pragma: no cover
    from models import LabTest


# Common spoken/typed abbreviations mapped to catalog search terms. This is a
# search-query expansion only -- it never changes what a test actually is,
# and an admin-entered alias on the test itself always applies too.
_ALIAS_HINTS = {
    "cbc": ("complete blood count", "cbc"),
    "complete blood count": ("cbc",),
    "tft": ("thyroid", "tft", "thyroid function"),
    "thyroid profile": ("tft", "thyroid function"),
    "thyroid function test": ("tft", "thyroid profile"),
    "sugar test": ("glucose", "blood sugar"),
    "blood sugar": ("glucose", "sugar test"),
    "fbs": ("fasting blood sugar", "glucose"),
    "rbs": ("random blood sugar", "glucose"),
    "hba1c": ("glycated haemoglobin", "glycated hemoglobin", "a1c"),
    "a1c": ("hba1c",),
    "lft": ("liver function", "lft"),
    "kft": ("kidney function", "renal function", "kft"),
    "rft": ("kidney function", "renal function", "rft"),
    "lipid profile": ("cholesterol", "lipid"),
    "cholesterol": ("lipid profile",),
    "cbc esr": ("cbc", "esr"),
    "urine routine": ("urine", "urinalysis"),
}


def _json_list(raw_value):
    try:
        value = json.loads(raw_value or "[]")
    except (TypeError, ValueError):
        value = []
    return [str(item).strip() for item in value if str(item).strip()] if isinstance(value, list) else []


def serialize_lab_test(test):
    return {
        "id": test.id,
        "test_code": test.test_code,
        "name": test.name,
        "category": test.category,
        "specimen_type": test.specimen_type,
        "preparation": test.preparation,
        "fasting_required": bool(test.fasting_required),
        "turnaround": test.turnaround_text,
        "price": float(test.default_price or 0),
        "aliases": _json_list(test.aliases_json),
    }


def _expanded_terms(query):
    normalized = str(query or "").strip().lower()
    terms = {normalized} if normalized else set()
    terms.update(_ALIAS_HINTS.get(normalized, ()))
    return [term for term in terms if term]


def search_lab_tests(query, *, category=None, limit=20, offset=0):
    base = LabTest.query.filter(LabTest.is_active.is_(True))
    if category:
        base = base.filter(LabTest.category.ilike("%{}%".format(str(category)[:100])))

    query = str(query or "").strip()
    if not query:
        rows = base.order_by(LabTest.name.asc()).offset(offset).limit(limit).all()
        return [serialize_lab_test(row) for row in rows], len(rows)

    terms = _expanded_terms(query)
    candidate_ids = set()
    ordered_rows = []
    for term in terms:
        like = "%{}%".format(term[:120])
        matches = base.filter(
            LabTest.name.ilike(like)
            | LabTest.test_code.ilike(like)
            | LabTest.category.ilike(like)
            | LabTest.aliases_json.ilike(like)
        ).order_by(LabTest.name.asc()).all()
        for row in matches:
            if row.id not in candidate_ids:
                candidate_ids.add(row.id)
                ordered_rows.append(row)

    page = ordered_rows[offset:offset + limit]
    return [serialize_lab_test(row) for row in page], len(ordered_rows)


def get_lab_test(test_id):
    return LabTest.query.filter_by(id=test_id, is_active=True).first()


def lab_test_categories():
    rows = (
        LabTest.query.with_entities(LabTest.category)
        .filter(LabTest.is_active.is_(True), LabTest.category.isnot(None), LabTest.category != "")
        .distinct()
        .order_by(LabTest.category.asc())
        .all()
    )
    return [row[0] for row in rows if row[0]]
