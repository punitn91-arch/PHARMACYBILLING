Flask-Migrate / Alembic has been wired into the app in a non-breaking way.

Once dependencies are installed, initialize migrations from the project root:

```bash
source venv/bin/activate
pip install -r requirements.txt
export FLASK_APP=app.py
flask db init
flask db migrate -m "initial schema baseline"
flask db upgrade
```

Notes:
- Runtime `ensure_column(...)` guards are still kept for backward compatibility on older live databases.
- New schema work should move through `flask db migrate` / `flask db upgrade` going forward.
