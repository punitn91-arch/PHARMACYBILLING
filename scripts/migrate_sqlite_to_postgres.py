import argparse
import os
from datetime import datetime

from sqlalchemy import MetaData, create_engine, select, text
from werkzeug.security import generate_password_hash


def normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def common_tables(src_meta: MetaData, dst_meta: MetaData):
    src_names = {t.name for t in src_meta.tables.values() if not t.name.startswith("sqlite_")}
    dst_names = set(dst_meta.tables.keys())
    names = src_names.intersection(dst_names)
    return [t for t in src_meta.sorted_tables if t.name in names]


def set_postgres_sequences(dst_conn, dst_meta: MetaData, table_names):
    for name in table_names:
        table = dst_meta.tables.get(name)
        if table is None:
            continue
        pk_cols = list(table.primary_key.columns)
        if len(pk_cols) != 1:
            continue
        pk = pk_cols[0]
        try:
            max_id = dst_conn.execute(
                text(f'SELECT COALESCE(MAX("{pk.name}"), 0) FROM "{name}"')
            ).scalar() or 0
        except Exception:
            continue

        seq = dst_conn.execute(
            text("SELECT pg_get_serial_sequence(:tbl, :col)"),
            {"tbl": name, "col": pk.name},
        ).scalar()
        if not seq:
            continue

        if int(max_id) > 0:
            dst_conn.execute(text(f"SELECT setval('{seq}', {int(max_id)}, true)"))
        else:
            dst_conn.execute(text(f"SELECT setval('{seq}', 1, false)"))


def upsert_login(dst_conn, dst_meta: MetaData, username: str, password: str):
    user_table = dst_meta.tables.get("user")
    if user_table is None:
        print("WARN: 'user' table not found. Skipping login update.")
        return

    cols = {c.name for c in user_table.columns}
    pwd_hash = generate_password_hash(password)

    rows = dst_conn.execute(
        text('SELECT "id", "username", "role" FROM "user" ORDER BY "id"')
    ).mappings().all()

    target_id = None
    for r in rows:
        if (r.get("username") or "").lower() == username.lower():
            target_id = r["id"]
            break

    if target_id is None:
        for r in rows:
            if (r.get("role") or "").lower() == "admin":
                target_id = r["id"]
                break

    if target_id is not None:
        updates = {}
        if "username" in cols:
            updates["username"] = username
        if "password_hash" in cols:
            updates["password_hash"] = pwd_hash
        if "role" in cols:
            updates["role"] = "admin"
        for c in cols:
            if c.startswith("can_"):
                updates[c] = True
        dst_conn.execute(
            user_table.update().where(user_table.c.id == target_id).values(**updates)
        )
        print(f"Login updated on user id={target_id} username='{username}'")
        return

    insert_vals = {}
    if "username" in cols:
        insert_vals["username"] = username
    if "password_hash" in cols:
        insert_vals["password_hash"] = pwd_hash
    if "role" in cols:
        insert_vals["role"] = "admin"
    if "created_at" in cols:
        insert_vals["created_at"] = datetime.utcnow()
    for c in cols:
        if c.startswith("can_"):
            insert_vals[c] = True

    dst_conn.execute(user_table.insert().values(**insert_vals))
    print(f"New admin login created username='{username}'")


def main():
    parser = argparse.ArgumentParser(
        description="Copy current SQLite data to PostgreSQL and optionally set login."
    )
    parser.add_argument(
        "--source",
        default="sqlite:///instance/pharmacy.db",
        help="Source SQLite URL (default: sqlite:///instance/pharmacy.db)",
    )
    parser.add_argument(
        "--dest",
        default=os.environ.get("DATABASE_URL", ""),
        help="Destination PostgreSQL URL (default from DATABASE_URL env)",
    )
    parser.add_argument("--username", default="", help="Set/overwrite admin username")
    parser.add_argument("--password", default="", help="Set/overwrite admin password")
    args = parser.parse_args()

    if not args.dest:
        raise SystemExit("DATABASE_URL missing. Pass --dest or set env DATABASE_URL.")

    src_url = normalize_db_url(args.source)
    dst_url = normalize_db_url(args.dest)

    src_engine = create_engine(src_url)
    dst_engine = create_engine(dst_url)

    src_meta = MetaData()
    dst_meta = MetaData()
    src_meta.reflect(bind=src_engine)
    dst_meta.reflect(bind=dst_engine)
    ordered_tables = common_tables(src_meta, dst_meta)

    if not ordered_tables:
        raise SystemExit("No common tables found between SQLite and PostgreSQL.")

    table_names = [t.name for t in ordered_tables]
    print("Tables to migrate:", ", ".join(table_names))

    with src_engine.connect() as src_conn, dst_engine.begin() as dst_conn:
        quoted = ", ".join(f'"{n}"' for n in table_names)
        dst_conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))

        for src_table in ordered_tables:
            rows = src_conn.execute(select(src_table)).mappings().all()
            if not rows:
                continue
            dst_table = dst_meta.tables[src_table.name]
            payload = [dict(r) for r in rows]
            dst_conn.execute(dst_table.insert(), payload)
            print(f"Copied {src_table.name}: {len(payload)} rows")

        set_postgres_sequences(dst_conn, dst_meta, table_names)

        if args.username and args.password:
            upsert_login(dst_conn, dst_meta, args.username, args.password)

    print("Migration complete.")


if __name__ == "__main__":
    main()
