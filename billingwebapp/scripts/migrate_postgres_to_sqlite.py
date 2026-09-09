import argparse
import os

from sqlalchemy import MetaData, create_engine, select, text


def normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def common_table_names(src_meta: MetaData, dst_meta: MetaData):
    src_names = {t.name for t in src_meta.tables.values() if not t.name.startswith("sqlite_")}
    dst_names = {t.name for t in dst_meta.tables.values() if not t.name.startswith("sqlite_")}
    return src_names.intersection(dst_names)


def ordered_source_tables(src_meta: MetaData, table_names):
    return [t for t in src_meta.sorted_tables if t.name in table_names]


def clear_destination(dst_conn, dst_engine, table_names):
    dialect = (dst_engine.dialect.name or "").lower()
    if dialect == "sqlite":
        dst_conn.execute(text("PRAGMA foreign_keys=OFF"))
        for name in table_names:
            dst_conn.execute(text(f'DELETE FROM "{name}"'))
        for name in table_names:
            try:
                dst_conn.execute(
                    text("DELETE FROM sqlite_sequence WHERE name = :name"),
                    {"name": name},
                )
            except Exception:
                # sqlite_sequence may not exist for all tables.
                pass
        dst_conn.execute(text("PRAGMA foreign_keys=ON"))
        return

    if dialect == "postgresql":
        quoted = ", ".join(f'"{n}"' for n in table_names)
        dst_conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))
        return

    for name in table_names:
        dst_conn.execute(text(f'DELETE FROM "{name}"'))


def main():
    parser = argparse.ArgumentParser(
        description="Copy PostgreSQL data to local SQLite database."
    )
    parser.add_argument(
        "--source",
        default=os.environ.get("DATABASE_URL", ""),
        help="Source PostgreSQL URL (default from DATABASE_URL env)",
    )
    parser.add_argument(
        "--dest",
        default="sqlite:///billingwebapp/instance/pharmacy.db",
        help="Destination SQLite URL (default: sqlite:///billingwebapp/instance/pharmacy.db)",
    )
    args = parser.parse_args()

    if not args.source:
        raise SystemExit("Source URL missing. Pass --source or set DATABASE_URL.")

    src_url = normalize_db_url(args.source)
    dst_url = normalize_db_url(args.dest)

    src_engine = create_engine(src_url)
    dst_engine = create_engine(dst_url)

    src_meta = MetaData()
    dst_meta = MetaData()
    src_meta.reflect(bind=src_engine)
    dst_meta.reflect(bind=dst_engine)

    names = common_table_names(src_meta, dst_meta)
    if not names:
        raise SystemExit("No common tables found between source and destination.")

    tables = ordered_source_tables(src_meta, names)
    table_names = [t.name for t in tables]
    print("Tables to copy:", ", ".join(table_names))

    with src_engine.connect() as src_conn, dst_engine.begin() as dst_conn:
        clear_destination(dst_conn, dst_engine, table_names)

        for src_table in tables:
            rows = src_conn.execute(select(src_table)).mappings().all()
            if not rows:
                print(f"Copied {src_table.name}: 0 rows")
                continue
            dst_table = dst_meta.tables[src_table.name]
            payload = [dict(r) for r in rows]
            dst_conn.execute(dst_table.insert(), payload)
            print(f"Copied {src_table.name}: {len(payload)} rows")

    print("Copy complete.")


if __name__ == "__main__":
    main()
