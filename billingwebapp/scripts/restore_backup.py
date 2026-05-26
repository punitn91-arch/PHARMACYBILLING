#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from app import app  # noqa: E402
from services.infra_safety import restore_backup_snapshot  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a restore drill or apply a snapshot restore for the billing app."
    )
    parser.add_argument(
        "--snapshot",
        default="latest",
        help="Snapshot name to restore. Defaults to latest.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the snapshot and print a restore summary without changing data.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the restore to the configured database and upload storage.",
    )
    parser.add_argument(
        "--skip-uploads",
        action="store_true",
        help="Restore database rows only and leave upload directories untouched.",
    )
    parser.add_argument(
        "--allow-production",
        action="store_true",
        help="Allow an apply restore when APP_ENV=production. Use only during a planned recovery window.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.apply and args.dry_run:
        print("Use either --apply or --dry-run, not both.", file=sys.stderr)
        return 2

    dry_run = True
    if args.apply:
        dry_run = False
    elif args.dry_run:
        dry_run = True

    with app.app_context():
        result = restore_backup_snapshot(
            app,
            snapshot_name=args.snapshot,
            include_uploads=not args.skip_uploads,
            dry_run=dry_run,
            allow_production=args.allow_production,
        )

    row_total = sum((result.get("table_counts") or {}).values())
    archive_keys = ", ".join(sorted((result.get("upload_archives") or {}).keys())) or "-"
    mode_label = "DRY RUN" if result.get("dry_run") else "APPLY"
    print(f"[{mode_label}] Snapshot: {result.get('snapshot_name')}")
    print(f"Rows in manifest: {row_total}")
    print(f"Upload archives: {archive_keys}")
    if result.get("dry_run"):
        print("No data was changed.")
    else:
        print("Restore completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
