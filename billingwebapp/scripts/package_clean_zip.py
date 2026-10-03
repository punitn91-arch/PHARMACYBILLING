"""Create a shareable zip of the project WITHOUT secrets, patient data or venv.

Usage (from the project root, the folder that contains Procfile):
    python billingwebapp/scripts/package_clean_zip.py
    python billingwebapp/scripts/package_clean_zip.py --output ../billingwebapp_clean.zip

Never share the raw project folder: it contains .env (live API keys),
instance/*.db and backup/*.db (patient and sales data) and a 600+ MB venv.
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import zipfile

EXCLUDED_DIRS = {
    "venv", ".venv", "env", "__pycache__", ".pycache_tmp", ".git", "instance",
    "backup", "backups", "node_modules", ".pytest_cache", "outbox", "uploads",
}
EXCLUDED_FILE_PATTERNS = [
    ".env", ".env.*", "*.db", "*.sqlite", "*.sqlite3", "*.pyc", ".DS_Store",
    "*.log", ".secret_key", "*.xlsx",
]
ALLOWED_DOTENV = {".env.example"}


def should_skip_file(name: str) -> bool:
    if name in ALLOWED_DOTENV:
        return False
    return any(fnmatch.fnmatch(name, pattern) for pattern in EXCLUDED_FILE_PATTERNS)


def build_zip(root: str, output: str) -> tuple[int, int]:
    root = os.path.abspath(root)
    output = os.path.abspath(output)
    added = skipped = 0
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for current, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
            for name in files:
                path = os.path.join(current, name)
                if os.path.abspath(path) == output or should_skip_file(name):
                    skipped += 1
                    continue
                archive.write(path, os.path.relpath(path, os.path.dirname(root)))
                added += 1
    return added, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="Project root folder (default: current folder)")
    parser.add_argument("--output", default="billingwebapp_clean.zip")
    args = parser.parse_args()
    added, skipped = build_zip(args.root, args.output)
    print(f"Created {args.output}: {added} files added, {skipped} sensitive/generated files skipped.")


if __name__ == "__main__":
    main()
