"""CLI utility to apply the Phase 2B / v1.0 additive database migration for pipeline_runs.

Usage:
    python scripts/migrate_v1_0.py [--url DATABASE_URL] [--dry-run]
"""

import argparse
from pathlib import Path
import sys
from typing import Optional, Sequence

from sqlalchemy import text

from beacon.config import get_settings
from beacon.logging_config import setup_logging
from beacon.storage import create_db_engine

MIGRATION_SQL_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "beacon"
    / "storage"
    / "migrations"
    / "v1_0_pipeline_runs.sql"
)


def load_migration_sql() -> str:
    """Load the raw migration SQL text."""
    if not MIGRATION_SQL_PATH.exists():
        raise FileNotFoundError(f"Migration script not found at {MIGRATION_SQL_PATH}")
    return MIGRATION_SQL_PATH.read_text(encoding="utf-8")


def apply_migration(engine, dry_run: bool = False) -> None:
    """Execute the migration DDL against the target database engine."""
    sql = load_migration_sql()
    if dry_run:
        print("--- Dry-run: v1.0 Migration DDL ---")
        print(sql)
        print("--- End DDL ---")
        return

    # Execute statements in a single transactional block
    with engine.begin() as conn:
        statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        for stmt in statements:
            conn.execute(text(stmt))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Apply Phase 2B / v1.0 additive database migration for pipeline_runs."
    )
    parser.add_argument(
        "--url", help="Override database URL (defaults to BEACON_DATABASE_URL)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print migration DDL without executing changes",
    )
    args = parser.parse_args(argv)
    if args.dry_run:
        apply_migration(None, dry_run=True)
        print("Dry-run complete. No changes were committed.")
        return 0

    settings = get_settings()
    setup_logging(settings)
    url = args.url or settings.database_url
    if not url:
        print(
            "ERROR: No database URL configured. "
            "Please provide --url or set the BEACON_DATABASE_URL environment variable.",
            file=sys.stderr,
        )
        return 1

    try:
        engine = create_db_engine(url)
        print(f"Applying v1.0 migration to {engine.url} ...")
        apply_migration(engine, dry_run=False)
        print("Migration v1.0 applied successfully.")
        return 0
    except Exception as err:
        print(f"ERROR: Migration failed: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
