"""CLI utility to apply the Phase 2C / v1.1 additive database migration for universal observation types.

Usage:
    python scripts/migrate_v1_1.py [--url DATABASE_URL] [--dry-run] [--confirm]
"""

import argparse
from pathlib import Path
import sys
from typing import Optional, Sequence

from sqlalchemy import text

from beacon.config import get_settings
from beacon.logging_config import redact_sensitive_text, setup_logging
from beacon.storage import create_db_engine

MIGRATION_SQL_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "beacon"
    / "storage"
    / "migrations"
    / "v1_1_observation_types.sql"
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
        print("--- Dry-run: v1.1 Migration DDL ---")
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
        description="Apply Phase 2C / v1.1 additive database migration for observation types."
    )
    parser.add_argument(
        "--url", help="Override database URL (defaults to BEACON_DATABASE_URL)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print migration DDL without executing changes",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Explicit confirmation required to apply migration against target database",
    )
    args = parser.parse_args(argv)
    if args.dry_run:
        apply_migration(None, dry_run=True)
        print("Dry-run complete. No changes were committed.")
        return 0

    if not args.confirm:
        print(
            "ERROR: Migration execution requires explicit confirmation. "
            "Please provide --confirm to apply the v1.1 migration against the target database, "
            "or use --dry-run to inspect the migration SQL without applying changes.",
            file=sys.stderr,
        )
        return 1

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

    lower_url = url.lower()
    if "pricepilot" in lower_url or ":5432" in lower_url:
        print(
            f"ERROR: Prohibited target database host/port or database name: {redact_sensitive_text(url)}. "
            "Migration blocked to safeguard protected infrastructure.",
            file=sys.stderr,
        )
        return 1

    try:
        engine = create_db_engine(url)
        print(f"Applying v1.1 observation types migration to: {redact_sensitive_text(url)}")
        apply_migration(engine)
        print("v1.1 migration applied successfully.")
        return 0
    except Exception as ex:
        print(f"Migration failed: {redact_sensitive_text(str(ex))}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
