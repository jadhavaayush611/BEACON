"""CLI utility to initialize or inspect the BEACON PostgreSQL database schema.

Usage:
    python scripts/init_db.py [--url DATABASE_URL] [--drop-first] [--yes]
"""

import argparse
import sys
from typing import Optional, Sequence

from beacon.config import get_settings
from beacon.logging_config import setup_logging
from beacon.storage import create_db_engine, drop_db, init_db


def confirm_drop(yes: bool, stdin=None) -> Optional[bool]:
    """Check confirmation before dropping database tables.

    Returns:
        True: Confirmed (either via --yes or interactive affirmative).
        False: Declined by user in interactive prompt (exit 0).
        None: Non-interactive environment without --yes (fail safe, exit 1).
    """
    if yes:
        return True

    stream = stdin if stdin is not None else sys.stdin
    if stream and hasattr(stream, "isatty") and stream.isatty():
        try:
            answer = input(
                "WARNING: --drop-first will permanently destroy all application tables. Continue? [y/N]: "
            ).strip().lower()
            return answer in ("y", "yes")
        except (EOFError, KeyboardInterrupt):
            return False

    # Non-interactive without --yes
    print(
        "ERROR: --drop-first was specified in a non-interactive environment without --yes. "
        "Provide --yes to confirm destructive table deletion.",
        file=sys.stderr,
    )
    return None


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Initialize BEACON database tables.")
    parser.add_argument("--url", help="Override database URL (defaults to BEACON_DATABASE_URL)")
    parser.add_argument("--drop-first", action="store_true", help="Drop existing tables before recreating")
    parser.add_argument("-y", "--yes", action="store_true", help="Confirm destructive table deletion non-interactively")
    args = parser.parse_args(argv)

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

    # Safety confirmation before any destructive database operation
    if args.drop_first:
        confirmation = confirm_drop(yes=args.yes)
        if confirmation is None:
            # Failed safe in non-interactive environment
            return 1
        if not confirmation:
            # User declined prompt
            print("Aborted: Database tables were not dropped.")
            return 0

    try:
        engine = create_db_engine(database_url=url)
        if args.drop_first:
            print("Dropping existing tables...")
            drop_db(engine)
        print("Initializing tables...")
        init_db(engine)
        print("Database schema successfully initialized.")
        return 0
    except Exception as err:
        print(f"ERROR: Database initialization failed: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
