"""Unit tests for scripts/migrate_v0_4.py migration CLI and SQL safety."""

from unittest.mock import patch
import pytest

from scripts.migrate_v0_4 import load_migration_sql, main


class TestMigrationV04:
    """Tests for Phase 4 migration utility."""

    def test_migration_sql_file_exists_and_contains_expected_tables(self) -> None:
        sql = load_migration_sql()
        assert "CREATE TABLE IF NOT EXISTS events" in sql
        assert "CREATE TABLE IF NOT EXISTS event_observations" in sql
        assert "CREATE TABLE IF NOT EXISTS resolution_decisions" in sql
        assert "CREATE TABLE IF NOT EXISTS merge_candidates" in sql

    def test_dry_run_flag_exits_cleanly(self, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["--dry-run"])
        assert exit_code == 0
        captured = capsys.readouterr()
        assert "Dry-run: Phase 4 Migration DDL" in captured.out
        assert "Dry-run complete. No changes were committed." in captured.out

    def test_missing_database_url_exits_with_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch("scripts.migrate_v0_4.get_settings") as mock_settings:
            mock_settings.return_value.database_url = None
            exit_code = main([])
            assert exit_code == 1
            captured = capsys.readouterr()
            assert "ERROR: No database URL configured" in captured.err
