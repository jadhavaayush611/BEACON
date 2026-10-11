"""Unit tests for scripts/init_db.py safety checks and confirmation workflows."""

import io
from pathlib import Path
from unittest.mock import patch
import pytest

import scripts.init_db as init_db_script


class DummyTTYStream(io.StringIO):
    """Mock stream that simulates an interactive terminal (isatty = True)."""

    def isatty(self) -> bool:
        return True


class DummyNonTTYStream(io.StringIO):
    """Mock stream that simulates a non-interactive pipe/redirection (isatty = False)."""

    def isatty(self) -> bool:
        return False


class TestInitDbSafety:
    """Tests for CLI database initialization safety and confirmation prompts."""

    @pytest.fixture
    def test_db_path(self, tmp_path: Path) -> str:
        db_file = tmp_path / "test_init.db"
        return f"sqlite:///{db_file.as_posix()}"

    def test_ordinary_init_does_not_prompt(self, test_db_path: str) -> None:
        """Ordinary execution without --drop-first creates tables and does not prompt."""
        with patch("builtins.input") as mock_input:
            ret = init_db_script.main(["--url", test_db_path])
            assert ret == 0
            mock_input.assert_not_called()

    def test_drop_first_interactive_affirmative(self, test_db_path: str) -> None:
        """When user types 'y' in interactive terminal, tables are recreated and returns 0."""
        init_db_script.main(["--url", test_db_path])

        with patch("scripts.init_db.sys.stdin", DummyTTYStream()), \
             patch("builtins.input", return_value="y") as mock_input:
            ret = init_db_script.main(["--url", test_db_path, "--drop-first"])
            assert ret == 0
            mock_input.assert_called_once()

    def test_drop_first_interactive_declined(self, test_db_path: str) -> None:
        """When user types 'n' or declines prompt, execution aborts with exit code 0 without dropping."""
        init_db_script.main(["--url", test_db_path])

        with patch("scripts.init_db.sys.stdin", DummyTTYStream()), \
             patch("builtins.input", return_value="n") as mock_input, \
             patch("scripts.init_db.drop_db") as mock_drop:
            ret = init_db_script.main(["--url", test_db_path, "--drop-first"])
            assert ret == 0
            mock_input.assert_called_once()
            mock_drop.assert_not_called()

    def test_drop_first_non_interactive_without_yes_fails_safe(self, test_db_path: str, capsys) -> None:
        """In non-interactive execution without --yes, execution fails safe with exit code 1."""
        init_db_script.main(["--url", test_db_path])

        with patch("scripts.init_db.sys.stdin", DummyNonTTYStream()), \
             patch("scripts.init_db.drop_db") as mock_drop:
            ret = init_db_script.main(["--url", test_db_path, "--drop-first"])
            assert ret == 1
            mock_drop.assert_not_called()

            captured = capsys.readouterr()
            assert "ERROR: --drop-first was specified in a non-interactive environment without --yes" in captured.err

    def test_drop_first_with_yes_succeeds_without_prompt(self, test_db_path: str) -> None:
        """Providing --yes allows --drop-first to proceed non-interactively without prompt."""
        with patch("builtins.input") as mock_input:
            ret = init_db_script.main(["--url", test_db_path, "--drop-first", "--yes"])
            assert ret == 0
            mock_input.assert_not_called()

    def test_missing_url_exits_with_error(self, capsys) -> None:
        """When no URL is available from arguments or settings, main exits with code 1."""
        with patch("scripts.init_db.get_settings") as mock_settings:
            mock_settings.return_value.database_url = None
            ret = init_db_script.main([])
            assert ret == 1
            captured = capsys.readouterr()
            assert "ERROR: No database URL configured" in captured.err
