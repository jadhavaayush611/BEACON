"""Unit tests for BEACON configuration and logging subsystem."""

import logging
from pathlib import Path
import pytest

from beacon.config import Settings, get_settings
from beacon.logging_config import SensitiveDataFilter, setup_logging


class TestConfiguration:
    """Tests for typed Settings loading and environment overrides."""

    def test_default_settings(self) -> None:
        settings = Settings()
        assert settings.environment == "development"
        assert settings.log_level == "INFO"
        assert settings.log_format == "text"
        assert settings.data_dir == Path("data")
        assert settings.raw_data_dir == Path("data/raw")
        assert settings.sample_data_dir == Path("data/sample")
        assert settings.enable_strict_validation is True
        assert settings.default_confidence_threshold == 0.5

    def test_environment_variable_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BEACON_ENVIRONMENT", "production")
        monkeypatch.setenv("BEACON_LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("BEACON_LOG_FORMAT", "json")
        monkeypatch.setenv("BEACON_DEFAULT_CONFIDENCE_THRESHOLD", "0.75")

        settings = Settings()
        assert settings.environment == "production"
        assert settings.log_level == "DEBUG"
        assert settings.log_format == "json"
        assert settings.default_confidence_threshold == 0.75

    def test_get_settings_caching(self) -> None:
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2


class TestLoggingSubsystem:
    """Tests for logging configuration, sanitization, and formatters."""

    def test_setup_logging_initialization(self) -> None:
        custom_settings = Settings(log_level="DEBUG", log_format="text")
        logger = setup_logging(custom_settings)
        assert logger.name == "beacon"
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) > 0

    def test_sensitive_data_filter_redacts_strings(self) -> None:
        data_filter = SensitiveDataFilter()
        record = logging.LogRecord(
            name="beacon.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="User login attempted with password=SecretPassword123 and token: AbcToken890",
            args=(),
            exc_info=None,
        )
        assert data_filter.filter(record) is True
        assert "password=[REDACTED]" in record.msg
        assert "token:[REDACTED]" in record.msg or "token=[REDACTED]" in record.msg
        assert "SecretPassword123" not in record.msg
        assert "AbcToken890" not in record.msg

    def test_sensitive_data_filter_redacts_dicts(self) -> None:
        data_filter = SensitiveDataFilter()
        payload = {
            "source_file": "events.json",
            "api_key": "top-secret-api-key",
            "auth_token": "bearer-12345",
            "nested": {
                "password": "my_db_password",
                "normal_field": "safe_value",
            },
        }
        record = logging.LogRecord(
            name="beacon.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=20,
            msg=payload,
            args=(),
            exc_info=None,
        )
        assert data_filter.filter(record) is True
        assert record.msg["api_key"] == "[REDACTED]"
        assert record.msg["auth_token"] == "[REDACTED]"
        assert record.msg["nested"]["password"] == "[REDACTED]"
        assert record.msg["nested"]["normal_field"] == "safe_value"
