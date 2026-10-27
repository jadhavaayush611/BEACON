"""Logging configuration and sanitization filters for BEACON.

Ensures sensitive credentials, authorization tokens, and private keys
are never written to console or log streams.
"""

import json
import logging
import re
from typing import Any, Optional

from beacon.config import Settings, get_settings

# Sensitive parameter keys and pattern signatures to redact from logs
SENSITIVE_KEY_PATTERNS = re.compile(
    r"(password|secret|token|api[_-]?key|auth|authorization|private[_-]?key)",
    re.IGNORECASE,
)


class SensitiveDataFilter(logging.Filter):
    """Logging filter that scrubs sensitive credential values from messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self._redact_string(record.msg)
        elif isinstance(record.msg, dict):
            record.msg = self._redact_dict(record.msg)
        return True

    def _redact_string(self, text: str) -> str:
        # Redact common key=value or key: value tokens
        redacted = re.sub(
            r"(?i)\b(password|secret|token|api[_-]?key|auth|authorization)\s*[:=]\s*([^\s,;&]+)",
            r"\1=[REDACTED]",
            text,
        )
        # Redact database connection URLs containing user credentials (e.g. postgresql://user:pass@host/db)
        redacted = re.sub(
            r"://([^:@\s]+):([^@\s]+)@",
            r"://\1:[REDACTED]@",
            redacted,
        )
        # Redact sensitive query parameters (e.g. ?token=xyz or &key=abc)
        redacted = re.sub(
            r"(?i)([?&](?:password|token|key|secret|auth)=)[^&\s]+",
            r"\1[REDACTED]",
            redacted,
        )
        return redacted

    def _redact_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        redacted = {}
        for k, v in data.items():
            if SENSITIVE_KEY_PATTERNS.search(str(k)):
                redacted[k] = "[REDACTED]"
            elif isinstance(v, dict):
                redacted[k] = self._redact_dict(v)
            else:
                redacted[k] = v
        return redacted


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter for production log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        log_payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_payload)


def setup_logging(settings: Optional[Settings] = None) -> logging.Logger:
    """Configure root BEACON logging with appropriate formatters and filters."""
    cfg = settings or get_settings()

    logger = logging.getLogger("beacon")
    raw_level = getattr(cfg, "log_level", "INFO")
    level_name = raw_level.upper() if isinstance(raw_level, str) else "INFO"
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    # Avoid duplicate handlers if setup_logging is called repeatedly
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)

        raw_format = getattr(cfg, "log_format", "text")
        if isinstance(raw_format, str) and raw_format.lower() == "json":
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%SZ",
            )

        handler.setFormatter(formatter)
        handler.addFilter(SensitiveDataFilter())
        logger.addHandler(handler)

    return logger


def redact_sensitive_text(text: str) -> str:
    """Scrub sensitive credentials, passwords, tokens, and database URLs from text."""
    flt = SensitiveDataFilter()
    return flt._redact_string(text)


def redact_sensitive_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively scrub sensitive keys and credential values from dictionaries."""
    flt = SensitiveDataFilter()
    return flt._redact_dict(data)
