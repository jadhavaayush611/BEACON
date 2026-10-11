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
        return re.sub(
            r"(?i)\b(password|secret|token|api[_-]?key|auth|authorization)\s*[:=]\s*([^\s,;]+)",
            r"\1=[REDACTED]",
            text,
        )

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
    logger.setLevel(getattr(logging, cfg.log_level.upper(), logging.INFO))

    # Avoid duplicate handlers if setup_logging is called repeatedly
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, cfg.log_level.upper(), logging.INFO))

        if cfg.log_format.lower() == "json":
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
