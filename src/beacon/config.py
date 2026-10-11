"""BEACON pipeline configuration management.

Provides typed, environment-driven configuration using Pydantic Settings.
Configuration is centralized here to avoid split authority and competing
settings implementations.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings for BEACON.

    Reads configuration from environment variables prefixed with `BEACON_`
    and optionally from a local `.env` file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="BEACON_",
        extra="ignore",
    )

    # Runtime environment
    environment: Literal["development", "test", "production"] = Field(
        default="development",
        description="Execution environment tier",
    )

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Application logging severity threshold",
    )
    log_format: Literal["text", "json"] = Field(
        default="text",
        description="Log output format (text or structured json)",
    )

    # Filesystem paths
    data_dir: Path = Field(
        default=Path("data"),
        description="Root data directory",
    )
    raw_data_dir: Path = Field(
        default=Path("data/raw"),
        description="Directory for ingested raw documents",
    )
    sample_data_dir: Path = Field(
        default=Path("data/sample"),
        description="Directory for synthetic sample datasets",
    )

    # Pipeline parameters
    enable_strict_validation: bool = Field(
        default=True,
        description="Whether to enforce strict schema constraints during ingestion",
    )
    default_confidence_threshold: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Minimum default confidence score for high-trust observations",
    )

    # Database & Storage
    database_url: Optional[str] = Field(
        default=None,
        description="PostgreSQL connection URL (e.g. postgresql+psycopg://user:pass@host:5432/dbname)",
    )
    database_echo: bool = Field(
        default=False,
        description="Enable SQLAlchemy query echo logging",
    )
    database_pool_size: int = Field(
        default=5,
        description="Database connection pool size",
    )
    database_max_overflow: int = Field(
        default=10,
        description="Database connection pool maximum overflow connections",
    )
    test_database_url: Optional[str] = Field(
        default=None,
        description="Explicit test database URL for PostgreSQL integration tests",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton instance of application settings."""
    return Settings()
