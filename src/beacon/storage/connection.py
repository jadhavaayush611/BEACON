"""Database engine, session management, and schema initialization."""

from contextlib import contextmanager
import logging
from typing import Generator, Optional

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from beacon.config import Settings, get_settings
from beacon.storage.schema import (
    Base,
    EventModel,
    EventObservationModel,
    MergeCandidateModel,
    ObservationModel,
    PipelineRunModel,
    QualityResultModel,
    ResolutionDecisionModel,
    SourceModel,
)

__all__ = [
    "Base",
    "create_db_engine",
    "get_session_factory",
    "get_session",
    "init_db",
    "drop_db",
    "EventModel",
    "EventObservationModel",
    "MergeCandidateModel",
    "ObservationModel",
    "PipelineRunModel",
    "QualityResultModel",
    "ResolutionDecisionModel",
    "SourceModel",
]

logger = logging.getLogger("beacon.storage.connection")


def create_db_engine(
    database_url: Optional[str] = None,
    echo: Optional[bool] = None,
    settings: Optional[Settings] = None,
    **engine_kwargs,
) -> Engine:
    """Create a SQLAlchemy Engine instance configured from settings or explicit arguments."""
    cfg = settings or get_settings()
    url = database_url or cfg.database_url
    if not url:
        raise ValueError(
            "Database URL is not configured. Provide database_url or set BEACON_DATABASE_URL."
        )

    is_echo = cfg.database_echo if echo is None else echo

    # Configure connection pool settings if connecting to standard relational engines
    kwargs = dict(engine_kwargs)
    if "sqlite" not in url.lower():
        if "pool_size" not in kwargs:
            kwargs["pool_size"] = cfg.database_pool_size
        if "max_overflow" not in kwargs:
            kwargs["max_overflow"] = cfg.database_max_overflow

    # Ensure passwords in URL are redacted when logging
    try:
        from sqlalchemy.engine.url import make_url
        safe_url = make_url(url).render_as_string(hide_password=True)
    except Exception:
        safe_url = "[REDACTED_DB_URL]"

    logger.info("Initializing database engine for: %s (echo=%s)", safe_url, is_echo)
    return create_engine(url, echo=is_echo, **kwargs)


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Return a configured sessionmaker bound to the given engine."""
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@contextmanager
def get_session(session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    """Provide a transactional session scope with automatic commit and rollback."""
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception as err:
        session.rollback()
        logger.error("Session transaction rolled back due to error: %s", err)
        raise
    finally:
        session.close()


def init_db(engine: Engine) -> None:
    """Create all tables defined in Base metadata if they do not already exist."""
    logger.info("Creating database tables if not present...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables initialized successfully.")


def drop_db(engine: Engine) -> None:
    """Drop all tables defined in Base metadata (used strictly in test environments)."""
    logger.warning("Dropping all database tables...")
    Base.metadata.drop_all(bind=engine)
    logger.info("Database tables dropped.")
