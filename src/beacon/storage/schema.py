"""SQLAlchemy ORM schemas for BEACON persistence layer."""

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    """Base declarative class for BEACON persistence models."""
    pass


class SourceModel(Base):
    """Stores source document and feed provenance records."""

    __tablename__ = "sources"

    source_id = Column(String(64), primary_key=True)
    source_file = Column(String(1024), nullable=False)
    source_type = Column(String(32), nullable=False)
    extraction_method = Column(String(32), nullable=False)
    ingested_at = Column(DateTime(timezone=True), nullable=False)
    extraction_status = Column(String(32), nullable=False)
    content_hash = Column(String(64), nullable=True, index=True)
    raw_content = Column(Text, nullable=True)
    metadata_json = Column("metadata", JSON, nullable=False, default=dict)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    observations = relationship(
        "ObservationModel",
        back_populates="source",
        cascade="all, delete-orphan",
    )
    quality_results = relationship(
        "QualityResultModel",
        back_populates="source",
        cascade="all, delete-orphan",
    )


class ObservationModel(Base):
    """Stores extracted canonical disaster observation records."""

    __tablename__ = "observations"

    observation_id = Column(String(64), primary_key=True)
    source_id = Column(
        String(64),
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type = Column(String(128), nullable=False, index=True)
    location = Column(Text, nullable=True)
    event_time = Column(DateTime(timezone=True), nullable=True, index=True)
    description = Column(Text, nullable=True)
    severity = Column(String(32), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    extraction_confidence = Column(Float, nullable=True)
    page_number = Column(Integer, nullable=True)
    source_reference = Column(String(512), nullable=True)
    raw_claim = Column(Text, nullable=True)
    validation_warnings = Column(JSON, nullable=False, default=list)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    source = relationship("SourceModel", back_populates="observations")
    quality_results = relationship(
        "QualityResultModel",
        back_populates="observation",
        foreign_keys="QualityResultModel.record_id",
    )


class QualityResultModel(Base):
    """Stores audit outcomes from data quality evaluations."""

    __tablename__ = "data_quality_results"

    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    record_id = Column(
        String(64),
        ForeignKey("observations.observation_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_id = Column(
        String(64),
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    rule_name = Column(String(128), nullable=False, index=True)
    field_name = Column(String(128), nullable=True)
    severity = Column(String(32), nullable=False)
    message = Column(Text, nullable=False)
    passed = Column(Boolean, nullable=False)
    details = Column(JSON, nullable=False, default=dict)
    checked_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    source = relationship("SourceModel", back_populates="quality_results")
    observation = relationship(
        "ObservationModel",
        back_populates="quality_results",
        foreign_keys=[record_id],
    )
