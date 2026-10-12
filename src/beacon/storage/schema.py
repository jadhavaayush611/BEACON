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
    UniqueConstraint,
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


class EventModel(Base):
    """Stores consolidated unified disaster event profiles."""

    __tablename__ = "events"

    event_id = Column(String(64), primary_key=True)
    event_type = Column(String(128), nullable=False, index=True)
    location = Column(Text, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=True)
    summary = Column(Text, nullable=False)
    severity = Column(String(32), nullable=True)
    resolution_rationale = Column(Text, nullable=True)
    conflicting_claims = Column(JSON, nullable=False, default=list)
    source_ids = Column(JSON, nullable=False, default=list)
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

    event_observations = relationship(
        "EventObservationModel",
        back_populates="event",
        cascade="all, delete-orphan",
    )
    decisions = relationship(
        "ResolutionDecisionModel",
        back_populates="event",
    )


class EventObservationModel(Base):
    """Junction table linking unified events to their constituent observations."""

    __tablename__ = "event_observations"
    __table_args__ = (
        UniqueConstraint("event_id", "observation_id", name="uq_event_observation"),
    )

    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    event_id = Column(
        String(64),
        ForeignKey("events.event_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    observation_id = Column(
        String(64),
        ForeignKey("observations.observation_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    confidence_score = Column(Float, nullable=True)
    assigned_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    event = relationship("EventModel", back_populates="event_observations")
    observation = relationship("ObservationModel")


class ResolutionDecisionModel(Base):
    """Stores explainable audit records of observation-to-event matching decisions."""

    __tablename__ = "resolution_decisions"

    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    observation_id = Column(
        String(64),
        ForeignKey("observations.observation_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    event_id = Column(
        String(64),
        ForeignKey("events.event_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    decision_type = Column(String(32), nullable=False, index=True)
    composite_score = Column(Float, nullable=False)
    feature_scores = Column(JSON, nullable=False, default=dict)
    rationale = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    observation = relationship("ObservationModel")
    event = relationship("EventModel", back_populates="decisions")


class MergeCandidateModel(Base):
    """Stores potential multi-event bridge / merge candidates for manual audit."""

    __tablename__ = "merge_candidates"

    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    primary_event_id = Column(
        String(64),
        ForeignKey("events.event_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    secondary_event_id = Column(
        String(64),
        ForeignKey("events.event_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    bridging_observation_id = Column(
        String(64),
        ForeignKey("observations.observation_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    similarity_score = Column(Float, nullable=False)
    rationale = Column(Text, nullable=False)
    status = Column(String(32), nullable=False, default="pending_review", index=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
