"""Unit tests for BEACON Data Quality framework, models, rules, and validator."""

from datetime import datetime, timezone

from beacon.models import DisasterObservation
from beacon.quality import (
    DataQualityValidator,
    QualityReport,
    RuleSeverity,
    ValidationOutcome,
    check_batch_duplicates,
    check_geographic_coordinates,
    check_numeric_measurements,
    check_required_fields,
    check_temporal_bounds,
    check_vocabulary_standards,
)


def _make_observation(
    observation_id: str = "obs_123456789abc",
    source_id: str = "src_abcdef123456",
    event_type: str = "flood",
    location: str | None = "River Valley",
    event_time: datetime | None = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc),
    description: str | None = "Water level rising",
    severity: str | None = "moderate",
    latitude: float | None = 34.0522,
    longitude: float | None = -118.2437,
    extraction_confidence: float | None = 0.95,
    page_number: int | None = 1,
) -> DisasterObservation:
    """Helper creating a baseline valid DisasterObservation."""
    return DisasterObservation(
        observation_id=observation_id,
        source_id=source_id,
        event_type=event_type,
        location=location,
        event_time=event_time,
        description=description,
        severity=severity,
        latitude=latitude,
        longitude=longitude,
        extraction_confidence=extraction_confidence,
        page_number=page_number,
    )


class TestDataQualityModels:
    """Tests for Data Quality enum and model representations."""

    def test_rule_severity_values(self) -> None:
        assert RuleSeverity.ERROR.value == "error"
        assert RuleSeverity.WARNING.value == "warning"
        assert RuleSeverity.INFO.value == "info"

    def test_validation_outcome_creation(self) -> None:
        outcome = ValidationOutcome(
            record_id="obs_001",
            source_id="src_001",
            rule_name="TEST_RULE",
            field_name="event_type",
            severity=RuleSeverity.WARNING,
            message="Test warning message",
            passed=False,
            details={"key": "val"},
        )
        assert outcome.record_id == "obs_001"
        assert outcome.severity == RuleSeverity.WARNING
        assert not outcome.passed
        assert outcome.details == {"key": "val"}

    def test_quality_report_methods(self) -> None:
        o1 = ValidationOutcome(
            record_id="obs_good",
            source_id="src_001",
            rule_name="R1",
            severity=RuleSeverity.INFO,
            message="all good",
            passed=True,
        )
        o2 = ValidationOutcome(
            record_id="obs_bad",
            source_id="src_001",
            rule_name="R2",
            severity=RuleSeverity.ERROR,
            message="fatal breach",
            passed=False,
        )
        report = QualityReport(
            total_checked=2,
            passed_count=1,
            failed_count=1,
            warning_count=0,
            outcomes=[o1, o2],
            accepted_records=[_make_observation(observation_id="obs_good")],
            rejected_record_ids=["obs_bad"],
        )

        assert not report.has_errors_for("obs_good")
        assert report.has_errors_for("obs_bad")
        assert len(report.get_outcomes_for("obs_good")) == 1
        assert len(report.get_outcomes_for("obs_bad")) == 1


class TestIndividualQualityRules:
    """Tests for individual data-quality evaluation rules."""

    def test_check_required_fields_valid(self) -> None:
        obs = _make_observation()
        outcomes = check_required_fields(obs)
        assert len(outcomes) == 0

    def test_check_geographic_coordinates_paired_and_valid(self) -> None:
        obs = _make_observation(latitude=12.5, longitude=77.5)
        outcomes = check_geographic_coordinates(obs)
        assert len(outcomes) == 0

    def test_check_geographic_coordinates_both_none(self) -> None:
        obs = _make_observation(latitude=None, longitude=None)
        outcomes = check_geographic_coordinates(obs)
        assert len(outcomes) == 0

    def test_check_geographic_coordinates_unpaired(self) -> None:
        obs_lat_only = _make_observation(latitude=12.5, longitude=None)
        outcomes1 = check_geographic_coordinates(obs_lat_only)
        assert len(outcomes1) == 1
        assert outcomes1[0].rule_name == "COORDINATES_PAIRED"
        assert outcomes1[0].severity == RuleSeverity.WARNING

        obs_lon_only = _make_observation(latitude=None, longitude=77.5)
        outcomes2 = check_geographic_coordinates(obs_lon_only)
        assert len(outcomes2) == 1
        assert outcomes2[0].rule_name == "COORDINATES_PAIRED"

    def test_check_temporal_bounds_none_timestamp(self) -> None:
        obs = _make_observation(event_time=None)
        outcomes = check_temporal_bounds(obs)
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "TIMESTAMP_MISSING"
        assert outcomes[0].severity == RuleSeverity.INFO
        assert outcomes[0].passed is True

    def test_check_temporal_bounds_valid_utc(self) -> None:
        obs = _make_observation(event_time=datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc))
        outcomes = check_temporal_bounds(obs)
        assert len(outcomes) == 0

    def test_check_temporal_bounds_implausible_year(self) -> None:
        # Construct an observation and manually bypass pydantic validation to test DQ rule defense
        obs = _make_observation()
        object.__setattr__(obs, "event_time", datetime(1850, 1, 1, tzinfo=timezone.utc))
        outcomes = check_temporal_bounds(obs)
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "TIMESTAMP_PLAUSIBLE_RANGE"
        assert outcomes[0].severity == RuleSeverity.ERROR

    def test_check_numeric_measurements_valid(self) -> None:
        obs = _make_observation(extraction_confidence=0.85, page_number=2)
        outcomes = check_numeric_measurements(obs)
        assert len(outcomes) == 0

    def test_check_numeric_measurements_low_confidence_warning(self) -> None:
        obs = _make_observation(extraction_confidence=0.15)
        outcomes = check_numeric_measurements(obs)
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "CONFIDENCE_LOW"
        assert outcomes[0].severity == RuleSeverity.WARNING

    def test_check_vocabulary_standards_standard_terms(self) -> None:
        obs = _make_observation(event_type="earthquake", severity="critical")
        outcomes = check_vocabulary_standards(obs)
        assert len(outcomes) == 0

    def test_check_vocabulary_standards_custom_event_type(self) -> None:
        obs = _make_observation(event_type="solar_flare")
        outcomes = check_vocabulary_standards(obs)
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "VOCABULARY_EVENT_CATEGORY"
        assert outcomes[0].severity == RuleSeverity.WARNING

    def test_check_vocabulary_standards_non_standard_severity(self) -> None:
        obs = _make_observation(severity="catastrophic")
        outcomes = check_vocabulary_standards(obs)
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "VOCABULARY_SEVERITY"
        assert outcomes[0].severity == RuleSeverity.WARNING

    def test_check_batch_duplicates_no_collision(self) -> None:
        obs1 = _make_observation(observation_id="obs_001", location="City A")
        obs2 = _make_observation(observation_id="obs_002", location="City B")
        outcomes = check_batch_duplicates([obs1, obs2])
        assert len(outcomes) == 0

    def test_check_batch_duplicates_duplicate_id(self) -> None:
        obs1 = _make_observation(observation_id="obs_001", location="City A")
        obs2 = _make_observation(observation_id="obs_001", location="City B")
        outcomes = check_batch_duplicates([obs1, obs2])
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "BATCH_DUPLICATE_ID"
        assert outcomes[0].severity == RuleSeverity.ERROR

    def test_check_batch_duplicates_duplicate_claim_warning(self) -> None:
        dt = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc)
        obs1 = _make_observation(observation_id="obs_001", source_id="src_1", location="Town", event_time=dt)
        obs2 = _make_observation(observation_id="obs_002", source_id="src_1", location="Town", event_time=dt)
        outcomes = check_batch_duplicates([obs1, obs2])
        assert len(outcomes) == 1
        assert outcomes[0].rule_name == "BATCH_DUPLICATE_CLAIM"
        assert outcomes[0].severity == RuleSeverity.WARNING


class TestDataQualityValidator:
    """Tests for DataQualityValidator batch partitioning and reporting."""

    def test_validate_partitions_accepted_and_rejected(self) -> None:
        validator = DataQualityValidator()
        obs_valid = _make_observation(observation_id="obs_good")
        obs_dup1 = _make_observation(observation_id="obs_colliding", location="Loc 1")
        obs_dup2 = _make_observation(observation_id="obs_colliding", location="Loc 2")

        report = validator.validate([obs_valid, obs_dup1, obs_dup2])

        assert report.total_checked == 3
        assert report.passed_count == 1
        assert report.failed_count == 1
        assert "obs_colliding" in report.rejected_record_ids
        assert len(report.accepted_records) == 1
        assert report.accepted_records[0].observation_id == "obs_good"

    def test_validate_accumulates_warnings_without_rejection(self) -> None:
        validator = DataQualityValidator()
        obs_warning = _make_observation(
            observation_id="obs_warn",
            event_type="volcano",  # Standard
            severity="moderate",   # Standard
            extraction_confidence=0.10,  # LOW confidence warning
            latitude=15.0,
            longitude=None,  # Unpaired warning
        )

        report = validator.validate([obs_warning])

        assert report.total_checked == 1
        assert report.passed_count == 1
        assert report.failed_count == 0
        assert report.warning_count >= 2
        assert len(report.accepted_records) == 1
