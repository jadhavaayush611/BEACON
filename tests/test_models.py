"""Unit tests for BEACON canonical Pydantic data contracts and validation rules."""

from datetime import datetime, timedelta, timezone
import pytest
from pydantic import ValidationError

from beacon.models import (
    ConflictingClaim,
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SeverityLevel,
    SourceRecord,
    SourceType,
    UnifiedDisasterEvent,
    ensure_utc_aware,
    generate_event_id,
    generate_observation_id,
    generate_source_id,
)


class TestSourceRecordContract:
    """Tests for the SourceRecord Pydantic data contract."""

    def test_valid_source_record(self) -> None:
        ingested_time = datetime(2026, 10, 12, 8, 30, tzinfo=timezone.utc)
        record = SourceRecord(
            source_id="src_12345678abcdef01",
            source_file="data/sample/events.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=ingested_time,
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="a" * 64,
            raw_content='[{"sample": true}]',
            metadata={"byte_size": 1024, "encoding": "utf-8"},
        )
        assert record.source_id == "src_12345678abcdef01"
        assert record.source_type == SourceType.JSON
        assert record.ingested_at == ingested_time
        assert record.metadata["byte_size"] == 1024

    def test_source_record_serialization_roundtrip(self) -> None:
        ingested_time = datetime(2026, 10, 12, 8, 30, tzinfo=timezone.utc)
        record = SourceRecord(
            source_id="src_test_ser",
            source_file="data/sample/events.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=ingested_time,
            metadata={"row_count": 42},
        )
        # Test dict dump
        d = record.model_dump()
        assert d["source_id"] == "src_test_ser"
        assert d["source_type"] == "json"

        # Test JSON roundtrip
        json_str = record.model_dump_json()
        restored = SourceRecord.model_validate_json(json_str)
        assert restored.source_id == record.source_id
        assert restored.ingested_at == record.ingested_at

    def test_timezone_normalization_to_utc(self) -> None:
        # Non-UTC timezone with +05:30 offset
        tz_offset = timezone(timedelta(hours=5, minutes=30))
        dt_local = datetime(2026, 10, 12, 14, 0, tzinfo=tz_offset)

        record = SourceRecord(
            source_id="src_test_tz",
            source_file="test.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=dt_local,
        )
        # 14:00 +05:30 -> 08:30 UTC
        assert record.ingested_at.tzinfo == timezone.utc
        assert record.ingested_at.hour == 8
        assert record.ingested_at.minute == 30

    def test_reject_naive_datetime(self) -> None:
        naive_dt = datetime(2026, 10, 12, 8, 0, 0)
        with pytest.raises(ValidationError) as exc_info:
            SourceRecord(
                source_id="src_fail_naive",
                source_file="test.json",
                source_type=SourceType.JSON,
                extraction_method=ExtractionMethod.JSON_PARSE,
                ingested_at=naive_dt,
            )
        assert "timezone-aware" in str(exc_info.value)

    def test_reject_invalid_sha256_hash(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SourceRecord(
                source_id="src_fail_hash",
                source_file="test.json",
                source_type=SourceType.JSON,
                extraction_method=ExtractionMethod.JSON_PARSE,
                ingested_at=datetime.now(timezone.utc),
                content_hash="invalid_hash_string",
            )
        assert "64-character hexadecimal" in str(exc_info.value)

    def test_reject_sensitive_credentials_in_metadata(self) -> None:
        forbidden_payloads = [
            {"api_key": "secret-12345"},
            {"auth_token": "bearer-token-abc"},
            {"user_password": "supersecretpassword"},
            {"private_key_pem": "-----BEGIN RSA..."},
        ]
        for meta in forbidden_payloads:
            with pytest.raises(ValidationError) as exc_info:
                SourceRecord(
                    source_id="src_secret_test",
                    source_file="test.json",
                    source_type=SourceType.JSON,
                    extraction_method=ExtractionMethod.JSON_PARSE,
                    ingested_at=datetime.now(timezone.utc),
                    metadata=meta,
                )
            assert "Forbidden credential key" in str(exc_info.value)

    def test_reject_unrecognized_extra_fields(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SourceRecord(
                source_id="src_extra",
                source_file="test.json",
                source_type=SourceType.JSON,
                extraction_method=ExtractionMethod.JSON_PARSE,
                ingested_at=datetime.now(timezone.utc),
                unexpected_extra_field="malformed",  # type: ignore[call-arg]
            )
        assert "Extra inputs are not permitted" in str(exc_info.value)


class TestDisasterObservationContract:
    """Tests for the DisasterObservation Pydantic data contract."""

    def test_valid_observation_full(self) -> None:
        obs_time = datetime(2026, 10, 12, 9, 15, tzinfo=timezone.utc)
        obs = DisasterObservation(
            observation_id="obs_001",
            source_id="src_001",
            event_type="flood",
            location="Riverside Metro, Sector 4",
            event_time=obs_time,
            description="Embankment breached by rising floodwater.",
            severity=SeverityLevel.SEVERE.value,
            latitude=28.6139,
            longitude=77.2090,
            extraction_confidence=0.95,
            page_number=1,
            source_reference="row_14",
            raw_claim="Sector 4 riverbank embankment breach observed.",
        )
        assert obs.observation_id == "obs_001"
        assert obs.latitude == 28.6139
        assert obs.longitude == 77.2090
        assert obs.extraction_confidence == 0.95
        assert obs.page_number == 1
        assert len(obs.validation_warnings) == 0

    def test_observation_serialization_roundtrip(self) -> None:
        obs_time = datetime(2026, 10, 12, 9, 15, tzinfo=timezone.utc)
        obs = DisasterObservation(
            observation_id="obs_roundtrip",
            source_id="src_001",
            event_type="flood",
            location="Riverside Metro",
            event_time=obs_time,
            severity=SeverityLevel.MODERATE.value,
            latitude=28.5,
            longitude=77.2,
            extraction_confidence=0.88,
        )
        json_str = obs.model_dump_json()
        restored = DisasterObservation.model_validate_json(json_str)
        assert restored.observation_id == obs.observation_id
        assert restored.event_time == obs.event_time
        assert restored.latitude == obs.latitude
        assert restored.longitude == obs.longitude

    def test_missing_optional_fields_are_none_without_fabrication(self) -> None:
        obs = DisasterObservation(
            observation_id="obs_002",
            source_id="src_002",
            event_type="wildfire",
        )
        assert obs.location is None
        assert obs.event_time is None  # Must NOT be defaulted to datetime.now()
        assert obs.description is None
        assert obs.severity is None
        assert obs.latitude is None
        assert obs.longitude is None
        assert obs.extraction_confidence is None
        assert obs.page_number is None
        assert obs.source_reference is None
        assert obs.raw_claim is None

    @pytest.mark.parametrize(
        "lat",
        [-90.1, 90.1, -180.0, 100.0],
    )
    def test_reject_invalid_latitude(self, lat: float) -> None:
        with pytest.raises(ValidationError) as exc_info:
            DisasterObservation(
                observation_id="obs_lat_fail",
                source_id="src_001",
                event_type="flood",
                latitude=lat,
            )
        assert "Latitude must be between -90.0 and 90.0" in str(exc_info.value)

    @pytest.mark.parametrize(
        "lon",
        [-180.1, 180.1, -200.0, 360.0],
    )
    def test_reject_invalid_longitude(self, lon: float) -> None:
        with pytest.raises(ValidationError) as exc_info:
            DisasterObservation(
                observation_id="obs_lon_fail",
                source_id="src_001",
                event_type="flood",
                longitude=lon,
            )
        assert "Longitude must be between -180.0 and 180.0" in str(exc_info.value)

    @pytest.mark.parametrize(
        "conf",
        [-0.01, 1.01, 2.0, -1.0],
    )
    def test_reject_invalid_confidence(self, conf: float) -> None:
        with pytest.raises(ValidationError) as exc_info:
            DisasterObservation(
                observation_id="obs_conf_fail",
                source_id="src_001",
                event_type="flood",
                extraction_confidence=conf,
            )
        assert "extraction_confidence must be between 0.0 and 1.0" in str(exc_info.value)

    @pytest.mark.parametrize(
        "page",
        [0, -1, -5],
    )
    def test_reject_non_positive_page_numbers(self, page: int) -> None:
        with pytest.raises(ValidationError) as exc_info:
            DisasterObservation(
                observation_id="obs_page_fail",
                source_id="src_001",
                event_type="flood",
                page_number=page,
            )
        assert "page_number must be a positive integer" in str(exc_info.value)

    def test_blank_description_converts_to_none_with_warning(self) -> None:
        for blank_val in ["", "   ", "\t\n  "]:
            obs = DisasterObservation(
                observation_id="obs_blank_desc",
                source_id="src_001",
                event_type="flood",
                description=blank_val,
            )
            assert obs.description is None
            assert any("Blank or whitespace-only description" in w for w in obs.validation_warnings)

    def test_extensible_unknown_category_accepted_with_warning(self) -> None:
        obs = DisasterObservation(
            observation_id="obs_custom_cat",
            source_id="src_001",
            event_type="geomagnetic_storm",  # Non-standard category
        )
        assert obs.event_type == "geomagnetic_storm"
        assert any("Non-standard event category 'geomagnetic_storm'" in w for w in obs.validation_warnings)

    def test_empty_event_type_raises_error(self) -> None:
        for empty_val in ["", "   "]:
            with pytest.raises(ValidationError) as exc_info:
                DisasterObservation(
                    observation_id="obs_empty_cat",
                    source_id="src_001",
                    event_type=empty_val,
                )
            assert "event_type must be a non-empty string" in str(exc_info.value)


class TestUnifiedDisasterEventContract:
    """Tests for the UnifiedDisasterEvent data contract."""

    def test_valid_unified_event(self) -> None:
        start = datetime(2026, 10, 12, 8, 0, tzinfo=timezone.utc)
        end = datetime(2026, 10, 12, 18, 0, tzinfo=timezone.utc)
        created = datetime(2026, 10, 12, 19, 0, tzinfo=timezone.utc)

        evt = UnifiedDisasterEvent(
            event_id="evt_flood_sector4",
            event_type="flood",
            location="Riverside Metro, Sector 4",
            start_time=start,
            end_time=end,
            summary="Synthesized flood profile across telemetry, CSV, and text feeds.",
            severity=SeverityLevel.SEVERE.value,
            observation_ids=["obs_001", "obs_002"],
            created_at=created,
            updated_at=created,
            conflicting_claims=[
                ConflictingClaim(
                    field_name="severity",
                    observation_id="obs_002",
                    source_id="src_csv_01",
                    claimed_value="moderate",
                    confidence=0.85,
                    resolution_status="superseded",
                    notes="Overruled by higher confidence sensor gauge (severe).",
                )
            ],
            resolution_rationale="Accepted highest confidence sensor reading (0.95 vs 0.85).",
            source_ids=["src_json_01", "src_csv_01"],
        )
        assert evt.event_id == "evt_flood_sector4"
        assert len(evt.observation_ids) == 2
        assert len(evt.conflicting_claims) == 1
        assert evt.conflicting_claims[0].claimed_value == "moderate"

        # Roundtrip JSON test
        json_str = evt.model_dump_json()
        restored = UnifiedDisasterEvent.model_validate_json(json_str)
        assert restored.event_id == evt.event_id
        assert len(restored.conflicting_claims) == 1
        assert restored.conflicting_claims[0].resolution_status == "superseded"

    def test_reject_empty_observation_ids(self) -> None:
        start = datetime(2026, 10, 12, 8, 0, tzinfo=timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            UnifiedDisasterEvent(
                event_id="evt_no_obs",
                event_type="flood",
                location="Test Location",
                start_time=start,
                summary="Test summary",
                observation_ids=[],  # Must have at least 1 observation
                created_at=start,
                updated_at=start,
            )
        assert "List should have at least 1 item" in str(exc_info.value)

    def test_reject_end_time_before_start_time(self) -> None:
        start = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
        earlier_end = datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc)
        with pytest.raises(ValidationError) as exc_info:
            UnifiedDisasterEvent(
                event_id="evt_invalid_times",
                event_type="flood",
                location="Test Location",
                start_time=start,
                end_time=earlier_end,
                summary="Test summary",
                observation_ids=["obs_001"],
                created_at=start,
                updated_at=start,
            )
        assert "cannot be earlier than start_time" in str(exc_info.value)


class TestDeterministicIdentifierGenerators:
    """Tests for rerun-stable identifier generation."""

    def test_source_id_deterministic(self) -> None:
        id1 = generate_source_id(SourceType.JSON, "data/sample/events.json")
        id2 = generate_source_id("json", "data/sample/events.json")
        id3 = generate_source_id(SourceType.CSV, "data/sample/events.csv")

        assert id1 == id2
        assert id1.startswith("src_")
        assert id1 != id3

    def test_observation_id_deterministic(self) -> None:
        id1 = generate_observation_id("src_123", source_reference="row_1", raw_claim="Flood breach")
        id2 = generate_observation_id("src_123", source_reference="row_1", raw_claim="Flood breach")
        id3 = generate_observation_id("src_123", source_reference="row_2", raw_claim="Flood breach")

        assert id1 == id2
        assert id1.startswith("obs_")
        assert id1 != id3

    def test_event_id_deterministic(self) -> None:
        start_time = datetime(2026, 10, 12, 8, 0, tzinfo=timezone.utc)
        id1 = generate_event_id("flood", "Riverside Metro, Sector 4", start_time)
        id2 = generate_event_id("FLOOD", "riverside metro, sector 4", start_time)
        assert id1 == id2
        assert id1.startswith("evt_")

    def test_ensure_utc_aware_helper(self) -> None:
        assert ensure_utc_aware(None) is None
        with pytest.raises(ValueError):
            ensure_utc_aware(datetime(2026, 1, 1, 0, 0, 0))
