"""Unit tests for BEACON transformation and normalization layer."""

from datetime import datetime, timezone
import pytest

from beacon.models import DisasterObservation
from beacon.pipeline.transform import ObservationTransformer


def _make_observation(
    observation_id: str = "obs_001",
    source_id: str = "src_001",
    event_type: str = "flash flood",
    location: str | None = "  Downtown Riverbank,  ",
    event_time: datetime | None = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
    description: str | None = "  Heavy downpour  caused flash   flooding.  ",
    severity: str | None = "  MODERATE ",
    latitude: float | None = 18.5204301234,
    longitude: float | None = 73.8567439876,
    extraction_confidence: float | None = 0.90,
    page_number: int | None = 1,
) -> DisasterObservation:
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


class TestObservationTransformer:
    """Tests for individual and batch transformations."""

    @pytest.fixture
    def transformer(self) -> ObservationTransformer:
        return ObservationTransformer()

    def test_normalize_location(self, transformer: ObservationTransformer) -> None:
        assert transformer.normalize_location("  Downtown Sector 5,  ") == "Downtown Sector 5"
        assert transformer.normalize_location("North   Ridge -") == "North Ridge"
        assert transformer.normalize_location("   ") is None
        assert transformer.normalize_location(None) is None

    def test_normalize_event_type_aliases(self, transformer: ObservationTransformer) -> None:
        assert transformer.normalize_event_type("FLASH FLOOD") == "flood"
        assert transformer.normalize_event_type("brushfire") == "wildfire"
        assert transformer.normalize_event_type("forest fire") == "wildfire"
        assert transformer.normalize_event_type("cyclonic storm") == "cyclone"
        assert transformer.normalize_event_type("tremor") == "earthquake"
        assert transformer.normalize_event_type("rockfall") == "landslide"
        assert transformer.normalize_event_type("mudslide") == "landslide"
        # Standard preserved
        assert transformer.normalize_event_type("Wildfire") == "wildfire"
        # Unknown preserved
        assert transformer.normalize_event_type("Solar Storm") == "solar storm"

    def test_normalize_severity(self, transformer: ObservationTransformer) -> None:
        assert transformer.normalize_severity("  CRITICAL  ") == "critical"
        assert transformer.normalize_severity("Severe") == "severe"
        assert transformer.normalize_severity("unknown") is None
        assert transformer.normalize_severity("N/A") is None
        assert transformer.normalize_severity("none") is None
        assert transformer.normalize_severity("") is None
        assert transformer.normalize_severity(None) is None

    def test_normalize_coordinate_precision(self, transformer: ObservationTransformer) -> None:
        assert transformer.normalize_coordinate(18.5204301234) == 18.52043
        assert transformer.normalize_coordinate(73.8567439876) == 73.856744
        assert transformer.normalize_coordinate(0.0) == 0.0
        assert transformer.normalize_coordinate(None) is None

    def test_normalize_description(self, transformer: ObservationTransformer) -> None:
        raw = "Heavy   rain \n  caused flash   flooding\tin   sector."
        expected = "Heavy rain caused flash flooding in sector."
        assert transformer.normalize_description(raw) == expected
        assert transformer.normalize_description("   ") is None
        assert transformer.normalize_description(None) is None

    def test_transform_record_preserves_provenance_and_identifiers(
        self, transformer: ObservationTransformer
    ) -> None:
        obs = _make_observation()
        transformed = transformer.transform(obs)

        # Identifiers must remain unchanged
        assert transformed.observation_id == obs.observation_id
        assert transformed.source_id == obs.source_id

        # Normalized values
        assert transformed.event_type == "flood"
        assert transformed.location == "Downtown Riverbank"
        assert transformed.severity == "moderate"
        assert transformed.latitude == 18.52043
        assert transformed.longitude == 73.856744
        assert transformed.description == "Heavy downpour caused flash flooding."

        # Pass-through fields unchanged
        assert transformed.event_time == obs.event_time
        assert transformed.extraction_confidence == obs.extraction_confidence
        assert transformed.page_number == obs.page_number

    def test_transform_non_fabrication_guarantee(self, transformer: ObservationTransformer) -> None:
        """Verify that when optional attributes are absent in raw data, they remain None."""
        sparse_obs = DisasterObservation(
            observation_id="obs_sparse",
            source_id="src_sparse",
            event_type="earthquake",
            location=None,
            event_time=None,
            description=None,
            severity=None,
            latitude=None,
            longitude=None,
            extraction_confidence=None,
            page_number=None,
        )

        transformed = transformer.transform(sparse_obs)

        assert transformed.location is None
        assert transformed.event_time is None
        assert transformed.description is None
        assert transformed.severity is None
        assert transformed.latitude is None
        assert transformed.longitude is None
        assert transformed.extraction_confidence is None
        assert transformed.page_number is None

    def test_transform_batch(self, transformer: ObservationTransformer) -> None:
        obs1 = _make_observation(observation_id="obs_1", event_type="tremor")
        obs2 = _make_observation(observation_id="obs_2", event_type="bushfire")

        batch = transformer.transform_batch([obs1, obs2])
        assert len(batch) == 2
        assert batch[0].event_type == "earthquake"
        assert batch[1].event_type == "wildfire"
