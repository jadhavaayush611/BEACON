"""Unit tests for EventProfileAggregator profile synthesis and conflict tracking."""

from datetime import datetime, timezone
import pytest

from beacon.models import DisasterObservation
from beacon.resolution.aggregator import EventProfileAggregator


@pytest.fixture
def aggregator() -> EventProfileAggregator:
    return EventProfileAggregator()


@pytest.fixture
def cluster_observations() -> list[DisasterObservation]:
    return [
        DisasterObservation(
            observation_id="obs_json_01",
            source_id="src_json",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
            severity="severe",
            extraction_confidence=0.95,
        ),
        DisasterObservation(
            observation_id="obs_csv_02",
            source_id="src_csv",
            event_type="flood",
            location="Riverside Metro District",
            event_time=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=34.0530,
            longitude=-118.2440,
            severity="moderate",  # Contradicts JSON severe
            extraction_confidence=0.88,
        ),
        DisasterObservation(
            observation_id="obs_txt_03",
            source_id="src_txt",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 14, 0, 0, tzinfo=timezone.utc),
            latitude=34.0520,
            longitude=-118.2435,
            severity="severe",
            extraction_confidence=0.90,
        ),
    ]


class TestEventProfileAggregator:
    """Tests for EventProfileAggregator synthesis and conflict handling."""

    def test_synthesize_temporal_extent(
        self, aggregator: EventProfileAggregator, cluster_observations: list[DisasterObservation]
    ) -> None:
        event = aggregator.synthesize(cluster_observations)

        # Earliest start_time: 10:00 UTC, latest end_time: 14:00 UTC
        assert event.start_time == datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert event.end_time == datetime(2026, 5, 15, 14, 0, 0, tzinfo=timezone.utc)

    def test_synthesize_centroid_and_bbox(
        self, aggregator: EventProfileAggregator, cluster_observations: list[DisasterObservation]
    ) -> None:
        event = aggregator.synthesize(cluster_observations)

        metadata = event.metadata
        assert "centroid" in metadata
        centroid = metadata["centroid"]
        assert 34.0520 <= centroid["latitude"] <= 34.0530
        assert -118.2440 <= centroid["longitude"] <= -118.2435

        assert "bounding_box" in metadata
        bbox = metadata["bounding_box"]
        assert bbox["min_latitude"] == 34.0520
        assert bbox["max_latitude"] == 34.0530

    def test_synthesize_severity_resolution_and_conflict_tracking(
        self, aggregator: EventProfileAggregator, cluster_observations: list[DisasterObservation]
    ) -> None:
        event = aggregator.synthesize(cluster_observations)

        # Severe is preferred (highest rank & plurality)
        assert event.severity == "severe"

        # The divergent CSV moderate claim is preserved in conflicting_claims
        severity_conflicts = [
            c for c in event.conflicting_claims if c.field_name == "severity"
        ]
        assert len(severity_conflicts) == 1
        assert severity_conflicts[0].observation_id == "obs_csv_02"
        assert severity_conflicts[0].claimed_value == "moderate"
        assert severity_conflicts[0].resolution_status == "superseded"

    def test_synthesize_location_resolution_and_conflict_tracking(
        self, aggregator: EventProfileAggregator, cluster_observations: list[DisasterObservation]
    ) -> None:
        event = aggregator.synthesize(cluster_observations)

        # 'Riverside Metro' has 2 votes vs 1 for 'Riverside Metro District'
        assert event.location == "Riverside Metro"

        loc_conflicts = [
            c for c in event.conflicting_claims if c.field_name == "location"
        ]
        assert len(loc_conflicts) == 1
        assert loc_conflicts[0].observation_id == "obs_csv_02"
        assert loc_conflicts[0].claimed_value == "Riverside Metro District"

    def test_synthesize_provenance_and_observation_links(
        self, aggregator: EventProfileAggregator, cluster_observations: list[DisasterObservation]
    ) -> None:
        event = aggregator.synthesize(cluster_observations)

        assert len(event.observation_ids) == 3
        assert set(event.source_ids) == {"src_json", "src_csv", "src_txt"}
        assert "3 observation(s)" in event.summary
        assert "3 distinct source(s)" in event.summary

    def test_idempotent_determinism(
        self, aggregator: EventProfileAggregator, cluster_observations: list[DisasterObservation]
    ) -> None:
        # Shuffled order of observations
        shuffled = [cluster_observations[2], cluster_observations[0], cluster_observations[1]]

        event1 = aggregator.synthesize(cluster_observations)
        event2 = aggregator.synthesize(shuffled)

        assert event1.event_id == event2.event_id
        assert event1.event_type == event2.event_type
        assert event1.location == event2.location
        assert event1.start_time == event2.start_time
        assert event1.end_time == event2.end_time
        assert event1.severity == event2.severity
        assert event1.observation_ids == event2.observation_ids
        assert event1.source_ids == event2.source_ids
        assert len(event1.conflicting_claims) == len(event2.conflicting_claims)

    def test_empty_observations_raises(self, aggregator: EventProfileAggregator) -> None:
        with pytest.raises(ValueError, match="empty"):
            aggregator.synthesize([])
