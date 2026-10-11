"""Data Quality Validator engine evaluating observations against explicit rules."""

import logging
from typing import Sequence

from beacon.models import DisasterObservation
from beacon.quality.models import (
    QualityReport,
    RuleSeverity,
    ValidationOutcome,
)
from beacon.quality.rules import (
    check_batch_duplicates,
    check_geographic_coordinates,
    check_numeric_measurements,
    check_required_fields,
    check_temporal_bounds,
    check_vocabulary_standards,
)

logger = logging.getLogger("beacon.quality.validator")


class DataQualityValidator:
    """Evaluates DisasterObservation records against explicit data-quality rules."""

    def __init__(self) -> None:
        pass

    def validate(self, observations: Sequence[DisasterObservation]) -> QualityReport:
        """Execute all data-quality rules across an observation batch.

        Returns a QualityReport partitioning records into accepted and rejected,
        with granular, machine-readable outcomes for every evaluated check.
        """
        outcomes: list[ValidationOutcome] = []
        rejected_ids: set[str] = set()
        obs_list = list(observations)

        logger.info("Executing Data Quality evaluation on %d observation records", len(obs_list))

        # 1. Intra-batch duplicate checks
        duplicate_outcomes = check_batch_duplicates(obs_list)
        outcomes.extend(duplicate_outcomes)
        for outcome in duplicate_outcomes:
            if not outcome.passed and outcome.severity == RuleSeverity.ERROR:
                rejected_ids.add(outcome.record_id)

        # 2. Record-level quality checks
        for obs in obs_list:
            record_outcomes: list[ValidationOutcome] = []

            # A. Required fields
            record_outcomes.extend(check_required_fields(obs))
            # B. Coordinates
            record_outcomes.extend(check_geographic_coordinates(obs))
            # C. Temporal bounds
            record_outcomes.extend(check_temporal_bounds(obs))
            # D. Numeric measurements
            record_outcomes.extend(check_numeric_measurements(obs))
            # E. Vocabularies
            record_outcomes.extend(check_vocabulary_standards(obs))

            # Check if any error-level failures occurred for this record
            has_fatal = any(
                not o.passed and o.severity == RuleSeverity.ERROR
                for o in record_outcomes
            )
            if has_fatal:
                rejected_ids.add(obs.observation_id)

            outcomes.extend(record_outcomes)

        # 3. Partition into accepted and rejected
        accepted_records = [o for o in obs_list if o.observation_id not in rejected_ids]
        warning_count = sum(1 for o in outcomes if not o.passed and o.severity == RuleSeverity.WARNING)

        logger.info(
            "Data Quality validation complete: %d accepted, %d rejected, %d warnings",
            len(accepted_records),
            len(rejected_ids),
            warning_count,
        )

        return QualityReport(
            total_checked=len(obs_list),
            passed_count=len(accepted_records),
            failed_count=len(rejected_ids),
            warning_count=warning_count,
            outcomes=outcomes,
            accepted_records=accepted_records,
            rejected_record_ids=sorted(list(rejected_ids)),
        )
