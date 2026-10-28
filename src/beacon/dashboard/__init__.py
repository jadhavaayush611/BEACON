"""BEACON Streamlit Dashboard package."""

from beacon.dashboard import formatters
from beacon.dashboard.components import (
    extract_geographic_dataframe,
    get_corroboration_badge_html,
    get_severity_badge_html,
    inject_custom_styles,
    get_system_status_pill_html,
    render_empty_state,
)
from beacon.dashboard.formatters import (
    explain_compression_ratio,
    format_compression_ratio,
    format_confidence,
    format_conflict_claim_markdown,
    format_conflict_claim_row,
    format_coordinates,
    format_decision_row,
    format_observation_row,
    format_score,
    format_timestamp,
)
from beacon.dashboard.services import DashboardService

__all__ = [
    "DashboardService",
    "explain_compression_ratio",
    "extract_geographic_dataframe",
    "format_compression_ratio",
    "format_confidence",
    "format_conflict_claim_markdown",
    "format_conflict_claim_row",
    "format_coordinates",
    "format_decision_row",
    "format_observation_row",
    "format_score",
    "format_timestamp",
    "formatters",
    "get_corroboration_badge_html",
    "get_severity_badge_html",
    "get_system_status_pill_html",
    "inject_custom_styles",
    "render_empty_state",
]
