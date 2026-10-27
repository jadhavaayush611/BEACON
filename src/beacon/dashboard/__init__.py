"""BEACON Streamlit Dashboard package."""

from beacon.dashboard.components import (
    extract_geographic_dataframe,
    get_corroboration_badge_html,
    get_severity_badge_html,
    inject_custom_styles,
    render_empty_state,
)
from beacon.dashboard.services import DashboardService

__all__ = [
    "DashboardService",
    "extract_geographic_dataframe",
    "get_corroboration_badge_html",
    "get_severity_badge_html",
    "inject_custom_styles",
    "render_empty_state",
]
