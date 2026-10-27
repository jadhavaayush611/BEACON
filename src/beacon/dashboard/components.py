"""Reusable UI design system and components for the BEACON Streamlit dashboard.

Provides consistent styling, status badges, empty states, and geographic helpers.
"""

from typing import Optional, Sequence
import pandas as pd
import streamlit as st

from beacon.models import UnifiedDisasterEvent

# ==============================================================================
# 1. Custom CSS Design System
# ==============================================================================

DASHBOARD_CSS = """
<style>
/* BEACON Design System */
.beacon-header {
    margin-bottom: 1.5rem;
}

.beacon-badge {
    display: inline-block;
    padding: 0.2rem 0.55rem;
    font-size: 0.75rem;
    font-weight: 600;
    border-radius: 9999px;
    line-height: 1;
    text-transform: uppercase;
    letter-spacing: 0.025em;
    vertical-align: middle;
}

.badge-critical { background-color: #fee2e2; color: #991b1b; border: 1px solid #fecaca; }
.badge-severe { background-color: #ffedd5; color: #9a3412; border: 1px solid #fed7aa; }
.badge-moderate { background-color: #fef3c7; color: #92400e; border: 1px solid #fde68a; }
.badge-minor { background-color: #e0f2fe; color: #075985; border: 1px solid #bae6fd; }
.badge-unspecified { background-color: #f1f5f9; color: #475569; border: 1px solid #e2e8f0; }

.badge-multi-source { background-color: #dcfce7; color: #166534; border: 1px solid #bbf7d0; }
.badge-single-source { background-color: #f3f4f6; color: #374151; border: 1px solid #e5e7eb; }
.badge-conflict { background-color: #fef2f2; color: #b91c1c; border: 1px solid #fca5a5; }

.beacon-card {
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 1.25rem;
    background-color: #ffffff;
    margin-bottom: 1rem;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
}

.beacon-metric-subtext {
    font-size: 0.8rem;
    color: #64748b;
    margin-top: 0.25rem;
}

.beacon-empty-box {
    border: 1px dashed #cbd5e1;
    border-radius: 8px;
    padding: 2rem 1.5rem;
    text-align: center;
    background-color: #f8fafc;
    margin: 1rem 0;
}
</style>
"""


def inject_custom_styles() -> None:
    """Inject custom CSS design system into Streamlit document head."""
    st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)


# ==============================================================================
# 2. Status & Severity Badges
# ==============================================================================

def get_severity_badge_html(severity: Optional[str]) -> str:
    """Generate HTML badge markup for a given disaster severity level."""
    sev = (severity or "unspecified").lower()
    css_class = f"badge-{sev}" if sev in ["critical", "severe", "moderate", "minor"] else "badge-unspecified"
    return f'<span class="beacon-badge {css_class}">{sev.upper()}</span>'


def get_corroboration_badge_html(source_count: int) -> str:
    """Generate HTML badge for single-source vs multi-source corroboration status."""
    if source_count > 1:
        return f'<span class="beacon-badge badge-multi-source">MULTI-SOURCE ({source_count})</span>'
    return '<span class="beacon-badge badge-single-source">SINGLE-SOURCE</span>'


# ==============================================================================
# 3. Geographic Context Helpers
# ==============================================================================

def extract_geographic_dataframe(events: Sequence[UnifiedDisasterEvent]) -> pd.DataFrame:
    """Extract coordinate points and metadata from events for st.map rendering."""
    points = []
    for ev in events:
        centroid = (ev.metadata or {}).get("centroid")
        if centroid and centroid.get("latitude") is not None and centroid.get("longitude") is not None:
            points.append({
                "latitude": float(centroid["latitude"]),
                "longitude": float(centroid["longitude"]),
                "event_id": ev.event_id,
                "event_type": ev.event_type.title(),
                "location": ev.location,
                "severity": (ev.severity or "unspecified").title(),
                "sources_count": len(ev.source_ids),
            })

    if not points:
        return pd.DataFrame(columns=["latitude", "longitude", "event_id", "event_type", "location", "severity", "sources_count"])

    return pd.DataFrame(points)


# ==============================================================================
# 4. Empty State Renderer
# ==============================================================================

def render_empty_state(
    title: str,
    message: str,
    icon: str = "🔍",
    guidance: Optional[str] = None,
) -> None:
    """Render a clean, informative empty-state container with actionable guidance."""
    st.markdown(
        f"""
        <div class="beacon-empty-box">
            <div style="font-size: 2rem; margin-bottom: 0.5rem;">{icon}</div>
            <div style="font-weight: 600; font-size: 1.1rem; color: #1e293b; margin-bottom: 0.25rem;">{title}</div>
            <div style="color: #64748b; font-size: 0.9rem; max-width: 500px; margin: 0 auto;">{message}</div>
            {f'<div style="color: #3b82f6; font-size: 0.85rem; margin-top: 0.75rem; font-weight: 500;">{guidance}</div>' if guidance else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )
