"""Reusable UI design system and components for the BEACON Streamlit dashboard.

Provides 8-bit Minecraft-Beacon-inspired visual styling, status badges,
animated pixel-art SVG logo, empty states, and geographic helpers.
"""

import time
from typing import Any, Optional, Sequence
import pandas as pd
import streamlit as st

from beacon.models import DisasterObservation, UnifiedDisasterEvent

# ==============================================================================
# 1. Custom CSS Design System: 8-Bit Minecraft-Beacon Command Center
# ==============================================================================

DASHBOARD_CSS = """
<style>
/* 8-Bit Minecraft-Inspired Command Center Design System */
@import url('https://fonts.googleapis.com/css2?family=Press+Start+2P&family=Silkscreen:wght@400;700&family=JetBrains+Mono:wght@400;500;700&display=swap');

:root {
    --pixel-bg: #070b14;
    --pixel-panel: #0b1120;
    --pixel-surface: #0f172a;
    --pixel-border: #1e293b;
    --pixel-border-bright: #334155;
    --pixel-cyan: #00f0ff;
    --pixel-cyan-dark: #0284c7;
    --pixel-blue: #38bdf8;
    --pixel-emerald: #10b981;
    --pixel-amber: #f59e0b;
    --pixel-red: #ef4444;
    --pixel-purple: #a855f7;
    --pixel-text: #f1f5f9;
    --pixel-text-dim: #94a3b8;
    --font-pixel-heading: 'Press Start 2P', 'Silkscreen', 'Courier New', ui-monospace, monospace;
    --font-pixel-label: 'Silkscreen', 'Press Start 2P', 'Courier New', ui-monospace, monospace;
    --font-mono: 'JetBrains Mono', 'Courier New', ui-monospace, Consolas, monospace;
}

/* Base application styling */
.stApp {
    background-color: var(--pixel-bg);
    background-image:
        radial-gradient(circle at 50% 0%, rgba(0, 240, 255, 0.08) 0%, transparent 65%),
        linear-gradient(rgba(0, 240, 255, 0.025) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0, 240, 255, 0.025) 1px, transparent 1px);
    background-size: 100% 100%, 20px 20px, 20px 20px;
    color: var(--pixel-text);
    font-family: var(--font-mono);
}

/* Sidebar Minecraft Environment & Bottom Branding Layout */
section[data-testid="stSidebar"] {
    background-color: #050811 !important;
    border-right: 2px solid var(--pixel-border) !important;
    box-shadow: 4px 0px 0px #020617 !important;
    position: relative !important;
    overflow: hidden !important;
}

section[data-testid="stSidebar"] > div {
    background-color: transparent !important;
}

section[data-testid="stSidebar"] div[data-testid="stSidebarContent"] {
    display: flex !important;
    flex-direction: column !important;
    height: 100% !important;
    position: relative !important;
    overflow: hidden !important;
}

section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"] {
    display: flex !important;
    flex-direction: column !important;
    flex: 1 1 auto !important;
    height: 100% !important;
    min-height: calc(100vh - 4.5rem) !important;
    padding-bottom: 1.5rem !important;
    position: relative !important;
    z-index: 1 !important;
}

section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"] > div,
section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"] [data-testid="stVerticalBlock"],
section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"] [data-testid="stElementContainer"] {
    display: flex !important;
    flex-direction: column !important;
    flex: 1 1 auto !important;
    height: 100% !important;
    min-height: 100% !important;
}

.beacon-sidebar-full-container {
    position: relative !important;
    width: 100% !important;
    flex: 1 1 auto !important;
    height: 100% !important;
    min-height: calc(100vh - 4.5rem) !important;
    display: flex !important;
    flex-direction: column !important;
    justify-content: flex-end !important;
    align-items: center !important;
}

/* Continuous Full-Height Beam from Top of Sidebar to Beacon Block */
.beacon-sidebar-full-beam {
    position: absolute !important;
    top: -5rem !important;
    bottom: 85px !important;
    left: 50% !important;
    transform: translateX(-50%) !important;
    width: 45px !important;
    pointer-events: none !important;
    z-index: 0 !important;
    overflow: visible !important;
}

.beacon-beam-glow {
    position: absolute !important;
    top: 0 !important;
    bottom: 0 !important;
    left: 0 !important;
    right: 0 !important;
    background: transparent !important;
}

.beacon-beam-mid {
    position: absolute !important;
    top: 0 !important;
    bottom: 0 !important;
    left: 10px !important;
    right: 10px !important;
    background: transparent !important;
}

.beacon-beam-core {
    position: absolute !important;
    top: 0 !important;
    bottom: 0 !important;
    left: 16px !important;
    right: 16px !important;
    background: transparent !important;
}

.beacon-beam-laser {
    position: absolute !important;
    top: 0 !important;
    bottom: 0 !important;
    left: 19px !important;
    right: 19px !important;
    background: transparent !important;
}

.beacon-beam-mote {
    position: absolute !important;
    width: 2px !important;
    height: 2px !important;
    background: #ffffff !important;
    box-shadow: 0 0 4px #00f0ff !important;
    opacity: 0 !important;
    pointer-events: none !important;
}

/* Full-height ascending energy particles */
.beacon-beam-mote.m1 { left: 14px !important; animation: beacon-full-mote 3.8s linear infinite !important; }
.beacon-beam-mote.m2 { left: 28px !important; width: 3px !important; height: 3px !important; animation: beacon-full-mote 4.4s linear infinite 1.3s !important; }
.beacon-beam-mote.m3 { left: 22px !important; width: 1px !important; height: 1px !important; animation: beacon-full-mote 3.2s linear infinite 2.2s !important; }
.beacon-beam-mote.m4 { left: 32px !important; animation: beacon-full-mote 4.9s linear infinite 0.7s !important; }

/* Middle-section energy particles (active through mid-column) */
.beacon-beam-mote.m5 { left: 10px !important; width: 3px !important; height: 3px !important; animation: beacon-mote-mid 3.4s ease-in-out infinite 0.5s !important; }
.beacon-beam-mote.m6 { left: 24px !important; width: 2px !important; height: 2px !important; animation: beacon-mote-mid 4.1s ease-in-out infinite 1.8s !important; }
.beacon-beam-mote.m7 { left: 18px !important; width: 1px !important; height: 1px !important; animation: beacon-mote-mid 2.9s ease-in-out infinite 1.1s !important; }
.beacon-beam-mote.m8 { left: 30px !important; width: 3px !important; height: 3px !important; animation: beacon-mote-mid 3.7s ease-in-out infinite 2.6s !important; }

/* Upper-section energy particles (active through upper-column) */
.beacon-beam-mote.m9  { left: 12px !important; width: 2px !important; height: 2px !important; animation: beacon-mote-upper 3.6s ease-in-out infinite 0.2s !important; }
.beacon-beam-mote.m10 { left: 26px !important; width: 3px !important; height: 3px !important; animation: beacon-mote-upper 4.3s ease-in-out infinite 1.5s !important; }
.beacon-beam-mote.m11 { left: 16px !important; width: 1px !important; height: 1px !important; animation: beacon-mote-upper 3.1s ease-in-out infinite 2.4s !important; }
.beacon-beam-mote.m12 { left: 34px !important; width: 2px !important; height: 2px !important; animation: beacon-mote-upper 4.8s ease-in-out infinite 0.9s !important; }

/* Micro-spark motes with cyan & blue glow accents */
.beacon-beam-mote.m13 { left: 8px !important; width: 2px !important; height: 2px !important; background: #00f0ff !important; box-shadow: 0 0 6px #00f0ff !important; animation: beacon-mote-mid 4.6s linear infinite 0.8s !important; }
.beacon-beam-mote.m14 { left: 20px !important; width: 3px !important; height: 3px !important; background: #a5f3fc !important; box-shadow: 0 0 5px #38bdf8 !important; animation: beacon-mote-upper 3.9s linear infinite 1.9s !important; }
.beacon-beam-mote.m15 { left: 27px !important; width: 2px !important; height: 2px !important; background: #38bdf8 !important; box-shadow: 0 0 5px #00f0ff !important; animation: beacon-full-mote 5.3s linear infinite 2.8s !important; }
.beacon-beam-mote.m16 { left: 36px !important; width: 1px !important; height: 1px !important; background: #ffffff !important; box-shadow: 0 0 4px #00f0ff !important; animation: beacon-mote-mid 3.3s linear infinite 1.4s !important; }

@keyframes beacon-full-mote {
    0% { bottom: 0; opacity: 0; transform: translateY(0); }
    15% { opacity: 0.95; }
    85% { opacity: 0.85; }
    100% { bottom: 100%; opacity: 0; transform: translateY(-40px); }
}

@keyframes beacon-mote-mid {
    0% { bottom: 25%; opacity: 0; transform: translateY(0) translateX(0); }
    20% { opacity: 0.95; }
    50% { transform: translateY(-30px) translateX(2px); }
    80% { opacity: 0.85; }
    100% { bottom: 75%; opacity: 0; transform: translateY(-60px) translateX(-1px); }
}

@keyframes beacon-mote-upper {
    0% { bottom: 55%; opacity: 0; transform: translateY(0) translateX(0); }
    20% { opacity: 0.95; }
    50% { transform: translateY(-25px) translateX(-2px); }
    80% { opacity: 0.85; }
    100% { bottom: 100%; opacity: 0; transform: translateY(-50px) translateX(1px); }
}

.beacon-sidebar-footer {
    margin-top: auto !important;
    display: flex !important;
    flex-direction: column !important;
    align-items: center !important;
    justify-content: center !important;
    text-align: center !important;
    width: 100% !important;
    position: relative !important;
    z-index: 1 !important;
}

.beacon-sidebar-wordmark {
    font-family: var(--font-pixel-heading);
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--pixel-cyan);
    text-shadow: 0 0 12px rgba(0, 240, 255, 0.75), 2px 2px 0px #020617;
    letter-spacing: 0.18em;
    margin-top: 0.25rem;
    margin-bottom: 0.5rem;
    text-align: center;
}

/* Header & Typography */
.beacon-header {
    margin-bottom: 0.85rem;
    padding-bottom: 0.75rem;
    border-bottom: 2px solid var(--pixel-border);
    box-shadow: 0 2px 0px #020617;
}

.beacon-title {
    font-family: var(--font-pixel-heading);
    font-size: 1.45rem;
    font-weight: 700;
    color: var(--pixel-cyan);
    letter-spacing: 0.06em;
    margin-bottom: 0.4rem;
    text-shadow: 0 0 10px rgba(0, 240, 255, 0.6), 2px 2px 0px #020617;
}

.beacon-subtitle {
    font-family: var(--font-pixel-label);
    font-size: 0.85rem;
    color: #38bdf8;
    letter-spacing: 0.1em;
    font-weight: 600;
    text-transform: uppercase;
}

/* Status Pills */
.beacon-status-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    padding: 0.3rem 0.65rem;
    font-family: var(--font-pixel-label);
    font-size: 0.72rem;
    font-weight: 700;
    border-radius: 0px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    border: 2px solid;
    box-shadow: 2px 2px 0px #020617;
}

.status-live {
    background-color: #064e3b;
    color: #10b981;
    border-color: #10b981;
    box-shadow: 2px 2px 0px #020617, 0 0 8px rgba(16, 185, 129, 0.4);
}
.status-memory {
    background-color: #451a03;
    color: #fbbf24;
    border-color: #f59e0b;
    box-shadow: 2px 2px 0px #020617, 0 0 8px rgba(245, 158, 11, 0.3);
}
.status-error {
    background-color: #450a0a;
    color: #f87171;
    border-color: #ef4444;
    box-shadow: 2px 2px 0px #020617, 0 0 8px rgba(239, 68, 68, 0.4);
}

/* Severity & Corroboration Badges */
.beacon-badge {
    display: inline-block;
    padding: 0.2rem 0.5rem;
    font-family: var(--font-pixel-label);
    font-size: 0.68rem;
    font-weight: 700;
    border-radius: 0px;
    line-height: 1.2;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    vertical-align: middle;
    border: 2px solid;
    box-shadow: 2px 2px 0px #020617;
}

.badge-critical { background-color: #450a0a; color: #fca5a5; border-color: #ef4444; }
.badge-severe { background-color: #431407; color: #fed7aa; border-color: #f97316; }
.badge-moderate { background-color: #3b2203; color: #fef08a; border-color: #f59e0b; }
.badge-minor { background-color: #082f49; color: #bae6fd; border-color: #38bdf8; }
.badge-unspecified { background-color: #1e293b; color: #cbd5e1; border-color: #475569; }

.badge-multi-source { background-color: #064e3b; color: #a7f3d0; border-color: #10b981; }
.badge-single-source { background-color: #0f172a; color: #94a3b8; border-color: #334155; }
.badge-conflict { background-color: #450a0a; color: #fca5a5; border-color: #ef4444; }

/* 8-Bit Pixel Cards */
.beacon-card, .beacon-pixel-card, .beacon-format-card {
    border: 2px solid var(--pixel-border);
    border-radius: 0px;
    padding: 1.15rem;
    background-color: var(--pixel-panel);
    margin-bottom: 0.85rem;
    box-shadow: 3px 3px 0px #020617;
    transition: all 0.15s ease-in-out;
}

.beacon-card:hover, .beacon-pixel-card:hover, .beacon-format-card:hover {
    border-color: var(--pixel-cyan);
    box-shadow: 3px 3px 0px rgba(0, 240, 255, 0.4), 0 0 10px rgba(0, 240, 255, 0.15);
}

.beacon-kpi-card {
    background-color: var(--pixel-panel);
    border: 2px solid var(--pixel-border);
    border-radius: 0px;
    padding: 1rem 1.15rem;
    margin-bottom: 0.75rem;
    box-shadow: 3px 3px 0px #020617;
    border-top: 3px solid var(--pixel-cyan);
    transition: all 0.15s ease-in-out;
}

.beacon-kpi-card:hover {
    border-color: var(--pixel-cyan);
    box-shadow: 3px 3px 0px rgba(0, 240, 255, 0.4), 0 0 12px rgba(0, 240, 255, 0.2);
}

/* KPI Accent Tags */
.kpi-tag {
    font-family: var(--font-pixel-label);
    font-size: 0.65rem;
    font-weight: 700;
    padding: 0.2rem 0.5rem;
    margin-bottom: 0.35rem;
    display: inline-block;
    border: 1px solid;
    box-shadow: 2px 2px 0px #020617;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}

.kpi-tag-cyan { background: #082f49; color: #00f0ff; border-color: #00f0ff; }
.kpi-tag-emerald { background: #064e3b; color: #10b981; border-color: #10b981; }
.kpi-tag-purple { background: #3b0764; color: #c084fc; border-color: #a855f7; }
.kpi-tag-amber { background: #451a03; color: #fbbf24; border-color: #f59e0b; }

.beacon-format-title {
    font-family: var(--font-pixel-label);
    font-size: 0.85rem;
    font-weight: 700;
    color: var(--pixel-cyan);
    margin-bottom: 0.35rem;
    letter-spacing: 0.04em;
}

.beacon-empty-box {
    border: 2px dashed var(--pixel-cyan);
    border-radius: 0px;
    padding: 2.25rem 1.5rem;
    text-align: center;
    background-color: #080d19;
    box-shadow: 4px 4px 0px #020617;
    margin: 1rem 0;
}

/* Streamlit Tabs 8-Bit Overrides */
div[data-testid="stTabs"] [role="tab"],
div[data-baseweb="tab-list"] [role="tab"],
div[data-testid="stTabs"] button[role="tab"],
div[data-testid="stTabs"] div[role="tab"] {
    font-family: var(--font-pixel-label) !important;
    font-size: 0.78rem !important;
    color: var(--pixel-text-dim) !important;
    background-color: var(--pixel-panel) !important;
    border: 2px solid var(--pixel-border) !important;
    border-bottom: none !important;
    box-shadow: 2px -2px 0px #020617 !important;
    padding: 0.55rem 0.95rem !important;
    margin-right: 0.35rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    border-radius: 0px !important;
    cursor: pointer !important;
    transition: all 0.15s ease !important;
}

div[data-testid="stTabs"] [role="tab"]:hover,
div[data-baseweb="tab-list"] [role="tab"]:hover {
    color: var(--pixel-cyan) !important;
    border-color: var(--pixel-cyan) !important;
    background-color: var(--pixel-surface) !important;
    box-shadow: 0 0 8px rgba(0, 240, 255, 0.4) !important;
}

div[data-testid="stTabs"] [role="tab"][aria-selected="true"],
div[data-baseweb="tab-list"] [role="tab"][aria-selected="true"] {
    color: var(--pixel-cyan) !important;
    background-color: var(--pixel-surface) !important;
    border-color: var(--pixel-cyan) !important;
    border-top: 3px solid var(--pixel-cyan) !important;
    box-shadow: 0 -2px 10px rgba(0, 240, 255, 0.5), inset 0 2px 0px var(--pixel-cyan) !important;
}

/* Streamlit Metric Overrides */
div[data-testid="stMetric"] {
    background: #0b1120 !important;
    border: 2px solid #1e293b !important;
    border-radius: 0px !important;
    box-shadow: 3px 3px 0px #020617 !important;
    padding: 0.75rem 1rem !important;
    border-top: 3px solid #00f0ff !important;
    transition: all 0.15s ease-in-out !important;
}

div[data-testid="stMetric"]:hover {
    border-color: #00f0ff !important;
    box-shadow: 3px 3px 0px rgba(0, 240, 255, 0.4), 0 0 12px rgba(0, 240, 255, 0.2) !important;
}

div[data-testid="stMetric"] label[data-testid="stMetricLabel"],
div[data-testid="stMetric"] label[data-testid="stMetricLabel"] p {
    font-family: var(--font-pixel-label) !important;
    font-size: 0.72rem !important;
    color: #94a3b8 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    margin: 0 !important;
}

div[data-testid="stMetric"] div[data-testid="stMetricValue"],
div[data-testid="stMetric"] div[data-testid="stMetricValue"] p {
    font-family: var(--font-pixel-heading) !important;
    font-size: 1.45rem !important;
    color: #00f0ff !important;
    text-shadow: 0 0 10px rgba(0, 240, 255, 0.7) !important;
    margin: 0 !important;
    line-height: 1.2 !important;
}

/* Streamlit Button Overrides - Prevent Red Accent Reversion */
button[data-testid="baseButton-secondary"],
button[data-testid="baseButton-primary"] {
    font-family: var(--font-pixel-label) !important;
    font-size: 0.78rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    border-radius: 0px !important;
    box-shadow: 3px 3px 0px #020617 !important;
    transition: all 0.1s ease !important;
}

button[data-testid="baseButton-secondary"] {
    background: #0f172a !important;
    color: var(--pixel-cyan) !important;
    border: 2px solid var(--pixel-cyan) !important;
}

button[data-testid="baseButton-primary"] {
    background: #0284c7 !important;
    color: #ffffff !important;
    border: 2px solid #38bdf8 !important;
    box-shadow: 3px 3px 0px #0369a1 !important;
}

button[data-testid="baseButton-secondary"]:hover,
button[data-testid="baseButton-primary"]:hover {
    transform: translate(1px, 1px) !important;
    box-shadow: 2px 2px 0px #020617, 0 0 10px rgba(0, 240, 255, 0.5) !important;
    border-color: var(--pixel-cyan) !important;
}

button:disabled,
button[disabled] {
    opacity: 0.45 !important;
    cursor: not-allowed !important;
    border-color: #334155 !important;
    color: #64748b !important;
    transform: none !important;
    box-shadow: 2px 2px 0px #020617 !important;
}

/* Tables and Dataframes */
div[data-testid="stDataFrame"],
div[data-testid="stTable"] {
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    background-color: var(--pixel-panel) !important;
    box-shadow: 3px 3px 0px #020617 !important;
    overflow-x: auto !important;
}

div[data-testid="stDataFrame"]:hover,
div[data-testid="stTable"]:hover {
    border-color: var(--pixel-border-bright) !important;
}

/* HTML / Native Table Elements */
table {
    border-collapse: collapse !important;
    width: 100% !important;
    background-color: var(--pixel-panel) !important;
    border: 2px solid var(--pixel-border) !important;
    font-family: var(--font-mono) !important;
}

thead th {
    background-color: #050811 !important;
    color: var(--pixel-cyan) !important;
    font-family: var(--font-pixel-label) !important;
    font-size: 0.75rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    border-bottom: 2px solid var(--pixel-border-bright) !important;
    padding: 0.65rem 0.85rem !important;
    text-align: left !important;
}

tbody td {
    color: var(--pixel-text) !important;
    font-family: var(--font-mono) !important;
    font-size: 0.82rem !important;
    border-bottom: 1px solid var(--pixel-border) !important;
    padding: 0.6rem 0.85rem !important;
}

tbody tr:nth-child(even) {
    background-color: rgba(15, 23, 42, 0.4) !important;
}

tbody tr:hover {
    background-color: rgba(0, 240, 255, 0.06) !important;
}

/* Form Inputs & Controls */
input, select, textarea,
div[data-baseweb="input"] input,
div[data-baseweb="textarea"] textarea {
    background-color: var(--pixel-panel) !important;
    color: var(--pixel-text) !important;
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    box-shadow: inset 2px 2px 0px #020617 !important;
    font-family: var(--font-mono) !important;
    font-size: 0.85rem !important;
}

div[data-baseweb="input"],
div[data-baseweb="textarea"] {
    background-color: transparent !important;
    border-radius: 0px !important;
}

input:focus, select:focus, textarea:focus,
div[data-baseweb="input"]:focus-within input,
div[data-baseweb="textarea"]:focus-within textarea {
    border-color: var(--pixel-cyan) !important;
    box-shadow: inset 2px 2px 0px #020617, 0 0 8px rgba(0, 240, 255, 0.5) !important;
    outline: none !important;
}

/* Selectbox and Multiselect */
div[data-testid="stSelectbox"] > div > div,
div[data-testid="stMultiSelect"] > div > div {
    background-color: var(--pixel-panel) !important;
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    color: var(--pixel-text) !important;
    box-shadow: inset 2px 2px 0px #020617 !important;
}

div[data-testid="stSelectbox"] > div > div:hover,
div[data-testid="stMultiSelect"] > div > div:hover {
    border-color: var(--pixel-border-bright) !important;
}

div[data-testid="stSelectbox"] > div > div:focus-within,
div[data-testid="stMultiSelect"] > div > div:focus-within {
    border-color: var(--pixel-cyan) !important;
    box-shadow: inset 2px 2px 0px #020617, 0 0 8px rgba(0, 240, 255, 0.5) !important;
}

/* BaseWeb Dropdown Popover & Menu Items */
div[data-baseweb="popover"],
div[data-baseweb="popover"] > div,
ul[data-baseweb="menu"] {
    background-color: #0b1120 !important;
    border: 2px solid var(--pixel-cyan) !important;
    border-radius: 0px !important;
    box-shadow: 4px 4px 0px #020617, 0 0 12px rgba(0, 240, 255, 0.3) !important;
}

li[data-baseweb="menu-item"] {
    background-color: #0b1120 !important;
    color: #f1f5f9 !important;
    font-family: var(--font-mono) !important;
    font-size: 0.82rem !important;
    border-radius: 0px !important;
    padding: 0.5rem 0.85rem !important;
    border-bottom: 1px solid #1e293b !important;
}

li[data-baseweb="menu-item"]:hover,
li[data-baseweb="menu-item"][aria-selected="true"] {
    background-color: #0f172a !important;
    color: var(--pixel-cyan) !important;
}

/* Multiselect Tags */
span[data-baseweb="tag"] {
    background-color: #082f49 !important;
    color: var(--pixel-cyan) !important;
    border: 1px solid var(--pixel-cyan) !important;
    border-radius: 0px !important;
    font-family: var(--font-pixel-label) !important;
    font-size: 0.7rem !important;
}

span[data-baseweb="tag"] svg {
    fill: var(--pixel-cyan) !important;
}

/* Checkbox & Radio Controls */
div[data-testid="stCheckbox"] label,
div[data-testid="stRadio"] label {
    font-family: var(--font-pixel-label) !important;
    font-size: 0.78rem !important;
    color: var(--pixel-text) !important;
    letter-spacing: 0.04em !important;
}

div[data-testid="stCheckbox"] input[type="checkbox"],
div[data-testid="stRadio"] input[type="radio"] {
    accent-color: var(--pixel-cyan) !important;
}

/* Slider */
div[data-testid="stSlider"] div[role="slider"] {
    background-color: var(--pixel-cyan) !important;
    border: 2px solid #ffffff !important;
    border-radius: 0px !important;
    box-shadow: 2px 2px 0px #020617, 0 0 8px rgba(0, 240, 255, 0.7) !important;
}

/* File Uploader Dropzone */
div[data-testid="stFileUploader"] {
    border: 2px dashed var(--pixel-cyan) !important;
    border-radius: 0px !important;
    background: #080d19 !important;
    box-shadow: 4px 4px 0px #020617 !important;
    padding: 1.25rem !important;
}

div[data-testid="stFileUploader"] section {
    background-color: transparent !important;
    border: none !important;
}

div[data-testid="stFileUploader"] span,
div[data-testid="stFileUploader"] small {
    font-family: var(--font-pixel-label) !important;
    color: var(--pixel-text-dim) !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.04em !important;
}

/* Expanders */
div[data-testid="stExpander"] {
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    background-color: var(--pixel-panel) !important;
    box-shadow: 3px 3px 0px #020617 !important;
    margin-bottom: 0.75rem !important;
}

div[data-testid="stExpander"]:hover {
    border-color: var(--pixel-border-bright) !important;
}

div[data-testid="stExpander"] summary {
    font-family: var(--font-pixel-label) !important;
    font-size: 0.82rem !important;
    color: var(--pixel-text) !important;
    letter-spacing: 0.04em !important;
}

div[data-testid="stExpander"] summary svg {
    color: var(--pixel-cyan) !important;
}

div[data-testid="stExpander"] div[data-testid="stExpanderDetails"] {
    border-top: 1px solid var(--pixel-border) !important;
}

/* Alerts / Banners */
div[data-testid="stAlert"] {
    border-radius: 0px !important;
    border: 2px solid !important;
    box-shadow: 3px 3px 0px #020617 !important;
    font-family: var(--font-mono) !important;
    font-size: 0.85rem !important;
}

div[data-testid="stAlert"] [data-testid="stMarkdownContainer"] p {
    font-family: var(--font-mono) !important;
    font-size: 0.85rem !important;
}

/* Progress Bar */
div[data-testid="stProgress"] {
    background-color: var(--pixel-surface) !important;
    border: 1px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    height: 14px !important;
    box-shadow: inset 2px 2px 0px #020617 !important;
}

div[data-testid="stProgress"] > div > div > div {
    background-color: var(--pixel-cyan) !important;
    box-shadow: 0 0 10px rgba(0, 240, 255, 0.6) !important;
    border-radius: 0px !important;
}

/* Charts and Maps Containers */
div[data-testid="stDeckGlJsonChart"],
div[data-testid="stPlotlyChart"],
div[data-testid="stVegaLiteChart"] {
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    background-color: #050811 !important;
    box-shadow: 3px 3px 0px #020617 !important;
    padding: 0.35rem !important;
}

/* Tooltips */
div[data-testid="stTooltipContent"] {
    background-color: #050811 !important;
    border: 2px solid var(--pixel-cyan) !important;
    border-radius: 0px !important;
    color: var(--pixel-text) !important;
    box-shadow: 3px 3px 0px #020617 !important;
    font-family: var(--font-mono) !important;
    font-size: 0.78rem !important;
}

/* Code Blocks */
code {
    background-color: #050811 !important;
    border: 1px solid var(--pixel-border) !important;
    color: var(--pixel-cyan) !important;
    border-radius: 0px !important;
    font-family: var(--font-mono) !important;
    padding: 0.15rem 0.35rem !important;
}

pre {
    background-color: #050811 !important;
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    box-shadow: 3px 3px 0px #020617 !important;
}


/* JSON Tree Viewer 8-Bit Overrides */
div[data-testid="stJson"],
div[data-testid="stJson"] pre {
    background-color: #050811 !important;
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    color: var(--pixel-text) !important;
    font-family: var(--font-mono) !important;
    font-size: 0.8rem !important;
    box-shadow: 3px 3px 0px #020617 !important;
    padding: 0.75rem !important;
}

/* File Uploader Selected File Items & Delete Button */
div[data-testid="stFileUploaderFile"] {
    background-color: #0b1120 !important;
    border: 1px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    box-shadow: 2px 2px 0px #020617 !important;
    padding: 0.45rem 0.75rem !important;
    color: var(--pixel-text) !important;
    margin-top: 0.4rem !important;
}

div[data-testid="stFileUploaderFile"] button[data-testid="stFileUploaderDeleteFile"],
div[data-testid="stFileUploaderFile"] button {
    border-radius: 0px !important;
    background: #1e1035 !important;
    border: 1px solid #ef4444 !important;
    color: #f87171 !important;
    padding: 0.15rem 0.45rem !important;
}

div[data-testid="stFileUploaderFile"] button:hover {
    background: #450a0a !important;
    color: #ffffff !important;
    box-shadow: 0 0 6px #ef4444 !important;
}

/* Date Input & Sliders */
div[data-testid="stDateInput"] input {
    background-color: var(--pixel-panel) !important;
    color: var(--pixel-text) !important;
    border: 2px solid var(--pixel-border) !important;
    border-radius: 0px !important;
    box-shadow: inset 2px 2px 0px #020617 !important;
    font-family: var(--font-mono) !important;
}

/* Process Timeline 8-Bit Components */
.beacon-timeline-container {
    background-color: #070c18;
    border: 2px solid var(--pixel-border);
    box-shadow: 3px 3px 0px #020617;
    padding: 1.15rem 1.35rem;
    margin: 1rem 0;
}

.beacon-timeline-header {
    font-family: var(--font-pixel-heading);
    font-size: 0.95rem;
    color: var(--pixel-cyan);
    letter-spacing: 0.06em;
    margin-bottom: 0.95rem;
    text-shadow: 0 0 8px rgba(0, 240, 255, 0.4);
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.beacon-stage-row {
    display: flex;
    align-items: flex-start;
    gap: 0.85rem;
    padding: 0.65rem 0.75rem;
    margin-bottom: 0.45rem;
    background: #0b1120;
    border-left: 3px solid var(--pixel-border);
    transition: all 0.15s ease;
}

.beacon-stage-row:hover {
    background: #0f172a;
    border-left-color: var(--pixel-cyan);
}

.beacon-stage-badge {
    font-family: var(--font-pixel-label);
    font-size: 0.68rem;
    font-weight: 700;
    padding: 0.2rem 0.55rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border: 1px solid;
    box-shadow: 1px 1px 0px #020617;
    white-space: nowrap;
}

.stage-completed { background: #064e3b; color: #10b981; border-color: #10b981; }
.stage-warning { background: #451a03; color: #fbbf24; border-color: #f59e0b; }
.stage-failed { background: #450a0a; color: #f87171; border-color: #ef4444; }
.stage-running { background: #082f49; color: #00f0ff; border-color: #00f0ff; animation: beacon-beam-pulse 1.8s infinite; }
.stage-pending { background: #0f172a; color: #64748b; border-color: #334155; }
.stage-skipped { background: #1e293b; color: #94a3b8; border-color: #475569; }

.beacon-stage-title {
    font-family: var(--font-pixel-label);
    font-size: 0.82rem;
    font-weight: 700;
    color: #f1f5f9;
    letter-spacing: 0.04em;
    margin-bottom: 0.2rem;
}

.beacon-stage-msg {
    font-family: var(--font-mono);
    font-size: 0.8rem;
    color: #94a3b8;
    line-height: 1.4;
}

.beacon-stage-metrics {
    margin-left: auto;
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--pixel-cyan);
    white-space: nowrap;
}

/* Incident & Observation Cards */
.beacon-obs-card {
    border: 2px solid var(--pixel-border);
    background: #0b1120;
    box-shadow: 3px 3px 0px #020617;
    padding: 1rem 1.15rem;
    margin-bottom: 0.85rem;
    transition: all 0.15s ease;
}

.beacon-obs-card:hover {
    border-color: var(--pixel-cyan);
    box-shadow: 3px 3px 0px rgba(0, 240, 255, 0.4), 0 0 10px rgba(0, 240, 255, 0.15);
}

.beacon-obs-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.6rem;
    border-bottom: 1px solid var(--pixel-border);
    padding-bottom: 0.5rem;
}

.beacon-obs-title {
    font-family: var(--font-pixel-heading);
    font-size: 0.88rem;
    font-weight: 700;
    color: #f1f5f9;
    display: flex;
    align-items: center;
    gap: 0.6rem;
}

.beacon-obs-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 0.6rem;
    margin: 0.65rem 0;
    font-size: 0.82rem;
}

.beacon-obs-field-label {
    font-family: var(--font-pixel-label);
    font-size: 0.68rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 0.15rem;
}

.beacon-obs-field-val {
    color: #cbd5e1;
    font-family: var(--font-mono);
}

.beacon-obs-claim {
    background: #050811;
    border-left: 3px solid var(--pixel-cyan);
    padding: 0.6rem 0.85rem;
    margin: 0.6rem 0;
    font-size: 0.82rem;
    color: #f1f5f9;
    font-style: italic;
}

/* ==============================================================================
 * Minecraft Beacon Light Beam & Particle Animations
 * ============================================================================== */

@keyframes beacon-beam-pulse {
    0%, 100% {
        opacity: 0.82;
        filter: drop-shadow(0 0 6px #00f0ff);
    }
    50% {
        opacity: 1.0;
        filter: drop-shadow(0 0 16px #00f0ff);
    }
}

@keyframes beacon-mote-rise-1 {
    0% { transform: translateY(0); opacity: 0; }
    30% { opacity: 0.9; }
    80% { opacity: 0.8; }
    100% { transform: translateY(-70px); opacity: 0; }
}

@keyframes beacon-mote-rise-2 {
    0% { transform: translateY(0); opacity: 0; }
    40% { opacity: 1; }
    75% { opacity: 0.7; }
    100% { transform: translateY(-60px); opacity: 0; }
}

.beacon-pixel-svg {
    overflow: visible !important;
}

.beacon-animated-beam {
    animation: beacon-beam-pulse 2.4s ease-in-out infinite;
}

.beacon-particle-1 {
    animation: beacon-mote-rise-1 3.2s linear infinite;
}

.beacon-particle-2 {
    animation: beacon-mote-rise-2 2.6s linear infinite 1.3s;
}

/* Accessibility: Respect prefers-reduced-motion */
@media (prefers-reduced-motion: reduce) {
    .beacon-animated-beam,
    .beacon-particle-1,
    .beacon-particle-2,
    .beacon-beam-glow,
    .beacon-beam-mid,
    .beacon-beam-core,
    .beacon-beam-laser {
        animation: none !important;
        opacity: 0.88 !important;
        filter: drop-shadow(0 0 6px #00f0ff) !important;
    }
    .beacon-beam-mote,
    [class*="beacon-beam-mote"] {
        display: none !important;
        animation: none !important;
    }
}
</style>
"""


def inject_custom_styles() -> None:
    """Inject custom 8-bit CSS design system into Streamlit document head."""
    st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)


# ==============================================================================
# 2. Minecraft-Inspired Pixel-Art Beacon Logo (Pure SVG)
# ==============================================================================

def get_minecraft_beacon_svg() -> str:
    """Generate an original pixel-art SVG representing a lit Minecraft Beacon block.

    Features:
    - Vertical electric cyan beam ascending through the sky.
    - Gentle vertical glow and opacity pulsing with reduced-motion support.
    - Glass casing with refraction glints, dark obsidian base, and glowing Nether Star core.
    - Stepped 3-tier pyramid foundation (diamond, netherite, obsidian layers).
    """
    return """
<div style="display: flex; justify-content: center; margin-bottom: 0.75rem;">
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 130" width="160" height="190" shape-rendering="crispEdges" class="beacon-pixel-svg">
  <defs>
    <filter id="beaconGlow" x="-100%" y="-100%" width="300%" height="300%">
      <feGaussianBlur stdDeviation="3" result="blur" />
      <feMerge>
        <feMergeNode in="blur" />
        <feMergeNode in="SourceGraphic" />
      </feMerge>
    </filter>
  </defs>

  <!-- Faint pixel stars in the sky -->
  <rect x="15" y="12" width="2" height="2" fill="#38bdf8" opacity="0.6" />
  <rect x="82" y="18" width="2" height="2" fill="#a5f3fc" opacity="0.8" />
  <rect x="25" y="45" width="2" height="2" fill="#00f0ff" opacity="0.5" />
  <rect x="74" y="52" width="2" height="2" fill="#38bdf8" opacity="0.7" />
  <rect x="12" y="70" width="2" height="2" fill="#ffffff" opacity="0.4" />
  <rect x="85" y="75" width="2" height="2" fill="#a5f3fc" opacity="0.6" />

  <!-- Vertical Beacon Beam (Ascending from casing through entire sidebar height) -->
  <!-- Outer aura glow -->
  <rect x="36" y="-1500" width="28" height="1592" fill="#00f0ff" opacity="0.12" class="beacon-animated-beam" filter="url(#beaconGlow)" />
  <!-- Mid beam -->
  <rect x="42" y="-1500" width="16" height="1592" fill="#38bdf8" opacity="0.4" class="beacon-animated-beam" />
  <!-- Core electric beam -->
  <rect x="46" y="-1500" width="8" height="1592" fill="#a5f3fc" opacity="0.85" class="beacon-animated-beam" />
  <!-- Center super-bright core line -->
  <rect x="48" y="-1500" width="4" height="1592" fill="#ffffff" opacity="0.95" class="beacon-animated-beam" />

  <!-- Floating energy motes (CSS animated) near casing & ascending through middle/upper beam -->
  <rect x="45" y="80" width="2" height="2" fill="#ffffff" class="beacon-particle-1" />
  <rect x="52" y="65" width="2" height="2" fill="#00f0ff" class="beacon-particle-2" />
  <rect x="47" y="40" width="3" height="3" fill="#a5f3fc" class="beacon-particle-1" />
  <rect x="50" y="20" width="2" height="2" fill="#ffffff" class="beacon-particle-2" />
  <rect x="44" y="0" width="2" height="2" fill="#00f0ff" class="beacon-particle-1" opacity="0.9" />
  <rect x="53" y="-30" width="3" height="3" fill="#a5f3fc" class="beacon-particle-2" opacity="0.85" />
  <rect x="46" y="-80" width="2" height="2" fill="#ffffff" class="beacon-particle-1" opacity="0.9" />
  <rect x="51" y="-140" width="3" height="3" fill="#38bdf8" class="beacon-particle-2" opacity="0.8" />
  <rect x="45" y="-220" width="2" height="2" fill="#00f0ff" class="beacon-particle-1" opacity="0.85" />
  <rect x="52" y="-320" width="2" height="2" fill="#ffffff" class="beacon-particle-2" opacity="0.9" />
  <rect x="47" y="-450" width="3" height="3" fill="#a5f3fc" class="beacon-particle-1" opacity="0.8" />
  <rect x="49" y="-600" width="2" height="2" fill="#00f0ff" class="beacon-particle-2" opacity="0.85" />
  <rect x="44" y="-750" width="2" height="2" fill="#ffffff" class="beacon-particle-1" opacity="0.8" />
  <rect x="53" y="-900" width="3" height="3" fill="#38bdf8" class="beacon-particle-2" opacity="0.75" />
  <rect x="46" y="-1100" width="2" height="2" fill="#a5f3fc" class="beacon-particle-1" opacity="0.8" />
  <rect x="50" y="-1300" width="2" height="2" fill="#00f0ff" class="beacon-particle-2" opacity="0.85" />

  <!-- Glass Casing of the Beacon Block (x=38, y=74, w=24, h=22) -->
  <rect x="38" y="74" width="24" height="2" fill="#7dd3fc" opacity="0.85" />
  <rect x="38" y="74" width="2" height="22" fill="#7dd3fc" opacity="0.85" />
  <rect x="60" y="74" width="2" height="22" fill="#0284c7" opacity="0.85" />
  <rect x="38" y="94" width="24" height="2" fill="#0284c7" opacity="0.85" />

  <!-- Glass interior tint -->
  <rect x="40" y="76" width="20" height="18" fill="#0c4a6e" opacity="0.4" />

  <!-- Glass reflection glints -->
  <rect x="42" y="78" width="4" height="2" fill="#ffffff" opacity="0.75" />
  <rect x="40" y="80" width="2" height="4" fill="#ffffff" opacity="0.75" />

  <!-- Obsidian Base of the Beacon Block -->
  <rect x="40" y="90" width="20" height="4" fill="#1e1035" />
  <rect x="42" y="91" width="16" height="2" fill="#3b1d60" />

  <!-- Nether Star / Beacon Core (Pulsing Diamond Star) -->
  <rect x="46" y="82" width="8" height="6" fill="#00f0ff" />
  <rect x="47" y="81" width="6" height="8" fill="#38bdf8" />
  <rect x="48" y="83" width="4" height="4" fill="#ffffff" />

  <!-- Stepped Pedestal Base (Pyramid tiers) -->
  <!-- Tier 1: Diamond block layer (y=96, w=36) -->
  <rect x="32" y="96" width="36" height="6" fill="#0284c7" />
  <rect x="34" y="96" width="32" height="2" fill="#38bdf8" />
  <rect x="34" y="100" width="32" height="2" fill="#0369a1" />
  <rect x="43" y="96" width="2" height="6" fill="#0c4a6e" />
  <rect x="55" y="96" width="2" height="6" fill="#0c4a6e" />

  <!-- Tier 2: Netherite / Dark Prismarine layer (y=102, w=52) -->
  <rect x="24" y="102" width="52" height="8" fill="#1e293b" />
  <rect x="26" y="102" width="48" height="2" fill="#334155" />
  <rect x="24" y="108" width="52" height="2" fill="#0f172a" />
  <rect x="37" y="102" width="2" height="8" fill="#020617" />
  <rect x="51" y="102" width="2" height="8" fill="#020617" />
  <rect x="63" y="102" width="2" height="8" fill="#020617" />

  <!-- Tier 3: Foundation bedrock/obsidian (y=110, w=72) -->
  <rect x="14" y="110" width="72" height="10" fill="#0f172a" />
  <rect x="16" y="110" width="68" height="2" fill="#1e293b" />
  <rect x="14" y="118" width="72" height="2" fill="#020617" />
  <rect x="30" y="110" width="2" height="10" fill="#020617" />
  <rect x="48" y="110" width="2" height="10" fill="#020617" />
  <rect x="68" y="110" width="2" height="10" fill="#020617" />

  <!-- Ground Stone Rim (y=120, w=100) -->
  <rect x="0" y="120" width="100" height="10" fill="#020617" />
  <rect x="0" y="120" width="100" height="2" fill="#0f172a" />
</svg>
</div>
"""


# ==============================================================================
# 3. Status & Severity Badges
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


def get_system_status_pill_html(is_connected: bool, schema_ready: bool) -> str:
    """Generate HTML status badge for system operational indicator."""
    if schema_ready:
        return '<span class="beacon-status-pill status-live">● POSTGRESQL ONLINE</span>'
    elif is_connected:
        return '<span class="beacon-status-pill status-memory">● SCHEMA UNINITIALIZED</span>'
    return '<span class="beacon-status-pill status-memory">● IN-MEMORY (STATELESS)</span>'


# ==============================================================================
# 4. Geographic Context Helpers
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
# 5. Empty State Renderer
# ==============================================================================

def render_empty_state(
    title: str,
    message: str,
    icon: str = "🔍",
    guidance: Optional[str] = None,
) -> None:
    """Render a clean, informative 8-bit empty-state container with actionable guidance."""
    st.markdown(
        f"""
        <div class="beacon-empty-box">
            <div style="font-size: 2rem; margin-bottom: 0.5rem; filter: drop-shadow(0 0 6px #00f0ff);">{icon}</div>
            <div style="font-family: var(--font-pixel-heading); font-size: 0.95rem; color: #00f0ff; margin-bottom: 0.5rem; text-shadow: 0 0 8px rgba(0, 240, 255, 0.4);">{title}</div>
            <div style="color: #94a3b8; font-size: 0.88rem; max-width: 540px; margin: 0 auto; line-height: 1.5;">{message}</div>
            {f'<div style="color: #38bdf8; font-family: var(--font-pixel-label); font-size: 0.75rem; margin-top: 0.85rem; letter-spacing: 0.05em;">{guidance}</div>' if guidance else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )

# ==============================================================================
# 6. Real-Time Process Timelines
# ==============================================================================

def render_process_timeline(
    stages: Sequence[dict[str, Any]],
    title: str = "Ingestion Process Timeline",
    placeholder: Optional[Any] = None,
) -> None:
    """Render an 8-bit animated step timeline for multi-stage operational processes."""
    status_icons = {
        "completed": "✅",
        "warning": "⚠️",
        "failed": "❌",
        "running": "⚡",
        "pending": "⏳",
        "skipped": "⏭️",
    }
    rows_html = []
    for s in stages:
        st_val = s.get("status", "pending").lower()
        badge_cls = f"stage-{st_val}"
        icon = status_icons.get(st_val, "●")
        name = s.get("name", "Stage")
        msg = s.get("message", "")
        elapsed = s.get("elapsed")
        elapsed_str = f"{elapsed:.2f}s" if elapsed is not None else ""
        counts = s.get("counts", {})
        counts_str = " | ".join(f"{k.replace('_', ' ').title()}: {v}" for k, v in counts.items())
        meta_str = f"{counts_str} ({elapsed_str})" if elapsed_str else counts_str

        meta_div = f'<div class="beacon-stage-metrics">{meta_str}</div>' if meta_str else ''
        row_snippet = (
            f'<div class="beacon-stage-row">'
            f'<span class="beacon-stage-badge {badge_cls}">{icon} {st_val.upper()}</span>'
            f'<div style="flex: 1 1 auto;">'
            f'<div class="beacon-stage-title">{name}</div>'
            f'<div class="beacon-stage-msg">{msg}</div>'
            f'</div>'
            f'{meta_div}'
            f'</div>'
        )
        rows_html.append(row_snippet)

    full_html = (
        f'<div class="beacon-timeline-container">'
        f'<div class="beacon-timeline-header">'
        f'<span>⚡ {title.upper()}</span>'
        f'<span style="font-size: 0.75rem; color: #38bdf8;">REAL-TIME PIPELINE TRACE</span>'
        f'</div>'
        f'{"".join(rows_html)}'
        f'</div>'
    )
    if placeholder is not None:
        placeholder.markdown(full_html, unsafe_allow_html=True)
    else:
        st.markdown(full_html, unsafe_allow_html=True)


def render_pipeline_timeline(
    result: Any,
    measured_exec_time: Optional[float] = None,
    animate: bool = False,
    min_presentation_seconds: float = 4.0,
) -> None:
    """Render the 5-stage ETL Pipeline Execution Timeline (Extract -> Transform -> Validate -> Resolve -> Load).

    If animate is True, progressively displays stages transitioning through pending, running,
    and completed over min_presentation_seconds (default >= 4.0s) using a monotonic clock,
    while displaying the real monotonic execution time measured during processing.
    Format: 'Execution: 0.03 s | Timeline: 4.0 s'
    """
    if result is None:
        return

    # Derive real stage statuses from actual pipeline result
    extract_status = "completed"
    if result.files_failed > 0 and result.files_succeeded > 0:
        extract_status = "warning"
    elif result.files_failed > 0 and result.files_succeeded == 0:
        extract_status = "failed"

    transform_status = "completed"

    val_status = "completed"
    if result.observations_rejected > 0:
        val_status = "warning" if result.observations_accepted > 0 else "failed"

    res_summary = result.resolution_summary or {}
    resolve_status = "completed" if result.events_synthesized > 0 or result.observations_accepted == 0 else "skipped"

    load_status = "completed"
    if not result.storage_enabled:
        load_status = "warning"
    elif any("Database persistence failed" in str(e) for e in result.errors):
        load_status = "failed"

    final_stages = [
        {
            "stage_id": "extract",
            "name": "1. Extract",
            "status": extract_status,
            "message": f"Extracted {result.observations_extracted} observation(s) across {result.files_discovered} source file(s).",
            "counts": {"succeeded": result.files_succeeded, "failed": result.files_failed},
        },
        {
            "stage_id": "transform",
            "name": "2. Transform",
            "status": transform_status,
            "message": f"Normalized spatiotemporal bounds and severity tags for {result.observations_transformed} record(s).",
            "counts": {"transformed": result.observations_transformed},
        },
        {
            "stage_id": "validate",
            "name": "3. Validate (Data Quality)",
            "status": val_status,
            "message": f"Applied DQ rules: {result.observations_accepted} accepted, {result.observations_rejected} rejected.",
            "counts": {"accepted": result.observations_accepted, "rejected": result.observations_rejected},
        },
        {
            "stage_id": "resolve",
            "name": "4. Resolve (Entity Synthesis)",
            "status": resolve_status,
            "message": (
                f"Synthesized {result.events_synthesized} canonical event(s) "
                f"({res_summary.get('ambiguous_count', 0)} ambiguous candidates flagged)."
            ),
            "counts": {"events": result.events_synthesized, "ambiguous": res_summary.get("ambiguous_count", 0)},
        },
        {
            "stage_id": "load",
            "name": "5. Load (Persistence)",
            "status": load_status,
            "message": (
                f"Persisted {result.records_persisted} record(s) and {result.events_persisted} event(s) to PostgreSQL."
                if result.storage_enabled and load_status == "completed"
                else ("Storage unconfigured: Cached in-memory without persistent DB writes." if not result.storage_enabled else "Persistence failed.")
            ),
            "counts": {"records_persisted": result.records_persisted, "events_persisted": result.events_persisted},
        },
    ]

    exec_dur = measured_exec_time if measured_exec_time is not None else getattr(result, "duration_seconds", 0.0)
    if exec_dur is None:
        exec_dur = 0.0

    if animate:
        timeline_start = time.perf_counter()
        placeholder = st.empty()
        stage_count = len(final_stages)
        stage_delay = min_presentation_seconds / max(1, stage_count)

        for step in range(stage_count):
            current_stages = []
            for i, stg in enumerate(final_stages):
                if i < step:
                    current_stages.append(dict(stg))
                elif i == step:
                    running_stg = dict(stg)
                    running_stg["status"] = "running"
                    running_stg["message"] = f"Executing {stg['name']}..."
                    current_stages.append(running_stg)
                else:
                    pending_stg = dict(stg)
                    pending_stg["status"] = "pending"
                    pending_stg["message"] = "Waiting in execution queue..."
                    current_stages.append(pending_stg)

            elapsed_timeline = time.perf_counter() - timeline_start
            header_title = (
                f"Pipeline Run '{result.execution_id}' | "
                f"Execution: {exec_dur:.2f} s | Timeline: {elapsed_timeline:.1f} s"
            )
            render_process_timeline(current_stages, title=header_title, placeholder=placeholder)
            time.sleep(stage_delay)

        total_timeline = max(min_presentation_seconds, time.perf_counter() - timeline_start)
        final_header = (
            f"Pipeline Run '{result.execution_id}' | "
            f"Execution: {exec_dur:.2f} s | Timeline: {total_timeline:.1f} s"
        )
        render_process_timeline(final_stages, title=final_header, placeholder=placeholder)
    else:
        # Instantaneous rendering (no sleep) with execution and timeline duration
        fallback_timeline = getattr(result, "timeline_presentation_seconds", None)
        if fallback_timeline is None or not isinstance(fallback_timeline, (int, float)):
            fallback_timeline = min_presentation_seconds
        header_title = (
            f"Pipeline Run '{result.execution_id}' | "
            f"Execution: {exec_dur:.2f} s | Timeline: {fallback_timeline:.1f} s"
        )
        render_process_timeline(final_stages, title=header_title)


# ==============================================================================
# 7. Unified Incident & Observation Cards
# ==============================================================================

def render_observation_cards(
    observations: Sequence[DisasterObservation],
    event_map: Optional[dict[str, str]] = None,
    resolution_decisions: Optional[dict[str, dict[str, Any]]] = None,
) -> None:
    """Render readable, expandable incident cards for extracted observations."""
    if not observations:
        render_empty_state("NO OBSERVATIONS EXTRACTED", "No disaster observations were extracted from the processed files.")
        return

    ev_map = event_map or {}
    dec_map = resolution_decisions or {}

    for idx, obs in enumerate(observations):
        event_id = ev_map.get(obs.observation_id)
        decision_info = dec_map.get(obs.observation_id)

        # Build card header
        sev_html = get_severity_badge_html(obs.severity)
        if event_id:
            res_status_html = f'<span class="beacon-badge badge-multi-source">LINKED: {event_id}</span>'
        elif decision_info and decision_info.get("decision_type") == "ambiguous_candidate":
            res_status_html = '<span class="beacon-badge badge-conflict">AMBIGUOUS CANDIDATE</span>'
        else:
            res_status_html = '<span class="beacon-badge badge-single-source">STANDALONE / RESOLVED</span>'

        loc_str = obs.location or "Unknown Location"
        coords_str = f"({obs.latitude:.4f}, {obs.longitude:.4f})" if (obs.latitude is not None and obs.longitude is not None) else "[No GPS Coordinates]"
        time_str = obs.event_time.strftime("%Y-%m-%d %H:%M:%S UTC") if obs.event_time else "⚠️ Missing / Unanchored temporal reference"

        # Determine expander label
        label = f"[{obs.event_type.upper()}] {loc_str} — {obs.observation_id} (Source: {obs.source_id})"

        with st.expander(label, expanded=(idx < 3)):
            claim_text = obs.raw_claim or obs.description or ""
            claim_html = f'<div class="beacon-obs-claim">"{claim_text}"</div>' if claim_text else ''
            card_html = (
                f'<div class="beacon-obs-card">'
                f'<div class="beacon-obs-header">'
                f'<div class="beacon-obs-title">'
                f'{sev_html}'
                f'<span>{obs.event_type.upper()} OBSERVATION</span>'
                f'</div>'
                f'<div>{res_status_html}</div>'
                f'</div>'
                f'<div class="beacon-obs-grid">'
                f'<div>'
                f'<div class="beacon-obs-field-label">Location & Coordinates</div>'
                f'<div class="beacon-obs-field-val">📍 {loc_str} {coords_str}</div>'
                f'</div>'
                f'<div>'
                f'<div class="beacon-obs-field-label">Temporal Anchor</div>'
                f'<div class="beacon-obs-field-val">🕒 {time_str}</div>'
                f'</div>'
                f'<div>'
                f'<div class="beacon-obs-field-label">Source Identifier</div>'
                f'<div class="beacon-obs-field-val">📡 <code>{obs.source_id}</code> (ref: {obs.source_reference or "N/A"})</div>'
                f'</div>'
                f'<div>'
                f'<div class="beacon-obs-field-label">Observation ID</div>'
                f'<div class="beacon-obs-field-val">🆔 <code>{obs.observation_id}</code></div>'
                f'</div>'
                f'</div>'
                f'{claim_html}'
                f'</div>'
            )
            st.markdown(card_html, unsafe_allow_html=True)

            # Validation findings / warnings
            if obs.validation_warnings:
                st.warning(f"⚠️ Validation Findings ({len(obs.validation_warnings)}):")
                for w in obs.validation_warnings:
                    st.write(f"- {w}")
            else:
                st.caption("✅ All data-quality baseline checks passed.")

            # Decision rationale if available
            if decision_info:
                st.info(
                    f"**Resolution Audit:** Score: `{decision_info.get('composite_score', 0):.2f}` — "
                    f"Rationale: *{decision_info.get('rationale', 'No rationale recorded.')}*"
                )

            # Secondary raw JSON inspector for debugging
            with st.expander("🔍 Raw Observation Payload & Metadata (JSON)"):
                st.json(obs.model_dump(mode="json"))
