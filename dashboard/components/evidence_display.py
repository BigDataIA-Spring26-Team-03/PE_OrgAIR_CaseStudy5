"""
Evidence Display Component — Streamlit UI for CS4 justifications (Task 9.5).

Exposes three public functions:
  render_evidence_card()          — single dimension card with L1-L5 badge
  render_company_evidence_panel() — full 7-dimension tabbed panel
  render_evidence_summary_table() — compact colour-coded summary table
"""
from __future__ import annotations

from typing import Dict

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEVEL_COLORS: Dict[int, str] = {
    1: "#ef4444",  # red    — Nascent
    2: "#f97316",  # orange — Developing
    3: "#eab308",  # yellow — Adequate
    4: "#22c55e",  # green  — Good
    5: "#14b8a6",  # teal   — Excellent
}

LEVEL_NAMES: Dict[int, str] = {
    1: "Nascent",
    2: "Developing",
    3: "Adequate",
    4: "Good",
    5: "Excellent",
}

STRENGTH_COLORS: Dict[str, str] = {
    "strong":   "#22c55e",
    "moderate": "#eab308",
    "weak":     "#ef4444",
}


# ---------------------------------------------------------------------------
# Single dimension card
# ---------------------------------------------------------------------------

def render_evidence_card(justification) -> None:
    """
    Render one dimension's evidence card.

    justification must expose:
        .dimension, .level, .score, .evidence_strength,
        .rubric_criteria, .supporting_evidence (list), .gaps_identified (list)
    Also handles plain dicts (from JSON-parsed agent output).
    """
    # Support both object attributes and dict keys
    def _get(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    level      = _get(justification, "level", 0)
    score      = _get(justification, "score", 0.0)
    dimension  = _get(justification, "dimension", "")
    strength   = _get(justification, "evidence_strength", "unknown")
    rubric     = _get(justification, "rubric_criteria", "")
    summary    = _get(justification, "generated_summary", None)
    evidence   = _get(justification, "supporting_evidence", [])
    gaps       = _get(justification, "gaps_identified", [])

    color      = LEVEL_COLORS.get(level, "#6b7280")
    level_name = LEVEL_NAMES.get(level, "Unknown")
    s_color    = STRENGTH_COLORS.get(strength, "#6b7280")

    with st.container():
        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            dim_label = str(dimension).replace("_", " ").title()
            st.markdown(f"### {dim_label}")

        with col2:
            st.markdown(
                f'<span style="background-color:{color};color:white;'
                f'padding:4px 12px;border-radius:12px;font-weight:bold;">'
                f"L{level} {level_name}</span>",
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(f"**{float(score):.1f}**")

        # Evidence strength
        st.markdown(
            f"Evidence strength: "
            f"<span style='color:{s_color};font-weight:bold;'>"
            f"{str(strength).title()}</span>",
            unsafe_allow_html=True,
        )

        # Rubric match
        if rubric:
            st.info(f"**Rubric Match:** {rubric}")

        # AI-generated IC summary (CS4 field)
        if summary:
            with st.expander("IC Summary (AI-generated)", expanded=False):
                st.write(summary)

        # Supporting evidence items
        if evidence:
            st.markdown("**Supporting Evidence:**")
            for i, ev in enumerate(evidence[:5], start=1):
                if isinstance(ev, dict):
                    src  = ev.get("source_type", "source")
                    text = ev.get("content", "")
                    conf = ev.get("confidence", 0.0)
                    url  = ev.get("source_url", None)
                else:
                    src  = getattr(ev, "source_type", "source")
                    text = getattr(ev, "content", "")
                    conf = getattr(ev, "confidence", 0.0)
                    url  = getattr(ev, "source_url", None)

                label = f"[{src}] {str(text)[:60]}..."
                with st.expander(label, expanded=False):
                    st.write(text)
                    st.caption(f"Confidence: {float(conf):.0%}")
                    if url:
                        st.markdown(f"[Source]({url})")

        # Gaps
        if gaps:
            st.warning("**Gaps Identified:**")
            for gap in gaps:
                st.markdown(f"- {gap}")

        st.divider()


# ---------------------------------------------------------------------------
# Full company evidence panel (7 dimensions, tabbed)
# ---------------------------------------------------------------------------

def render_company_evidence_panel(
    company_id: str,
    justifications: Dict[str, object],
) -> None:
    """
    Render the full evidence panel for a company.

    justifications: dict mapping dimension string → justification object or dict
    """
    st.header(f"🔍 Evidence Analysis: {company_id}")

    if not justifications:
        st.warning("No justifications available for this company.")
        return

    def _get(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    # Summary metrics
    total_evidence = sum(
        len(_get(j, "supporting_evidence") or []) for j in justifications.values()
    )
    avg_level = sum(
        _get(j, "level", 0) for j in justifications.values()
    ) / len(justifications)
    strong_count = sum(
        1 for j in justifications.values()
        if _get(j, "evidence_strength", "") == "strong"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Evidence Items", total_evidence)
    c2.metric("Avg Level",            f"L{avg_level:.1f}")
    c3.metric("Strong Evidence",      f"{strong_count}/{len(justifications)}")
    c4.metric("Dimensions",           len(justifications))

    # Tabbed view — one tab per dimension
    dim_keys   = list(justifications.keys())
    dim_labels = [d.replace("_", " ").title() for d in dim_keys]
    tabs       = st.tabs(dim_labels)

    for tab, dim in zip(tabs, dim_keys):
        with tab:
            render_evidence_card(justifications[dim])


# ---------------------------------------------------------------------------
# Compact summary table
# ---------------------------------------------------------------------------

def render_evidence_summary_table(
    justifications: Dict[str, object],
) -> None:
    """Render a colour-coded summary table across all dimensions."""
    if not justifications:
        st.info("No evidence data to display.")
        return

    def _get(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    rows = []
    for dim, just in justifications.items():
        level = _get(just, "level", 0)
        rows.append({
            "Dimension": dim.replace("_", " ").title(),
            "Score":     round(float(_get(just, "score", 0.0)), 1),
            "Level":     f"L{level}",
            "Evidence":  str(_get(just, "evidence_strength", "")).title(),
            "Items":     len(_get(just, "supporting_evidence") or []),
            "Gaps":      len(_get(just, "gaps_identified") or []),
        })

    df = pd.DataFrame(rows)

    def _color_level(val: str) -> str:
        try:
            lvl = int(val[1])
        except (IndexError, ValueError):
            return ""
        bg = LEVEL_COLORS.get(lvl, "#ffffff")
        return f"background-color:{bg};color:white;"

    styled = df.style.applymap(_color_level, subset=["Level"])
    st.dataframe(styled, use_container_width=True, hide_index=True)