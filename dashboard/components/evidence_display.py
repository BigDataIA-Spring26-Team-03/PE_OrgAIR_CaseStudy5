"""Evidence Display Component — Streamlit UI for CS4 justifications (Task 9.5)."""
from __future__ import annotations

from typing import Dict, List

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Colour / label constants
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
        .rubric_criteria, .supporting_evidence (list),
        .gaps_identified (list)
    """
    level = getattr(justification, "level", 0)
    color = LEVEL_COLORS.get(level, "#6b7280")
    level_name = LEVEL_NAMES.get(level, "Unknown")

    with st.container():
        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            dim_label = str(justification.dimension).replace("_", " ").title()
            st.markdown(f"### {dim_label}")

        with col2:
            st.markdown(
                f'<span style="background-color:{color};color:white;'
                f'padding:4px 12px;border-radius:12px;font-weight:bold;">'
                f"L{level} {level_name}</span>",
                unsafe_allow_html=True,
            )

        with col3:
            score = getattr(justification, "score", 0.0)
            st.markdown(f"**{score:.1f}**")

        # Evidence strength badge
        strength = getattr(justification, "evidence_strength", "unknown")
        s_color = STRENGTH_COLORS.get(strength, "#6b7280")
        st.markdown(
            f"Evidence strength: "
            f"<span style='color:{s_color};font-weight:bold;'>"
            f"{strength.title()}</span>",
            unsafe_allow_html=True,
        )

        # Rubric match
        rubric = getattr(justification, "rubric_criteria", "")
        if rubric:
            st.info(f"**Rubric Match:** {rubric}")

        # Generated LLM summary (CS4 specific field)
        summary = getattr(justification, "generated_summary", None)
        if summary:
            with st.expander("IC Summary (AI-generated)", expanded=False):
                st.write(summary)

        # Supporting evidence items (from CS4 RAG)
        evidence_items = getattr(justification, "supporting_evidence", [])
        if evidence_items:
            st.markdown("**Supporting Evidence:**")
            for i, ev in enumerate(evidence_items[:5], start=1):
                content = getattr(ev, "content", str(ev))
                src_type = getattr(ev, "source_type", "source")
                confidence = getattr(ev, "confidence", 0.0)
                label = f"[{src_type}] {content[:60]}..."
                with st.expander(label, expanded=False):
                    st.write(content)
                    st.caption(f"Confidence: {confidence:.0%}")
                    src_url = getattr(ev, "source_url", None)
                    if src_url:
                        st.markdown(f"[Source]({src_url})")

        # Gaps
        gaps = getattr(justification, "gaps_identified", [])
        if gaps:
            st.warning("**Gaps Identified:**")
            for gap in gaps:
                st.markdown(f"- {gap}")

        st.divider()


# ---------------------------------------------------------------------------
# Full company evidence panel
# ---------------------------------------------------------------------------

def render_company_evidence_panel(
    company_id: str,
    justifications: Dict[str, object],
) -> None:
    """
    Render the full evidence panel for a company.

    justifications: dict mapping dimension string → justification object
    """
    st.header(f"🔍 Evidence Analysis: {company_id}")

    if not justifications:
        st.warning("No justifications available for this company.")
        return

    # Summary metrics row
    total_evidence = sum(
        len(getattr(j, "supporting_evidence", [])) for j in justifications.values()
    )
    avg_level = sum(
        getattr(j, "level", 0) for j in justifications.values()
    ) / len(justifications)
    strong_count = sum(
        1 for j in justifications.values()
        if getattr(j, "evidence_strength", "") == "strong"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Evidence Items", total_evidence)
    c2.metric("Avg Level", f"L{avg_level:.1f}")
    c3.metric("Strong Evidence", f"{strong_count}/{len(justifications)}")
    c4.metric("Dimensions", len(justifications))

    # Dimension tabs
    dim_keys = list(justifications.keys())
    dim_labels = [d.replace("_", " ").title() for d in dim_keys]
    tabs = st.tabs(dim_labels)

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

    rows = []
    for dim, just in justifications.items():
        level = getattr(just, "level", 0)
        rows.append({
            "Dimension":  dim.replace("_", " ").title(),
            "Score":      getattr(just, "score", 0.0),
            "Level":      f"L{level}",
            "Evidence":   getattr(just, "evidence_strength", "").title(),
            "Items":      len(getattr(just, "supporting_evidence", [])),
            "Gaps":       len(getattr(just, "gaps_identified", [])),
        })

    df = pd.DataFrame(rows)

    def _color_level(val: str):
        try:
            lvl = int(val[1])
        except (IndexError, ValueError):
            return ""
        bg = LEVEL_COLORS.get(lvl, "#ffffff")
        return f"background-color:{bg};color:white;"

    styled = df.style.applymap(_color_level, subset=["Level"])
    st.dataframe(styled, use_container_width=True, hide_index=True)