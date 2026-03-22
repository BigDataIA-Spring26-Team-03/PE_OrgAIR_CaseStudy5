"""
Portfolio Intelligence page — Fund-AI-R, V^R vs H^R, evidence panel.
Reusable from streamlit_app or dashboard standalone.
"""
from __future__ import annotations

import asyncio

import nest_asyncio
try:
    nest_asyncio.apply()
except ValueError:
    pass

import pandas as pd
import plotly.express as px
import streamlit as st

from src.services.integration.portfolio_data_service import portfolio_data_service
from src.services.analytics.fund_air import fund_air_calculator
from src.services.integration.cs3_client import Dimension
from dashboard.components.evidence_display import (
    render_evidence_summary_table,
    render_company_evidence_panel,
)


@st.cache_data(ttl=300)
def load_portfolio(_fund_id: str, _bust: int = 0):
    """Load and score all portfolio companies via PortfolioDataService (CS1-CS4)."""
    async def _load():
        return await portfolio_data_service.get_portfolio_view(_fund_id)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        companies = loop.run_until_complete(_load())
    finally:
        loop.close()

    rows = [
        {
            "company_id": c.company_id,
            "ticker": c.ticker,
            "name": c.name,
            "sector": c.sector,
            "org_air": c.org_air,
            "vr_score": c.vr_score,
            "hr_score": c.hr_score,
            "synergy_score": c.synergy_score,
            "delta": c.delta_since_entry,
            "evidence_count": c.evidence_count,
        }
        for c in companies
    ]
    return pd.DataFrame(rows), companies


@st.cache_data(ttl=600)
def load_justifications(_ticker: str, _bust: int = 0) -> dict | None:
    """Fetch CS4 justifications for all 7 dimensions. Cached 10 min."""
    async def _fetch():
        from src.services.cs4_client import cs4_client
        results = {}
        for dim in Dimension:
            try:
                j = await cs4_client.generate_justification(_ticker, dim)
                results[dim.value] = j
            except Exception:
                pass
        return results if results else None

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_fetch())
    except Exception:
        return None
    finally:
        loop.close()


def run(fund_id: str, show_evidence: bool, bust_key: int) -> None:
    """
    Render the Portfolio Intelligence page.

    Parameters
    ----------
    fund_id : str
        Fund identifier for portfolio view.
    show_evidence : bool
        Whether to show the evidence analysis panel.
    bust_key : int
        Cache bust key (e.g. 1 when refresh clicked).
    """
    try:
        portfolio_df, companies = load_portfolio(fund_id, bust_key)
    except Exception as e:
        st.error(f"❌ Failed to connect to CS1-CS4: {e}")
        st.info("Ensure the FastAPI scoring service is running.")
        return

    if not companies:
        st.error("❌ No portfolio companies loaded. Check Snowflake/API connectivity.")
        st.info("Ensure the FastAPI scoring service is running and Snowflake is configured.")
        return

    st.sidebar.success(f"✅ Loaded {len(companies)} companies from CS1-CS4")

    ev_map = {c.company_id: 100.0 for c in companies}
    fund_metrics = fund_air_calculator.calculate_fund_metrics(fund_id, companies, ev_map)

    st.title("📊 Portfolio Overview")

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Fund-AI-R", f"{fund_metrics.fund_air:.1f}")
    m2.metric("Companies", fund_metrics.company_count)
    m3.metric("AI Leaders ≥70", fund_metrics.ai_leaders_count)
    m4.metric("AI Laggards <50", fund_metrics.ai_laggards_count)
    m5.metric("Avg Δ Entry", f"{fund_metrics.avg_delta_since_entry:+.1f}")
    m6.metric("Sector HHI", f"{fund_metrics.sector_hhi:.3f}")

    st.markdown("---")

    col_left, col_right = st.columns([3, 2])
    with col_left:
        fig = px.scatter(
            portfolio_df,
            x="vr_score",
            y="hr_score",
            size="org_air",
            color="sector",
            hover_name="name",
            hover_data={"ticker": True, "org_air": ":.1f", "delta": ":.1f"},
            title="Portfolio AI-Readiness Map  (data from CS3)",
            labels={
                "vr_score": "V^R  (Idiosyncratic Readiness)",
                "hr_score": "H^R  (Systematic Readiness)",
            },
            height=420,
        )
        fig.add_hline(y=60, line_dash="dash", line_color="gray",
                     annotation_text="H^R threshold", annotation_position="right")
        fig.add_vline(x=60, line_dash="dash", line_color="gray",
                     annotation_text="V^R threshold", annotation_position="top")
        st.plotly_chart(fig, width="stretch")

    with col_right:
        q_dist = fund_metrics.quartile_distribution
        q_df = pd.DataFrame(
            [{"Quartile": f"Q{q}", "Companies": cnt} for q, cnt in q_dist.items()]
        )
        fig2 = px.bar(
            q_df, x="Quartile", y="Companies",
            color="Quartile",
            color_discrete_map={
                "Q1": "#14b8a6", "Q2": "#22c55e",
                "Q3": "#eab308", "Q4": "#ef4444",
            },
            title="Sector-Relative Quartile Distribution",
            height=420,
        )
        st.plotly_chart(fig2, width="stretch")

    fig3 = px.bar(
        portfolio_df.sort_values("org_air", ascending=False),
        x="ticker", y="org_air",
        color="org_air",
        color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        text="org_air",
        title="Org-AI-R Scores by Company  (data from CS3)",
        labels={"org_air": "Org-AI-R", "ticker": "Company"},
        height=350,
    )
    fig3.update_traces(texttemplate="%{text:.1f}", textposition="outside")
    fig3.add_hline(y=60, line_dash="dot", line_color="gray",
                   annotation_text="Minimum threshold")
    st.plotly_chart(fig3, width="stretch")

    fig4 = px.bar(
        portfolio_df.sort_values("delta"),
        x="ticker", y="delta",
        color="delta",
        color_continuous_scale="RdYlGn",
        title="Delta Since Entry  (current Org-AI-R − entry Org-AI-R)",
        labels={"delta": "Δ Org-AI-R", "ticker": "Company"},
        height=320,
    )
    fig4.add_hline(y=0, line_color="gray")
    st.plotly_chart(fig4, width="stretch")

    st.subheader("Portfolio Companies")
    display_cols = [
        "ticker", "name", "sector", "org_air",
        "vr_score", "hr_score", "synergy_score", "delta", "evidence_count",
    ]
    st.dataframe(
        portfolio_df[display_cols].style
        .background_gradient(subset=["org_air"], cmap="RdYlGn", vmin=0, vmax=100)
        .format({
            "org_air": "{:.1f}",
            "vr_score": "{:.1f}",
            "hr_score": "{:.1f}",
            "synergy_score": "{:.1f}",
            "delta": "{:+.1f}",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Inline toggle (also in sidebar) — always visible in main content
    st.markdown("---")
    show_evidence = st.checkbox(
        "📋 Show Evidence Panel — load CS4 justifications per dimension",
        value=show_evidence,
        key="portfolio_show_evidence_inline",
    )

    if show_evidence:
        st.markdown("---")
        st.subheader("🔍 Evidence Analysis")
        selected_ticker = st.selectbox(
            "Select company for evidence breakdown",
            options=portfolio_df["ticker"].tolist(),
            key="evidence_ticker",
        )
        load_btn = st.button("📥 Load Evidence", type="primary", key="load_evidence_btn")
        if load_btn or st.session_state.get("evidence_loaded_for") == selected_ticker:
            with st.spinner(f"Generating justifications for {selected_ticker} (7 dimensions)..."):
                justifications = load_justifications(selected_ticker, bust_key)
            if justifications:
                st.session_state["evidence_loaded_for"] = selected_ticker
                st.session_state["evidence_data"] = justifications
            else:
                st.error(
                    f"No justifications for {selected_ticker}. "
                    "Ensure the company is scored (CS3) and evidence is indexed (Chromadb)."
                )
        if st.session_state.get("evidence_data") and st.session_state.get("evidence_loaded_for") == selected_ticker:
            justifications = st.session_state["evidence_data"]
            st.success(f"Loaded {len(justifications)}/7 dimensions")
            render_evidence_summary_table(justifications)
            st.markdown("---")
            render_company_evidence_panel(selected_ticker, justifications)

    st.markdown("---")
    st.caption(
        "PE Org-AI-R Platform · CS5 Agentic Portfolio Intelligence · "
        "All data sourced from CS1-CS4 via PortfolioDataService"
    )
