"""
PE Org-AI-R Portfolio Intelligence Dashboard (Task 9.6)

ALL data comes from CS1-CS4 via PortfolioDataService.
Run with:  streamlit run dashboard/app.py
"""
from __future__ import annotations

import asyncio
import sys
import os

import nest_asyncio
nest_asyncio.apply()

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Path fix so dashboard/ can import from src/
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.integration.portfolio_data_service import portfolio_data_service
from src.services.analytics.fund_air import fund_air_calculator
from src.services.tracking.assessment_history import assessment_history_service
from dashboard.components.evidence_display import render_evidence_summary_table

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="PE Org-AI-R Dashboard",
    page_icon="📈",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
st.sidebar.title("PE Org-AI-R")
st.sidebar.markdown("*Agentic Portfolio Intelligence*")
fund_id = st.sidebar.text_input("Fund ID", value="growth_fund_v")
show_evidence = st.sidebar.checkbox("Show Evidence Panel", value=False)
refresh = st.sidebar.button("🔄 Refresh Data")

# ---------------------------------------------------------------------------
# Data loading (cached 5 min, busted on manual refresh)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_portfolio(_fund_id: str, _bust: int = 0):
    """Load and score all portfolio companies via PortfolioDataService."""
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
            "ticker":          c.ticker,
            "name":            c.name,
            "sector":          c.sector,
            "org_air":         c.org_air,
            "vr_score":        c.vr_score,
            "hr_score":        c.hr_score,
            "synergy_score":   c.synergy_score,
            "delta":           c.delta_since_entry,
            "evidence_count":  c.evidence_count,
            "company_id":      c.company_id,
        }
        for c in companies
    ]
    return pd.DataFrame(rows), companies


bust_key = 1 if refresh else 0

try:
    portfolio_df, companies = load_portfolio(fund_id, bust_key)
    st.sidebar.success(f"✅ Loaded {len(portfolio_df)} companies")
except Exception as e:
    st.error(f"❌ Failed to connect to CS1-CS4: {e}")
    st.info("Ensure the scoring service is running (python -m app.main).")
    st.stop()

# ---------------------------------------------------------------------------
# Fund-AI-R metrics row
# ---------------------------------------------------------------------------
ev_map = {c.company_id: 100.0 for c in companies}  # equal weight
fund_metrics = fund_air_calculator.calculate_fund_metrics(fund_id, companies, ev_map)

st.title("📊 Portfolio Overview")

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Fund-AI-R",      f"{fund_metrics.fund_air:.1f}")
m2.metric("Companies",      fund_metrics.company_count)
m3.metric("AI Leaders ≥70", fund_metrics.ai_leaders_count)
m4.metric("AI Laggards <50",fund_metrics.ai_laggards_count)
m5.metric("Avg Δ Entry",    f"{fund_metrics.avg_delta_since_entry:+.1f}")
m6.metric("Sector HHI",     f"{fund_metrics.sector_hhi:.3f}")

st.markdown("---")

# ---------------------------------------------------------------------------
# V^R vs H^R scatter (the key CS5 required chart)
# ---------------------------------------------------------------------------
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
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    # Quartile distribution bar chart
    q_dist = fund_metrics.quartile_distribution
    q_df = pd.DataFrame(
        [{"Quartile": f"Q{q}", "Companies": cnt} for q, cnt in q_dist.items()]
    )
    fig2 = px.bar(
        q_df, x="Quartile", y="Companies",
        color="Quartile",
        color_discrete_map={"Q1": "#14b8a6", "Q2": "#22c55e",
                            "Q3": "#eab308", "Q4": "#ef4444"},
        title="Sector-Relative Quartile Distribution",
        height=420,
    )
    st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Org-AI-R bar chart
# ---------------------------------------------------------------------------
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
st.plotly_chart(fig3, use_container_width=True)

# ---------------------------------------------------------------------------
# Delta since entry chart
# ---------------------------------------------------------------------------
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
st.plotly_chart(fig4, use_container_width=True)

# ---------------------------------------------------------------------------
# Portfolio companies table
# ---------------------------------------------------------------------------
st.subheader("Portfolio Companies")

display_cols = ["ticker", "name", "sector", "org_air", "vr_score",
                "hr_score", "synergy_score", "delta", "evidence_count"]
st.dataframe(
    portfolio_df[display_cols].style.background_gradient(
        subset=["org_air"], cmap="RdYlGn", vmin=0, vmax=100
    ).format({
        "org_air":       "{:.1f}",
        "vr_score":      "{:.1f}",
        "hr_score":      "{:.1f}",
        "synergy_score": "{:.1f}",
        "delta":         "{:+.1f}",
    }),
    use_container_width=True,
    hide_index=True,
)

# ---------------------------------------------------------------------------
# Evidence panel (optional)
# ---------------------------------------------------------------------------
if show_evidence:
    st.markdown("---")
    selected_ticker = st.selectbox(
        "Select company for evidence breakdown",
        options=portfolio_df["ticker"].tolist(),
    )
    st.subheader(f"Evidence Summary — {selected_ticker}")
    st.info(
        "Full evidence cards require calling CS4 `generate_justification` per dimension. "
        "Enable via the LangGraph agent workflow (Task 10.4)."
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    "PE Org-AI-R Platform · CS5 Agentic Portfolio Intelligence · "
    "All data sourced from CS1-CS4 via PortfolioDataService"
)