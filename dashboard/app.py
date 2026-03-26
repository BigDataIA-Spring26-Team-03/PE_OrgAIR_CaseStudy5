"""
PE Org-AI-R Portfolio Dashboard.

Standalone entry point — can also be accessed via streamlit_app under
"📊 Portfolio Intelligence (CS5)".

Run:
    streamlit run dashboard/app.py
"""
import os
import sys

# Path fix so dashboard/ can import from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio

import pandas as pd
import plotly.express as px
import streamlit as st
import requests

from src.services.integration.portfolio_data_service import portfolio_data_service
from dashboard.components.evidence_display import (
    render_company_evidence_panel,
    render_evidence_summary_table,
)

st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📈",
    layout="wide",
)

st.sidebar.title("PE Org-AI-R")
st.sidebar.markdown("*Portfolio Dashboard*")
fund_id = st.sidebar.text_input("Fund ID", value="growth_fund_v")
show_evidence = st.sidebar.checkbox("Show Evidence Panel", value=False)
refresh = st.sidebar.button("🔄 Refresh Data")
bust_key = 1 if refresh else 0

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")

with st.sidebar.expander("🧺 Portfolio Builder", expanded=True):
    st.caption("Pick companies for this fund, then Generate.")

    # Load companies list
    all_companies = []
    try:
        r = requests.get(f"{API_BASE_URL}/api/v1/companies", params={"limit": 500, "offset": 0}, timeout=20)
        r.raise_for_status()
        all_companies = r.json() or []
    except Exception as e:
        st.warning(f"Could not load companies list from API: {e}")

    # Load current portfolio (if any)
    current_tickers: list[str] = []
    try:
        r2 = requests.get(f"{API_BASE_URL}/api/v1/portfolios/{fund_id}/companies", timeout=20)
        if r2.status_code == 200:
            current_tickers = [str(x.get("ticker") or "").upper() for x in (r2.json() or []) if x.get("ticker")]
    except Exception:
        pass

    # Options as "TICKER — Name"
    option_map: dict[str, str] = {}
    options: list[str] = []
    for c in all_companies:
        t = str(c.get("ticker") or "").upper().strip()
        n = str(c.get("name") or "").strip()
        if not t:
            continue
        label = f"{t} — {n}" if n else t
        options.append(label)
        option_map[label] = t

    default_labels = [lbl for lbl, t in option_map.items() if t in set(current_tickers)]
    picked = st.multiselect(
        "Companies",
        options=sorted(options),
        default=sorted(default_labels),
    )

    cols = st.columns(2)
    with cols[0]:
        if st.button("Generate / Save Portfolio"):
            tickers = [option_map[p] for p in picked if p in option_map]
            if not tickers:
                st.warning("Select at least 1 company.")
            else:
                try:
                    rp = requests.post(
                        f"{API_BASE_URL}/api/v1/portfolios/{fund_id}/companies",
                        json={"tickers": tickers},
                        timeout=30,
                    )
                    rp.raise_for_status()
                    st.success(f"Saved {len(tickers)} companies to fund '{fund_id}'.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save portfolio: {e}")
    with cols[1]:
        if st.button("Clear Portfolio"):
            try:
                rp = requests.post(
                    f"{API_BASE_URL}/api/v1/portfolios/{fund_id}/companies",
                    json={"tickers": []},
                    timeout=30,
                )
                rp.raise_for_status()
                st.success("Cleared portfolio.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to clear portfolio: {e}")

@st.cache_data(ttl=300)
def _load_portfolio(_fund_id: str, _bust: int = 0):
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
            "ticker": c.ticker,
            "name": c.name,
            "org_air": float(c.org_air),
            "vr_score": float(c.vr_score),
            "hr_score": float(c.hr_score),
            "delta": float(c.delta_since_entry),
        }
        for c in companies
    ]
    return pd.DataFrame(rows), companies


st.title("📊 Portfolio Dashboard")

try:
    portfolio_df, companies = _load_portfolio(fund_id, bust_key)
except Exception as e:
    st.error(f"Failed to load portfolio: {e}")
    st.info("Ensure FastAPI (8000) + Snowflake are configured.")
    st.stop()

if not companies:
    st.warning("No companies in this portfolio yet. Use Portfolio Builder to select companies.")
    st.stop()

# ---------------------------------------------------------------------
# Spec-required metrics row (4 columns)
# ---------------------------------------------------------------------
avg_vr = float(portfolio_df["vr_score"].mean()) if not portfolio_df.empty else 0.0
avg_delta = float(portfolio_df["delta"].mean()) if not portfolio_df.empty else 0.0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Fund-AI-R", f"{portfolio_df['org_air'].mean():.1f}")
m2.metric("Companies", int(len(companies)))
m3.metric("Avg V^R", f"{avg_vr:.1f}")
m4.metric("Avg Delta", f"{avg_delta:+.1f}")

st.markdown("---")

# ---------------------------------------------------------------------
# Spec-required scatter (V^R vs H^R)
# ---------------------------------------------------------------------
fig = px.scatter(
    portfolio_df,
    x="vr_score",
    y="hr_score",
    size="org_air",
    hover_name="name",
    hover_data={"ticker": True, "org_air": ":.1f", "delta": ":.1f"},
    title="Portfolio AI-Readiness Map (V^R vs H^R)",
    labels={
        "vr_score": "V^R (Idiosyncratic Readiness)",
        "hr_score": "H^R (Systematic Readiness)",
    },
    height=420,
)
fig.add_hline(y=60, line_dash="dash", line_color="gray")
fig.add_vline(x=60, line_dash="dash", line_color="gray")
st.plotly_chart(fig, width="stretch")

st.markdown("---")

# ---------------------------------------------------------------------
# Spec-required table with org_air gradient
# ---------------------------------------------------------------------
st.subheader("Portfolio Companies")
display_cols = ["ticker", "name", "org_air", "vr_score", "hr_score", "delta"]
st.dataframe(
    portfolio_df[display_cols].style
    .background_gradient(subset=["org_air"], cmap="RdYlGn", vmin=0, vmax=100)
    .format(
        {
            "org_air": "{:.1f}",
            "vr_score": "{:.1f}",
            "hr_score": "{:.1f}",
            "delta": "{:+.1f}",
        }
    ),
    use_container_width=True,
    hide_index=True,
)

if show_evidence:
    st.markdown("---")
    st.header("🔍 Evidence (CS4 Justifications)")

    tickers = [str(c.ticker).upper() for c in companies if getattr(c, "ticker", None)]
    selected = st.selectbox("Company", options=tickers, index=0)

    @st.cache_data(ttl=600)
    def _fetch_justifications(_ticker: str, _bust: int = 0) -> dict:
        from src.services.integration.cs3_client import Dimension
        from src.services.cs4_client import cs4_client

        async def _fetch():
            results = {}
            for dim in Dimension:
                try:
                    j = await cs4_client.generate_justification(_ticker, dim)
                    results[dim.value] = j
                except Exception:
                    pass
            return results

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_fetch())
        finally:
            loop.close()

    justifications = _fetch_justifications(selected, bust_key)
    render_evidence_summary_table(justifications)
    render_company_evidence_panel(selected, justifications)
