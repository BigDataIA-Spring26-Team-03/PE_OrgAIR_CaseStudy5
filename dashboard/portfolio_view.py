"""
Shared Portfolio Dashboard UI (metrics, scatter, table, builder, evidence).

Used by `dashboard/app.py` and `streamlit_app/app.py` (CS5 page).
"""

from __future__ import annotations

import asyncio
import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from src.services.integration.portfolio_data_service import portfolio_data_service
from dashboard.components.evidence_display import (
    render_company_evidence_panel,
    render_evidence_summary_table,
)


@st.cache_data(ttl=300)
def load_portfolio_dataframe(_fund_id: str, _bust: int = 0):
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


def _get_cs4_justification_client():
    """
    Resolve the CS4 client without ``from ... import cs4_client``, which can fail with
    ``ImportError: cannot import name 'cs4_client'`` when the module is mid-import
    (circular import edge cases). Falls back to ``CS4Client()`` if the lazy singleton
    is not yet bound on the module object.
    """
    import importlib

    m = importlib.import_module("src.services.cs4_client")
    client = getattr(m, "cs4_client", None)
    if client is not None:
        return client
    return m.CS4Client()


def fetch_dimension_justifications(_ticker: str, _bust: int = 0) -> dict:
    from src.services.integration.cs3_client import Dimension

    cs4 = _get_cs4_justification_client()

    async def _fetch():
        results = {}
        for dim in Dimension:
            try:
                j = await cs4.generate_justification(_ticker, dim)
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


def render_portfolio_builder(
    api_base_url: str,
    fund_id: str,
    *,
    key_prefix: str = "",
) -> None:
    """Sidebar expander: load companies, multiselect, save/clear portfolio."""
    p = key_prefix.strip()
    pk = f"{p}_" if p else ""

    with st.sidebar.expander("🧺 Portfolio Builder", expanded=True):
        st.caption("Pick companies for this fund, then Generate.")

        all_companies: list = []
        try:
            r = requests.get(
                f"{api_base_url}/api/v1/companies",
                params={"limit": 500, "offset": 0},
                timeout=20,
            )
            r.raise_for_status()
            all_companies = r.json() or []
        except Exception as e:
            st.warning(f"Could not load companies list from API: {e}")

        current_tickers: list[str] = []
        try:
            r2 = requests.get(
                f"{api_base_url}/api/v1/portfolios/{fund_id}/companies",
                timeout=20,
            )
            if r2.status_code == 200:
                current_tickers = [
                    str(x.get("ticker") or "").upper()
                    for x in (r2.json() or [])
                    if x.get("ticker")
                ]
        except Exception:
            pass

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
            key=f"{pk}pb_companies_multiselect",
        )

        cols = st.columns(2)
        with cols[0]:
            if st.button("Generate / Save Portfolio", key=f"{pk}pb_save"):
                tickers = [option_map[p] for p in picked if p in option_map]
                if not tickers:
                    st.warning("Select at least 1 company.")
                else:
                    try:
                        rp = requests.post(
                            f"{api_base_url}/api/v1/portfolios/{fund_id}/companies",
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
            if st.button("Clear Portfolio", key=f"{pk}pb_clear"):
                try:
                    rp = requests.post(
                        f"{api_base_url}/api/v1/portfolios/{fund_id}/companies",
                        json={"tickers": []},
                        timeout=30,
                    )
                    rp.raise_for_status()
                    st.success("Cleared portfolio.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to clear portfolio: {e}")


def render_portfolio_dashboard_body(
    fund_id: str,
    show_evidence: bool,
    bust_key: int,
    *,
    main_title: str = "📊 Portfolio Dashboard",
    evidence_select_key: str = "portfolio_evidence_company",
    stop_on_error: bool = True,
    stop_on_empty: bool = True,
) -> bool:
    """
    Main-area portfolio dashboard: metrics, scatter, table, optional evidence.
    Returns True if the full dashboard was rendered; False if load failed or portfolio
    was empty and ``stop_on_*`` was False (caller may render more content below).
    """
    st.title(main_title)

    try:
        portfolio_df, companies = load_portfolio_dataframe(fund_id, bust_key)
    except Exception as e:
        st.error(f"Failed to load portfolio: {e}")
        st.info("Ensure FastAPI (8000) + Snowflake are configured.")
        if stop_on_error:
            st.stop()
        return False

    if not companies:
        st.warning(
            "No companies in this portfolio yet. Use Portfolio Builder to select companies."
        )
        if stop_on_empty:
            st.stop()
        return False

    avg_vr = float(portfolio_df["vr_score"].mean()) if not portfolio_df.empty else 0.0
    avg_delta = float(portfolio_df["delta"].mean()) if not portfolio_df.empty else 0.0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Fund-AI-R", f"{portfolio_df['org_air'].mean():.1f}")
    m2.metric("Companies", int(len(companies)))
    m3.metric("Avg V^R", f"{avg_vr:.1f}")
    m4.metric("Avg Delta", f"{avg_delta:+.1f}")

    st.markdown("---")

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

    st.subheader("Portfolio Companies")
    display_cols = ["ticker", "name", "org_air", "vr_score", "hr_score", "delta"]
    st.dataframe(
        portfolio_df[display_cols]
        .style.background_gradient(subset=["org_air"], cmap="RdYlGn", vmin=0, vmax=100)
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
        selected = st.selectbox("Company", options=tickers, index=0, key=evidence_select_key)

        justifications = fetch_dimension_justifications(selected, bust_key)
        render_evidence_summary_table(justifications)
        render_company_evidence_panel(selected, justifications)

    return True


def default_api_base_url() -> str:
    return os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
