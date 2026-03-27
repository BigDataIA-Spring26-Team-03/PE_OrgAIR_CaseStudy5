"""
PE Org-AI-R Portfolio Dashboard.

Standalone entry point — same UI is embedded in streamlit_app under
"📊 Portfolio Intelligence (CS5)".

Run:
    streamlit run dashboard/app.py
"""
import os
import sys

# Path fix so dashboard/ can import from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st

from dashboard.portfolio_view import (
    default_api_base_url,
    render_portfolio_builder,
    render_portfolio_dashboard_body,
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

API_BASE_URL = default_api_base_url()

render_portfolio_builder(API_BASE_URL, fund_id, key_prefix="standalone")

render_portfolio_dashboard_body(
    fund_id,
    show_evidence,
    bust_key,
    evidence_select_key="standalone_evidence_company",
)
