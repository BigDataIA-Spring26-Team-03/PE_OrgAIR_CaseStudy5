"""
PE Org-AI-R Portfolio Intelligence Dashboard (Task 9.6).

Standalone entry point — can also be accessed via streamlit_app under
"📊 Portfolio Intelligence (CS5)".

Run:
    streamlit run dashboard/app.py
"""
import os
import sys

# Path fix so dashboard/ can import from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st

from dashboard.pages.portfolio_intelligence import run as run_portfolio_intelligence

st.set_page_config(
    page_title="PE Org-AI-R Dashboard",
    page_icon="📈",
    layout="wide",
)

st.sidebar.title("PE Org-AI-R")
st.sidebar.markdown("*Agentic Portfolio Intelligence — CS5*")
fund_id = st.sidebar.text_input("Fund ID", value="growth_fund_v")
show_evidence = st.sidebar.checkbox("Show Evidence Panel", value=False)
refresh = st.sidebar.button("🔄 Refresh Data")
bust_key = 1 if refresh else 0

run_portfolio_intelligence(fund_id, show_evidence, bust_key)
