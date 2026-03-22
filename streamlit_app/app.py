# streamlit_app/app.py
"""
PE Org-AI-R Platform - Complete Dashboard
CS1: Platform Foundation + CS2: Evidence Collection

Features:
- CS1: Companies, Assessments, Dimension Scores
- CS2: Signal Collection, Patent Analytics, SEC Documents
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from api_client import APIClient
from datetime import datetime
import json

# Page config
st.set_page_config(
    page_title="PE Org-AI-R Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 20px;
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 30px;
    }
    .metric-card {
        padding: 20px;
        border-radius: 10px;
        background-color: #f0f2f6;
    }
    .score-high {
        color: #28a745;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .score-medium {
        color: #ffc107;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .score-low {
        color: #dc3545;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .status-badge {
        padding: 5px 10px;
        border-radius: 5px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Initialize API client
@st.cache_resource
def get_api_client():
    return APIClient()

api = get_api_client()

# Sidebar
st.sidebar.markdown("# 🏢 PE Org-AI-R")
st.sidebar.markdown("**AI-Readiness Assessment Platform**")
st.sidebar.markdown("---")

# Navigation
page = st.sidebar.radio(
    "📍 Navigation",
    [
        "🏠 Dashboard",
        "🏢 Companies",
        "📋 Assessments",
        "📊 Dimension Scores",
        "🎯 Signal Collection",
        "📈 Signal Analytics",
        "🔬 Patent Deep Dive",
        "📄 SEC Documents",
        "🚀 Score Company (CS3)",      # ← ADD THIS
        "⭐ CS3: Org-AI-R Results",
        "🔧 System Health",
        "🔍 Evidence Search",
        "📝 Score Justification",
        "🗒️ Analyst Notes"
    ]
)
st.sidebar.markdown("---")
st.sidebar.caption("**Case Study 1:** Platform Foundation ✅")
st.sidebar.caption("**Case Study 2:** Evidence Collection ✅")
st.sidebar.caption("Built with FastAPI + Snowflake + USPTO")

# ============================================
# 🏠 DASHBOARD (Enhanced with CS2)
# ============================================
if page == "🏠 Dashboard":
    st.markdown('<p class="main-header">📊 Platform Dashboard</p>', unsafe_allow_html=True)
    
    try:
        # Get data - each call wrapped so one failure doesn't break the whole dashboard
        try:
            companies = api.list_companies(limit=100)
        except Exception as e:
            st.warning(f"Could not load companies: {e}")
            companies = []

        try:
            assessments = api.list_assessments(limit=100)
        except Exception as e:
            st.warning(f"Could not load assessments: {e}")
            assessments = []

        # Try to get CS2 signals
        try:
            signals_data = api.get_all_signal_summaries()
            signals_available = True
        except Exception as e:
            st.warning(f"Could not load signal summaries: {e}")
            signals_available = False
            signals_data = {"count": 0, "summaries": []}
        
        # Top metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Companies", len(companies))
        
        with col2:
            st.metric("📋 Assessments", len(assessments))
        
        with col3:
            if signals_available:
                st.metric("🎯 Companies with Signals", signals_data.get('count', 0))
            else:
                st.metric("📝 Draft Assessments", sum(1 for a in assessments if a['status'] == 'draft'))
        
        with col4:
            if signals_available and signals_data.get('summaries'):
                avg = sum(s['composite_score'] for s in signals_data['summaries']) / len(signals_data['summaries'])
                st.metric("📈 Avg Composite Score", f"{avg:.1f}/100")
            else:
                st.metric("✅ Approved", sum(1 for a in assessments if a['status'] == 'approved'))
        
        st.markdown("---")
        
        # CS2 Top Performers
        if signals_available and signals_data.get('summaries'):
            st.markdown('<p class="sub-header">🏆 Top Performers (Composite Score)</p>', unsafe_allow_html=True)
            
            top_companies = sorted(
                signals_data['summaries'],
                key=lambda x: x['composite_score'],
                reverse=True
            )[:5]
            
            for i, comp in enumerate(top_companies, 1):
                col1, col2, col3, col4 = st.columns([0.5, 3, 2, 1.5])
                
                with col1:
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"#{i}"
                    st.markdown(f"### {medal}")
                
                with col2:
                    st.markdown(f"**{comp['ticker']}** - {comp['company_name']}")
                
                with col3:
                    # Mini progress bars for each category
                    st.caption(f"Jobs: {comp.get('jobs_score', 0)}/100")
                    st.progress(comp.get('jobs_score', 0) / 100)
                
                with col4:
                    score = comp['composite_score']
                    color_class = "score-high" if score >= 70 else "score-medium" if score >= 40 else "score-low"
                    st.markdown(f'<div class="{color_class}">{score}/100</div>', unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Companies by Industry")
            if companies:
                try:
                    industries = api.get_industries()
                    industry_map = {str(ind['id']): ind['name'] for ind in industries}
                except:
                    industry_map = {}
                
                industry_counts = {}
                for company in companies:
                    ind_id = str(company['industry_id'])
                    ind_name = industry_map.get(ind_id, ind_id[:8])
                    industry_counts[ind_name] = industry_counts.get(ind_name, 0) + 1
                
                fig = px.bar(
                    x=list(industry_counts.keys()),
                    y=list(industry_counts.values()),
                    labels={'x': 'Industry', 'y': 'Count'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Assessments by Status")
            if assessments:
                status_counts = {}
                for a in assessments:
                    status_counts[a['status']] = status_counts.get(a['status'], 0) + 1
                
                fig = px.pie(
                    values=list(status_counts.values()),
                    names=list(status_counts.keys())
                )
                st.plotly_chart(fig, use_container_width=True)
    
    except Exception as e:
        st.error(f"Error loading dashboard: {e}")
        st.info("Make sure FastAPI is running: `poetry run uvicorn app.main:create_app --factory --reload`")

# ============================================
# 🏢 COMPANIES (CS1)
# ============================================
elif page == "🏢 Companies":
    st.markdown('<p class="main-header">🏢 Companies Management</p>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📋 List Companies", "➕ Create Company", "✏️ Update Company"])
    
    with tab1:
        st.markdown("### All Companies")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            search = st.text_input("🔍 Search", placeholder="Name or ticker...")
        with col2:
            limit = st.number_input("Per page", 5, 100, 20)
        
        try:
            companies = api.list_companies(limit=limit)
            
            if search:
                companies = [
                    c for c in companies 
                    if search.lower() in c['name'].lower() 
                    or (c.get('ticker') and search.lower() in c['ticker'].lower())
                ]
            
            if companies:
                df = pd.DataFrame(companies)
                df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
                
                display_cols = ['name', 'ticker', 'position_factor', 'created_at']
                st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
                
                st.success(f"Showing {len(companies)} companies")
                
                # Delete
                st.markdown("#### 🗑️ Delete Company")
                to_delete = st.selectbox(
                    "Select company",
                    options=[(c['name'], c['id']) for c in companies],
                    format_func=lambda x: x[0]
                )
                
                if st.button("⚠️ Delete", type="secondary"):
                    if st.session_state.get('confirm_delete'):
                        try:
                            api.delete_company(str(to_delete[1]))
                            st.success(f"✅ Deleted {to_delete[0]}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                        st.session_state.confirm_delete = False
                    else:
                        st.session_state.confirm_delete = True
                        st.warning("Click again to confirm!")
            else:
                st.info("No companies found")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab2:
        st.markdown("### Create New Company")
        
        try:
            industries = api.get_industries()
            industry_opts = {f"{i['name']} ({i['sector']})": i['id'] for i in industries}
            
            with st.form("create_company", clear_on_submit=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    name = st.text_input("Company Name *", placeholder="Apple Inc")
                    industry = st.selectbox("Industry *", list(industry_opts.keys()))
                
                with col2:
                    ticker = st.text_input("Ticker", placeholder="AAPL", max_chars=10)
                    position = st.slider("Position Factor", -1.0, 1.0, 0.0, 0.1)
                
                if st.form_submit_button("✨ Create", use_container_width=True):
                    if name:
                        try:
                            result = api.create_company({
                                "name": name,
                                "ticker": ticker or None,
                                "industry_id": industry_opts[industry],
                                "position_factor": position
                            })
                            st.success(f"✅ Created {name}!")
                            st.balloons()
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        st.error("Name is required!")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab3:
        st.markdown("### Update Company")
        
        try:
            companies = api.list_companies(limit=100)
            
            if companies:
                selected = st.selectbox(
                    "Select Company",
                    [(c['name'], c['id']) for c in companies],
                    format_func=lambda x: x[0]
                )
                
                current = api.get_company(str(selected[1]))
                industries = api.get_industries()
                industry_opts = {f"{i['name']} ({i['sector']})": i['id'] for i in industries}
                
                current_ind = None
                for name, iid in industry_opts.items():
                    if str(iid) == str(current['industry_id']):
                        current_ind = name
                        break
                
                with st.form("update_company"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        name = st.text_input("Name", value=current['name'])
                        industry = st.selectbox("Industry", list(industry_opts.keys()), 
                                              index=list(industry_opts.keys()).index(current_ind) if current_ind else 0)
                    
                    with col2:
                        ticker = st.text_input("Ticker", value=current.get('ticker', ''))
                        position = st.slider("Position", -1.0, 1.0, float(current['position_factor']), 0.1)
                    
                    if st.form_submit_button("💾 Update"):
                        try:
                            api.update_company(str(selected[1]), {
                                "name": name,
                                "ticker": ticker or None,
                                "industry_id": industry_opts[industry],
                                "position_factor": position
                            })
                            st.success("✅ Updated!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
        
        except Exception as e:
            st.error(f"Error: {e}")

# ============================================
# 📋 ASSESSMENTS (CS1)
# ============================================
elif page == "📋 Assessments":
    st.markdown('<p class="main-header">📋 Assessments</p>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📋 List", "➕ Create"])
    
    with tab1:
        st.markdown("### All Assessments")
        
        try:
            companies_list = api.list_companies(limit=100)
            
            filter_comp = st.selectbox(
                "Filter by Company",
                [("All", None)] + [(c['name'], c['id']) for c in companies_list],
                format_func=lambda x: x[0]
            )
            
            assessments = api.list_assessments(
                limit=50,
                company_id=str(filter_comp[1]) if filter_comp[1] else None
            )
            
            if assessments:
                for a in assessments:
                    try:
                        comp = api.get_company(str(a['company_id']))
                        comp_name = comp['name']
                    except:
                        comp_name = str(a['company_id'])[:8]
                    
                    status_emoji = {
                        'draft': '🟡', 'in_progress': '🔵',
                        'submitted': '🟣', 'approved': '🟢'
                    }.get(a['status'], '⚪')
                    
                    with st.expander(f"{status_emoji} {comp_name} - {a['assessment_type']}"):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.write(f"**Type:** {a['assessment_type']}")
                            st.write(f"**Status:** {a['status']}")
                        
                        with col2:
                            st.write(f"**Primary:** {a.get('primary_assessor', 'N/A')}")
                            st.write(f"**Secondary:** {a.get('secondary_assessor', 'N/A')}")
                        
                        with col3:
                            vr = a.get('vr_score')
                            if vr:
                                st.metric("VR Score", f"{vr:.1f}")
                            st.caption(f"Created: {a['created_at'][:10]}")
                        
                        # Update status
                        new_status = st.selectbox(
                            "Update Status",
                            ['draft', 'in_progress', 'submitted', 'approved'],
                            index=['draft', 'in_progress', 'submitted', 'approved'].index(a['status']),
                            key=f"s_{a['id']}"
                        )
                        
                        if st.button("Update", key=f"b_{a['id']}"):
                            if new_status != a['status']:
                                try:
                                    api.update_assessment_status(str(a['id']), new_status)
                                    st.success(f"✅ Updated to {new_status}!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab2:
        st.markdown("### Create Assessment")
        
        try:
            companies_list = api.list_companies(limit=100)
            
            if not companies_list:
                st.warning("Create companies first!")
            else:
                comp_opts = {f"{c['name']} ({c.get('ticker', 'N/A')})": c['id'] for c in companies_list}
                
                with st.form("create_assess", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        comp = st.selectbox("Company *", list(comp_opts.keys()))
                        type_ = st.selectbox("Type *", ['screening', 'due_diligence', 'quarterly', 'exit_prep'],
                                           format_func=lambda x: x.replace('_', ' ').title())
                    
                    with col2:
                        primary = st.text_input("Primary Assessor *")
                        secondary = st.text_input("Secondary Assessor")
                    
                    if st.form_submit_button("✨ Create", use_container_width=True):
                        if primary:
                            try:
                                result = api.create_assessment({
                                    "company_id": comp_opts[comp],
                                    "assessment_type": type_,
                                    "primary_assessor": primary,
                                    "secondary_assessor": secondary or None
                                })
                                st.success("✅ Assessment created!")
                                st.info(f"ID: {result['id']}")
                                st.balloons()
                            except Exception as e:
                                st.error(f"Error: {e}")
                        else:
                            st.error("Primary assessor required!")
        
        except Exception as e:
            st.error(f"Error: {e}")

# ============================================
# 📊 DIMENSION SCORES (CS1)
# ============================================
elif page == "📊 Dimension Scores":
    st.markdown('<p class="main-header">📊 Dimension Scores</p>', unsafe_allow_html=True)
    
    try:
        assessments = api.list_assessments(limit=100)
        
        if not assessments:
            st.warning("Create assessments first!")
        else:
            # Select assessment
            assess_opts = []
            for a in assessments:
                try:
                    comp = api.get_company(str(a['company_id']))
                    label = f"{comp['name']} - {a['assessment_type']} ({a['status']})"
                except:
                    label = f"{a['assessment_type']} ({a['status']})"
                assess_opts.append((label, a['id']))
            
            selected = st.selectbox("Select Assessment", assess_opts, format_func=lambda x: x[0])
            assess_id = str(selected[1])
            
            tab1, tab2 = st.tabs(["📈 View Scores", "➕ Add Score"])
            
            with tab1:
                try:
                    scores = api.get_dimension_scores(assess_id)
                    
                    if scores:
                        # Metrics
                        total_weighted = sum(s['score'] * s['weight'] for s in scores)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Dimensions", len(scores))
                        with col2:
                            avg = sum(s['score'] for s in scores) / len(scores)
                            st.metric("Average", f"{avg:.1f}")
                        with col3:
                            st.metric("**Weighted Score**", f"{total_weighted:.1f}/100")
                        
                        # Table
                        df = pd.DataFrame(scores)
                        df['dimension'] = df['dimension'].str.replace('_', ' ').str.title()
                        df['weight'] = (df['weight'] * 100).round(0).astype(int).astype(str) + '%'
                        
                        st.dataframe(
                            df[['dimension', 'score', 'weight', 'confidence', 'evidence_count']],
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Radar chart
                        fig = go.Figure()
                        fig.add_trace(go.Scatterpolar(
                            r=[s['score'] for s in scores],
                            theta=[s['dimension'].replace('_', ' ').title() for s in scores],
                            fill='toself'
                        ))
                        fig.update_layout(
                            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No scores yet!")
                
                except:
                    st.info("No scores yet!")
            
            with tab2:
                st.markdown("### Add Score")
                
                try:
                    existing = api.get_dimension_scores(assess_id)
                    existing_dims = [s['dimension'] for s in existing]
                except:
                    existing_dims = []
                
                all_dims = [
                    ("data_infrastructure", "Data Infrastructure", 0.25),
                    ("ai_governance", "AI Governance", 0.20),
                    ("technology_stack", "Technology Stack", 0.15),
                    ("talent_skills", "Talent & Skills", 0.15),
                    ("leadership_vision", "Leadership & Vision", 0.10),
                    ("use_case_portfolio", "Use Case Portfolio", 0.10),
                    ("culture_change", "Culture & Change", 0.05),
                ]
                
                available = [d for d in all_dims if d[0] not in existing_dims]
                
                if not available:
                    st.success("✅ All 7 dimensions scored!")
                else:
                    with st.form("add_score", clear_on_submit=True):
                        dim = st.selectbox(
                            "Dimension",
                            available,
                            format_func=lambda x: f"{x[1]} (Weight: {int(x[2]*100)}%)"
                        )
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            score = st.slider("Score", 0, 100, 75)
                            conf = st.slider("Confidence", 0.0, 1.0, 0.8, 0.05)
                        with col2:
                            evidence = st.number_input("Evidence Count", 0, value=5)
                        
                        if st.form_submit_button("✨ Add", use_container_width=True):
                            try:
                                api.create_dimension_score(assess_id, {
                                    "assessment_id": assess_id,
                                    "dimension": dim[0],
                                    "score": score,
                                    "confidence": conf,
                                    "evidence_count": evidence
                                })
                                st.success(f"✅ {dim[1]} score added!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 🎯 SIGNAL COLLECTION (CS2 - NEW!)
# ============================================
elif page == "🎯 Signal Collection":
    st.markdown('<p class="main-header">🎯 External Signal Collection</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Collect external signals to measure the **Say-Do Gap**:
    - 🔵 **Jobs** (30%) - AI/ML hiring activity  
    - 🟢 **Tech** (25%) - Technology stack maturity
    - 🟣 **Patents** (25%) - Innovation activity
    - 🟠 **Leadership** (20%) - Executive AI expertise
    """)
    
    st.markdown("---")
    
    try:
        companies = api.list_companies(limit=100)
        
        if not companies:
            st.warning("Add companies first!")
        else:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                comp_choice = st.selectbox(
                    "Select Company",
                    [(f"{c['ticker']} - {c['name']}", c['ticker']) for c in companies if c.get('ticker')],
                    format_func=lambda x: x[0]
                )
                ticker = comp_choice[1]
            
            with col2:
                years = st.number_input("Years (Patents)", 1, 10, 5)
            
            location = st.text_input("Job Location", "United States")
            
            st.markdown("---")
            
            # Collection buttons
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("🚀 Collect ALL Signals", type="primary", use_container_width=True):
                    with st.spinner(f"Collecting signals for {ticker}..."):
                        try:
                            result = api.collect_all_signals(ticker, years, location)
                            st.success(f"✅ Collection started for {ticker}!")
                            st.info("Takes 30-60 seconds. Check Signal Analytics for results.")
                            with st.expander("Response"):
                                st.json(result)
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col2:
                if st.button("🔬 Patents Only", use_container_width=True):
                    with st.spinner("Collecting patents..."):
                        try:
                            result = api.collect_patents_only(ticker, years)
                            st.success("✅ Patent collection started!")
                            with st.expander("Response"):
                                st.json(result)
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col3:
                if st.button("🔄 Refresh", use_container_width=True):
                    st.rerun()
            
            # Current status
            st.markdown("---")
            st.markdown("### Current Status")
            
            try:
                summary = api.get_signal_summary(ticker)
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Jobs", f"{summary.get('jobs_score', 0)}/100")
                with col2:
                    st.metric("Tech", f"{summary.get('tech_score', 0)}/100")
                with col3:
                    st.metric("Patents", f"{summary.get('patents_score', 0)}/100")
                with col4:
                    st.metric("Leadership", f"{summary.get('leadership_score', 0)}/100")
                with col5:
                    st.metric("**Composite**", f"{summary.get('composite_score', 0)}/100")
                
                st.caption(f"Last updated: {summary.get('last_updated', 'Never')}")
            
            except:
                st.info(f"No signals yet for {ticker}. Click 'Collect ALL Signals' above!")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 📈 SIGNAL ANALYTICS (CS2 - NEW!)
# ============================================
elif page == "📈 Signal Analytics":
    st.markdown('<p class="main-header">📈 Signal Analytics</p>', unsafe_allow_html=True)
    
    try:
        data = api.get_all_signal_summaries()
        summaries = data.get('summaries', [])
        
        if not summaries:
            st.warning("No signal data! Go to Signal Collection first.")
        else:
            # Metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Companies Analyzed", len(summaries))
            with col2:
                avg = sum(s['composite_score'] for s in summaries) / len(summaries)
                st.metric("Avg Composite", f"{avg:.1f}/100")
            with col3:
                total = sum(s.get('signal_count', 0) for s in summaries)
                st.metric("Total Signals", total)
            with col4:
                top = max(summaries, key=lambda x: x['composite_score'])
                st.metric("Top Performer", f"{top['ticker']} ({top['composite_score']})")
            
            st.markdown("---")
            
            # Comparison table
            st.markdown("### Company Comparison")
            
            df = pd.DataFrame(summaries)
            df = df.rename(columns={
                'ticker': 'Ticker',
                'company_name': 'Company',
                'jobs_score': 'Jobs',
                'patents_score': 'Patents',
                'tech_score': 'Tech',
                'leadership_score': 'Leadership',
                'composite_score': 'Composite'
            })
            
            display = df[['Ticker', 'Company', 'Jobs', 'Patents', 'Tech', 'Leadership', 'Composite']].copy()
            
            # Color coding
            def color_score(val):
                if pd.isna(val):
                    return ''
                color = '#28a745' if val >= 70 else '#ffc107' if val >= 40 else '#dc3545'
                return f'background-color: {color}; color: white;'
            
            styled = display.style.applymap(
                color_score,
                subset=['Jobs', 'Patents', 'Tech', 'Leadership', 'Composite']
            )
            
            st.dataframe(styled, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Signal Breakdown")
                
                fig = go.Figure()
                fig.add_trace(go.Bar(name='Jobs (30%)', x=df['Ticker'], y=df['Jobs'], marker_color='#3498db'))
                fig.add_trace(go.Bar(name='Patents (25%)', x=df['Ticker'], y=df['Patents'], marker_color='#9b59b6'))
                fig.add_trace(go.Bar(name='Tech (25%)', x=df['Ticker'], y=df['Tech'], marker_color='#2ecc71'))
                fig.add_trace(go.Bar(name='Leadership (20%)', x=df['Ticker'], y=df['Leadership'], marker_color='#e74c3c'))
                
                fig.update_layout(barmode='group', yaxis_title="Score")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### Composite Distribution")
                
                fig = px.scatter(
                    df, x='Patents', y='Jobs', size='Composite', color='Composite',
                    hover_data=['Ticker', 'Company'],
                    color_continuous_scale='RdYlGn',
                    labels={'Patents': 'Innovation', 'Jobs': 'Hiring'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Say-Do Gap
            st.markdown("---")
            st.markdown("### 🎯 Say-Do Gap Analysis")
            
            df['Gap'] = abs(df['Patents'] - df['Jobs'])
            top_gaps = df.nlargest(8, 'Gap')
            
            fig = px.bar(
                top_gaps, x='Ticker', y='Gap',
                color='Gap', color_continuous_scale='Reds',
                title="Largest Say-Do Gaps"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.info("💡 **High gap** = Companies innovate (patents) but don't hire (jobs). Might be outsourcing or overstating AI.")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 🔬 PATENT DEEP DIVE (CS2 - NEW!)
# ============================================
elif page == "🔬 Patent Deep Dive":
    st.markdown('<p class="main-header">🔬 Patent Deep Dive</p>', unsafe_allow_html=True)
    
    try:
        data = api.get_all_signal_summaries()
        summaries = data.get('summaries', [])
        
        if not summaries:
            st.warning("Collect signals first!")
        else:
            # Company selector
            comp_choice = st.selectbox(
                "Select Company",
                [(f"{s['ticker']} - {s['company_name']} (Score: {s['patents_score']}/100)", s['ticker']) for s in summaries],
                format_func=lambda x: x[0]
            )
            ticker = comp_choice[1]
            
            try:
                signals = api.get_signals_by_ticker(ticker)
                patent_sigs = [s for s in signals.get('signals', []) 
                              if 'innovation' in s.get('category', '').lower() or 'patent' in s.get('category', '').lower()]
                
                if patent_sigs:
                    sig = patent_sigs[0]
                    meta = json.loads(sig.get('metadata', '{}')) if isinstance(sig.get('metadata'), str) else sig.get('metadata', {})
                    
                    # Key metrics
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Patents", meta.get('total_patents', 0))
                    with col2:
                        st.metric("AI Patents", meta.get('ai_patents', 0))
                    with col3:
                        st.metric("Recent (1yr)", meta.get('recent_ai_patents', 0))
                    with col4:
                        st.metric("Categories", meta.get('category_count', 0))
                    
                    st.markdown("---")
                    
                    # Score breakdown
                    st.markdown("### Score Breakdown")
                    
                    breakdown = meta.get('score_breakdown', {})
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Count", f"{breakdown.get('patent_count', 0)}/50")
                        st.caption("5 pts/patent (max 50)")
                    with col2:
                        st.metric("Recency", f"{breakdown.get('recency', 0)}/20")
                        st.caption("2 pts/recent (max 20)")
                    with col3:
                        st.metric("Diversity", f"{breakdown.get('diversity', 0)}/30")
                        st.caption("10 pts/category (max 30)")
                    with col4:
                        total = sum(breakdown.values())
                        st.metric("**Total**", f"{total}/100")
                        st.caption(f"{meta.get('maturity_level', 'Unknown')}")
                    
                    # Categories
                    st.markdown("### Categories")
                    categories = meta.get('categories', [])
                    if categories:
                        category_names = {
                            'ml_core': '🤖 ML Core',
                            'nlp': '💬 NLP',
                            'computer_vision': '👁️ Computer Vision',
                            'predictive': '📊 Predictive',
                            'automation': '🤖 Automation'
                        }
                        cols = st.columns(len(categories))
                        for i, cat in enumerate(categories):
                            with cols[i]:
                                st.info(category_names.get(cat, cat))
                    
                    # Sample patents
                    st.markdown("### Sample Patents")
                    samples = meta.get('sample_patents', [])
                    if samples:
                        for p in samples[:5]:
                            with st.expander(f"📄 {p.get('number', 'N/A')}"):
                                st.markdown(f"**Title:** {p.get('title', 'N/A')}")
                                st.markdown(f"**Categories:** {', '.join(p.get('categories', []))}")
                                st.markdown(f"**CPC:** {', '.join(p.get('cpc_codes', []))}")
                else:
                    st.warning(f"No patent signals for {ticker}")
            
            except Exception as e:
                st.error(f"Error: {e}")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 📄 SEC DOCUMENTS (CS2 - NEW!)
# ============================================
elif page == "📄 SEC Documents":
    st.markdown('<p class="main-header">📄 SEC Documents</p>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📥 Collect Documents", "📋 View Documents"])
    
    with tab1:
        st.markdown("### Collect SEC Filings")
        
        try:
            companies = api.list_companies(limit=100)
            
            if companies:
                comp_choice = st.selectbox(
                    "Select Company",
                    [(f"{c['ticker']} - {c['name']}", c['ticker']) for c in companies if c.get('ticker')],
                    format_func=lambda x: x[0]
                )
                ticker = comp_choice[1]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    filing_types = st.multiselect(
                        "Filing Types",
                        ["10-K", "10-Q", "8-K", "DEF 14A"],
                        default=["10-K", "10-Q"]
                    )
                
                with col2:
                    limit = st.number_input("Limit per type", 1, 5, 1)
                
                steps = st.multiselect(
                    "Pipeline Steps",
                    ["download", "parse", "clean", "chunk"],
                    default=["download", "parse", "clean", "chunk"]
                )
                
                if st.button("📥 Collect Documents", type="primary", use_container_width=True):
                    if filing_types and steps:
                        with st.spinner(f"Collecting documents for {ticker}..."):
                            try:
                                result = api.collect_documents(ticker, filing_types, limit, steps)
                                st.success(f"✅ Collection complete!")
                                st.json(result)
                            except Exception as e:
                                st.error(f"Error: {e}")
                    else:
                        st.error("Select at least one filing type and step!")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab2:
        st.markdown("### View SEC Documents")
        
        try:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                ticker_filter = st.text_input("Filter by Ticker", placeholder="WMT")
            with col2:
                filing_filter = st.selectbox("Filing Type", ["All", "10-K", "10-Q", "8-K", "DEF 14A"])
            with col3:
                status_filter = st.selectbox("Status", ["All", "downloaded", "parsed", "cleaned", "chunked"])
            
            docs_data = api.list_documents(
                ticker=ticker_filter if ticker_filter else None,
                filing_type=filing_filter if filing_filter != "All" else None,
                status=status_filter if status_filter != "All" else None,
                limit=50
            )
            
            docs = docs_data.get('items', [])
            
            if docs:
                st.success(f"Found {len(docs)} documents")
                
                for doc in docs:
                    status_color = {
                        'downloaded': '🟡',
                        'parsed': '🔵',
                        'cleaned': '🟣',
                        'chunked': '🟢',
                        'failed': '🔴'
                    }.get(doc.get('status', ''), '⚪')
                    
                    with st.expander(f"{status_color} {doc.get('ticker')} - {doc.get('filing_type')} ({doc.get('status')})"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**Filing Date:** {doc.get('filing_date', 'N/A')}")
                            st.write(f"**Status:** {doc.get('status')}")
                            st.write(f"**Chunks:** {doc.get('chunk_count', 0)}")
                        
                        with col2:
                            st.write(f"**S3 Key:** `{doc.get('s3_key', 'N/A')[:50]}...`")
                            if doc.get('source_url'):
                                st.markdown(f"[View on SEC.gov]({doc['source_url']})")
                        
                        # View chunks
                        if doc.get('chunk_count', 0) > 0:
                            if st.button("View Chunks", key=f"chunk_{doc['id']}"):
                                try:
                                    chunks_data = api.get_document_chunks(doc['id'], limit=10)
                                    chunks = chunks_data.get('items', [])
                                    
                                    st.markdown(f"**Showing {len(chunks)} chunks:**")
                                    for chunk in chunks:
                                        st.text_area(
                                            f"Chunk {chunk.get('chunk_index', 'N/A')}",
                                            chunk.get('content', '')[:500],
                                            height=150,
                                            key=f"chunk_content_{chunk['id']}"
                                        )
                                except Exception as e:
                                    st.error(f"Error loading chunks: {e}")
            else:
                st.info("No documents found. Collect some in the 'Collect Documents' tab!")
        
        except Exception as e:
            st.error(f"Error: {e}")


# ============================================
# ⭐ CS3: ORG-AI-R RESULTS (NEW!)
# ============================================
elif page == "⭐ CS3: Org-AI-R Results":
    st.markdown('<p class="main-header">⭐ CS3: Org-AI-R Scoring Results</p>', unsafe_allow_html=True)
    st.caption("Organizational AI Readiness - Complete Assessment")
    
    # Load CS3 results
    @st.cache_data
    def load_org_air_results():
        import json
        from pathlib import Path
        
        results = []
        results_dir = Path("results")
        
        for file in results_dir.glob("*_org_air_result.json"):
            try:
                with open(file) as f:
                    data = json.load(f)
                    results.append(data)
            except Exception as e:
                st.warning(f"Error loading {file.name}: {e}")
        
        return sorted(results, key=lambda x: x.get('final_score', 0), reverse=True)
    
    try:
        companies = load_org_air_results()
        
        if not companies:
            st.warning("⚠️ No Org-AI-R results found!")
            st.info("Run: `poetry run python scripts/score_all_5.py`")
            st.stop()
        
        # Sub-navigation
        view = st.radio(
            "Select View",
            ["📊 Portfolio Overview", "🔍 Company Detail", "📈 Component Analysis", "🎯 Evidence Analysis"],
            horizontal=True
        )
        
        st.markdown("---")
        
        # =========================================================================
        # VIEW 1: PORTFOLIO OVERVIEW
        # =========================================================================
        
        if view == "📊 Portfolio Overview":
            st.subheader("5-Company Portfolio - Org-AI-R Scores")
            
            # Top metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            
            avg_score = sum(c['final_score'] for c in companies) / len(companies)
            avg_vr = sum(c['vr_score'] for c in companies) / len(companies)
            avg_hr = sum(c['hr_score'] for c in companies) / len(companies)
            top = companies[0]
            total_evidence = sum(c.get('confidence', {}).get('evidence_count', 0) for c in companies)
            
            with col1:
                st.metric("Portfolio Avg", f"{avg_score:.2f}")
            with col2:
                st.metric("Avg VR", f"{avg_vr:.2f}")
            with col3:
                st.metric("Avg HR", f"{avg_hr:.2f}")
            with col4:
                st.metric("Top Performer", f"{top['ticker']}")
            with col5:
                st.metric("Total Evidence", total_evidence)
            
            # Company scores table
            st.markdown("---")
            st.markdown("#### 📊 All Companies")
            
            df = pd.DataFrame([
                {
                    "Rank": i,
                    "Ticker": c['ticker'],
                    "Company": c['company_name'],
                    "Sector": c['sector'],
                    "Org-AI-R": round(c['final_score'], 2),
                    "VR": round(c['vr_score'], 2),
                    "HR": round(c['hr_score'], 2),
                    "Synergy": round(c['synergy_score'], 2),
                    "Evidence": c.get('confidence', {}).get('evidence_count', 0),
                    "CI Width": round(c['confidence']['ci_upper'] - c['confidence']['ci_lower'], 2)
                }
                for i, c in enumerate(companies, 1)
            ])
            
            # Color-code scores
            def color_score(val):
                if pd.isna(val) or not isinstance(val, (int, float)):
                    return ''
                if val >= 70:
                    return 'background-color: #d4edda; color: #155724;'
                elif val >= 50:
                    return 'background-color: #fff3cd; color: #856404;'
                else:
                    return 'background-color: #f8d7da; color: #721c24;'
            
            styled = df.style.applymap(
                color_score,
                subset=['Org-AI-R', 'VR', 'HR', 'Synergy']
            )
            
            st.dataframe(styled, use_container_width=True, hide_index=True)
            
            # Bar chart comparison
            st.markdown("---")
            st.markdown("#### 📊 Score Comparison")
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Org-AI-R (Final)',
                x=df['Ticker'],
                y=df['Org-AI-R'],
                marker_color='#1f77b4',
                text=df['Org-AI-R'].round(1),
                textposition='outside'
            ))
            
            fig.add_trace(go.Bar(
                name='VR (Company)',
                x=df['Ticker'],
                y=df['VR'],
                marker_color='#ff7f0e'
            ))
            
            fig.add_trace(go.Bar(
                name='HR (Sector)',
                x=df['Ticker'],
                y=df['HR'],
                marker_color='#2ca02c'
            ))
            
            fig.update_layout(
                barmode='group',
                yaxis_title="Score (0-100)",
                height=450,
                yaxis_range=[0, 110]
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Scatter plot
            st.markdown("---")
            st.markdown("#### 🎯 VR vs HR Positioning")
            
            fig = px.scatter(
                df,
                x='VR',
                y='HR',
                size='Org-AI-R',
                color='Sector',
                text='Ticker',
                hover_data=['Company', 'Org-AI-R', 'Evidence'],
                labels={'VR': 'VR (Company Readiness)', 'HR': 'HR (Sector Opportunity)'}
            )
            
            fig.update_traces(textposition='top center')
            fig.update_layout(height=500)
            
            st.plotly_chart(fig, use_container_width=True)
        
        # =========================================================================
        # VIEW 2: COMPANY DETAIL
        # =========================================================================
        
        elif view == "🔍 Company Detail":
            st.subheader("Company Deep Dive")
            
            # Company selector
            selected = st.selectbox(
                "Select Company",
                options=[f"{c['ticker']} - {c['company_name']}" for c in companies],
                index=0
            )
            
            ticker = selected.split(" - ")[0]
            company = next(c for c in companies if c['ticker'] == ticker)
            
            # Header
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"## {company['company_name']}")
                st.caption(f"**{company['ticker']}** | **Sector:** {company['sector']}")
            with col2:
                score = company['final_score']
                score_class = "score-high" if score >= 70 else "score-medium" if score >= 50 else "score-low"
                st.markdown(f'<div class="{score_class}">Org-AI-R: {score:.2f}</div>', unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Key metrics
            st.markdown("#### 📊 Core Components")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("VR (Company)", f"{company['vr_score']:.2f}", 
                         help="Idiosyncratic Readiness - Company-specific AI capabilities")
            with col2:
                st.metric("HR (Sector)", f"{company['hr_score']:.2f}",
                         help="Horizon Readiness - Sector AI maturity")
            with col3:
                st.metric("Synergy", f"{company['synergy_score']:.2f}",
                         help="VR×HR interaction with alignment & timing factors")
            with col4:
                st.metric("Position Factor", f"{company['position_factor']:.3f}",
                         help="Company position vs sector peers (-1 to 1)")
            
            # Formula breakdown
            with st.expander("📐 Formula Breakdown"):
                st.latex(r"\text{Org-AI-R} = (1 - \beta) \cdot [\alpha \cdot VR + (1-\alpha) \cdot HR] + \beta \cdot \text{Synergy}")
                st.markdown(f"""
                - **α = 0.60** (60% company-specific, 40% sector)
                - **β = 0.12** (88% base, 12% synergy)
                - **VR** = {company['vr_score']:.2f}
                - **HR** = {company['hr_score']:.2f}
                - **Synergy** = {company['synergy_score']:.2f}
                - **Result** = {company['final_score']:.2f}
                """)
            
            st.markdown("---")
            
            # Dimension scores
            st.markdown("#### 📋 7 Dimension Breakdown")
            
            dims = company['dimension_scores']
            
            # Radar chart + table
            col1, col2 = st.columns([1, 1])
            
            with col1:
                fig = go.Figure()
                
                fig.add_trace(go.Scatterpolar(
                    r=[dims[d]['score'] for d in dims],
                    theta=[d.replace("_", " ").title() for d in dims],
                    fill='toself',
                    name=ticker,
                    line_color='#1f77b4'
                ))
                
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 100],
                            tickfont=dict(size=10)
                        )
                    ),
                    height=400,
                    showlegend=False
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                dim_df = pd.DataFrame([
                    {
                        "Dimension": d.replace("_", " ").title(),
                        "Score": round(data['score'], 1),
                        "Conf": round(data['confidence'], 2),
                        "Sources": len(data.get('contributing_sources', []))
                    }
                    for d, data in dims.items()
                ])
                
                st.dataframe(dim_df, use_container_width=True, hide_index=True, height=400)
            
            # Confidence interval
            st.markdown("---")
            st.markdown("#### 📊 Confidence & Reliability")
            
            ci = company['confidence']
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Lower Bound (95%)", f"{ci['ci_lower']:.2f}")
            with col2:
                st.metric("Upper Bound (95%)", f"{ci['ci_upper']:.2f}")
            with col3:
                st.metric("SEM", f"{ci['sem']:.2f}", 
                         help="Standard Error of Mean")
            with col4:
                reliability = ci.get('reliability', 0)
                if isinstance(reliability, str):
                    st.metric("Reliability", reliability)
                else:
                    st.metric("Reliability", f"{reliability:.1%}")
            
            # Confidence interval visualization
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=[ticker],
                y=[company['final_score']],
                error_y=dict(
                    type='data',
                    symmetric=False,
                    array=[ci['ci_upper'] - company['final_score']],
                    arrayminus=[company['final_score'] - ci['ci_lower']]
                ),
                mode='markers',
                marker=dict(size=15, color='#1f77b4'),
                name='Org-AI-R'
            ))
            
            fig.update_layout(
                title="95% Confidence Interval",
                yaxis_title="Score",
                yaxis_range=[0, 100],
                height=300
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.info(f"**Evidence Count:** {ci['evidence_count']} sources | **CI Width:** {ci['ci_upper'] - ci['ci_lower']:.2f} points")
        
        # =========================================================================
        # VIEW 3: COMPONENT ANALYSIS
        # =========================================================================
        
        elif view == "📈 Component Analysis":
            st.subheader("Score Component Analysis")
            
            # Component breakdown table
            st.markdown("#### 📊 Component Scores by Company")
            
            comp_df = pd.DataFrame([
                {
                    "Ticker": c['ticker'],
                    "Company": c['company_name'][:25],
                    "VR": round(c['vr_score'], 2),
                    "HR": round(c['hr_score'], 2),
                    "Synergy": round(c['synergy_score'], 2),
                    "Position Factor": round(c['position_factor'], 3),
                    "Talent Conc.": round(c['talent_concentration'], 3),
                    "Org-AI-R": round(c['final_score'], 2)
                }
                for c in companies
            ])
            
            st.dataframe(comp_df, use_container_width=True, hide_index=True)
            
            # Grouped bar chart
            st.markdown("---")
            st.markdown("#### Component Comparison")
            
            fig = go.Figure()
            
            components = ['VR', 'HR', 'Synergy']
            colors = ['#3498db', '#2ecc71', '#e74c3c']
            
            for comp, color in zip(components, colors):
                fig.add_trace(go.Bar(
                    name=comp,
                    x=comp_df['Ticker'],
                    y=comp_df[comp],
                    marker_color=color,
                    text=comp_df[comp].round(1),
                    textposition='outside'
                ))
            
            fig.update_layout(
                barmode='group',
                yaxis_title="Score",
                height=450,
                yaxis_range=[0, 110]
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Dimension heatmap
            st.markdown("---")
            st.markdown("#### 🔥 Dimension Heatmap")
            
            dimension_names = [
                "data_infrastructure", "ai_governance", "technology_stack",
                "talent", "leadership", "use_case_portfolio", "culture"
            ]
            
            heatmap_data = []
            for c in companies:
                row = [c['dimension_scores'][dim]['score'] for dim in dimension_names]
                heatmap_data.append(row)
            
            fig = go.Figure(data=go.Heatmap(
                z=heatmap_data,
                x=[d.replace("_", " ").title() for d in dimension_names],
                y=[c['ticker'] for c in companies],
                colorscale='RdYlGn',
                zmin=0,
                zmax=100,
                text=[[f"{val:.1f}" for val in row] for row in heatmap_data],
                texttemplate='%{text}',
                textfont={"size": 12}
            ))
            
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
            
            # Sector analysis
            st.markdown("---")
            st.markdown("#### 🏭 Sector Performance")
            
            sector_data = {}
            for c in companies:
                sector = c['sector']
                if sector not in sector_data:
                    sector_data[sector] = {'scores': [], 'count': 0}
                sector_data[sector]['scores'].append(c['final_score'])
                sector_data[sector]['count'] += 1
            
            sector_df = pd.DataFrame([
                {
                    "Sector": sector,
                    "Companies": data['count'],
                    "Avg Score": round(sum(data['scores']) / len(data['scores']), 2),
                    "Best": round(max(data['scores']), 2)
                }
                for sector, data in sector_data.items()
            ])
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.dataframe(sector_df, use_container_width=True, hide_index=True)
            
            with col2:
                fig = px.bar(
                    sector_df,
                    x='Sector',
                    y='Avg Score',
                    color='Avg Score',
                    color_continuous_scale='RdYlGn',
                    text='Avg Score'
                )
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(height=300, yaxis_range=[0, 100])
                st.plotly_chart(fig, use_container_width=True)
        
        # =========================================================================
        # VIEW 4: EVIDENCE ANALYSIS
        # =========================================================================
        
        elif view == "🎯 Evidence Analysis":
            st.subheader("Evidence Source Analysis")
            
            # Company selector
            selected = st.selectbox(
                "Select Company",
                options=[f"{c['ticker']} - {c['company_name']}" for c in companies],
                index=0,
                key="evidence_company_select"
            )
            
            ticker = selected.split(" - ")[0]
            company = next(c for c in companies if c['ticker'] == ticker)
            
            st.markdown(f"### {company['company_name']} - Evidence Sources")
            
            # Evidence summary
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Evidence Sources", company.get('confidence', {}).get('evidence_count', 0))
            with col2:
                # Count unique sources
                all_sources = set()
                for dim_data in company['dimension_scores'].values():
                    all_sources.update(dim_data.get('contributing_sources', []))
                st.metric("Unique Sources", len(all_sources))
            with col3:
                avg_sources = sum(len(d.get('contributing_sources', [])) for d in company['dimension_scores'].values()) / 7
                st.metric("Avg Sources/Dimension", f"{avg_sources:.1f}")
            
            # Source matrix
            st.markdown("---")
            st.markdown("#### 📋 Evidence Source Matrix")
            
            all_possible_sources = [
                "technology_hiring",
                "innovation_activity",
                "digital_presence",
                "leadership_signals",
                "glassdoor_reviews",
                "board_composition",
                "sec_item_1",
                "sec_item_1a",
                "sec_item_7"
            ]
            
            # Build matrix
            matrix_data = []
            for dim_name, dim_data in company['dimension_scores'].items():
                row = {"Dimension": dim_name.replace("_", " ").title()}
                sources = dim_data.get('contributing_sources', [])
                
                for source in all_possible_sources:
                    row[source.replace("_", " ").title()] = "✅" if source in sources else ""
                
                row["Total"] = len(sources)
                matrix_data.append(row)
            
            matrix_df = pd.DataFrame(matrix_data)
            st.dataframe(matrix_df, use_container_width=True, hide_index=True)
            
            # Source usage count
            st.markdown("---")
            st.markdown("#### 📊 Source Usage Frequency")
            
            source_counts = {}
            for dim_data in company['dimension_scores'].values():
                for source in dim_data.get('contributing_sources', []):
                    source_counts[source] = source_counts.get(source, 0) + 1
            
            if source_counts:
                source_df = pd.DataFrame([
                    {
                        "Source": source.replace("_", " ").title(),
                        "Used in Dimensions": count,
                        "Usage %": round((count / 7) * 100, 1)
                    }
                    for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
                ])
                
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    st.dataframe(source_df, use_container_width=True, hide_index=True)
                
                with col2:
                    fig = px.pie(
                        source_df,
                        values='Used in Dimensions',
                        names='Source',
                        title="Evidence Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Dimension detail with sources
            st.markdown("---")
            st.markdown("#### 🔍 Dimension-Level Detail")
            
            for dim_name, dim_data in company['dimension_scores'].items():
                with st.expander(f"**{dim_name.replace('_', ' ').title()}** - Score: {dim_data['score']:.2f}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric("Score", f"{dim_data['score']:.2f}/100")
                        st.metric("Confidence", f"{dim_data['confidence']:.2f}")
                    
                    with col2:
                        sources = dim_data.get('contributing_sources', [])
                        st.metric("Source Count", len(sources))
                        
                        if sources:
                            st.markdown("**Sources:**")
                            for source in sources:
                                st.caption(f"• {source.replace('_', ' ').title()}")
    
    except Exception as e:
        st.error(f"Error loading CS3 results: {e}")
        import traceback
        st.code(traceback.format_exc())

# ============================================
# 🚀 SCORE COMPANY (CS3)
# ============================================
elif page == "🚀 Score Company (CS3)":
    st.markdown('<p class="main-header">🚀 Score Company - CS3</p>', unsafe_allow_html=True)
    st.caption("Run the complete Org-AI-R scoring pipeline")
    
    try:
        companies_list = api.list_companies(limit=100)
        
        if not companies_list:
            st.warning("Add companies first!")
        else:
            # Company selection
            col1, col2 = st.columns([2, 1])
            
            with col1:
                comp_opts = [(f"{c['ticker']} - {c['name']}", c) for c in companies_list if c.get('ticker')]
                
                if not comp_opts:
                    st.warning("No companies with tickers found!")
                    st.stop()
                
                comp_choice = st.selectbox(
                    "Select Company to Score",
                    comp_opts,
                    format_func=lambda x: x[0]
                )
                company = comp_choice[1]
                ticker = company['ticker']
            
            with col2:
                sector_map = {
                    "550e8400-e29b-41d4-a716-446655440003": "Technology",
                    "550e8400-e29b-41d4-a716-446655440005": "Financial Services",
                    "550e8400-e29b-41d4-a716-446655440004": "Retail",
                    "550e8400-e29b-41d4-a716-446655440001": "Industrials",
                    "550e8400-e29b-41d4-a716-446655440002": "Healthcare"
                }
                
                default_sector = sector_map.get(company.get('industry_id'), "Technology")
                
                sector = st.selectbox(
                    "Sector",
                    list(sector_map.values()),
                    index=list(sector_map.values()).index(default_sector) if default_sector in sector_map.values() else 0
                )
            
            # Pipeline info
            with st.expander("ℹ️ Pipeline Steps", expanded=False):
                st.markdown("""
                **Complete Org-AI-R Pipeline:**
                1. 📥 Fetch company data (CS1)
                2. 📊 Fetch evidence (CS2: signals, SEC docs)
                3. 🎭 Collect Glassdoor culture data
                4. 👔 Collect board governance data
                5. 🗺️ Map evidence → 7 dimensions
                6. 🧮 Calculate: VR → PF → HR → Synergy → Org-AI-R → CI
                7. 💾 Save to database & JSON file
                
                **Estimated time:** 2-3 minutes
                """)
            
            st.markdown("---")
            
            # Score button
            if st.button(f"🚀 Score {ticker}", type="primary", use_container_width=True):
                
                import requests
                import time
                
                progress_placeholder = st.empty()
                status_placeholder = st.empty()
                
                with progress_placeholder.container():
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                
                try:
                    # Start scoring
                    status_text.text("📥 Starting scoring...")
                    progress_bar.progress(10)
                    
                    response = requests.post(
                        f"http://localhost:8000/api/v1/scoring/score/{ticker}",
                        params={"sector": sector},
                        timeout=10
                    )
                    
                    if response.status_code != 200:
                        st.error(f"❌ Failed to start: {response.text}")
                        st.stop()
                    
                    data = response.json()
                    task_id = data.get('task_id')
                    
                    status_text.text(f"✅ Task started: {task_id[:8]}...")
                    progress_bar.progress(20)
                    
                    # Poll for completion
                    max_wait = 300
                    start_time = time.time()
                    
                    while time.time() - start_time < max_wait:
                        time.sleep(5)
                        
                        try:
                            status_resp = requests.get(
                                f"http://localhost:8000/api/v1/scoring/status/{task_id}",
                                timeout=5
                            )
                            
                            if status_resp.status_code == 200:
                                status_data = status_resp.json()
                                current_status = status_data.get('status')
                                elapsed = int(time.time() - start_time)
                                
                                if current_status == "queued":
                                    progress_bar.progress(20)
                                    status_text.text(f"⏳ Queued... ({elapsed}s)")
                                
                                elif current_status == "running":
                                    progress = min(80, 20 + int(elapsed / 2))
                                    progress_bar.progress(progress)
                                    status_text.text(f"🔄 Running... ({elapsed}s)")
                                
                                elif current_status == "completed":
                                    progress_bar.progress(100)
                                    status_text.text(f"✅ Completed in {elapsed}s!")
                                    
                                    progress_placeholder.empty()
                                    
                                    st.success(f"✅ {ticker} scored successfully!")
                                    
                                    st.markdown("### 📊 Results")
                                    
                                    col1, col2, col3, col4 = st.columns(4)
                                    
                                    with col1:
                                        st.metric("Org-AI-R", f"{status_data['final_score']:.2f}")
                                    with col2:
                                        st.metric("VR", f"{status_data['vr_score']:.2f}")
                                    with col3:
                                        st.metric("HR", f"{status_data['hr_score']:.2f}")
                                    with col4:
                                        st.metric("Synergy", f"{status_data['synergy_score']:.2f}")
                                    
                                    ci = status_data.get('confidence', {})
                                    
                                    st.markdown("#### 📊 Confidence Interval")
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Lower (95%)", f"{ci.get('ci_lower', 0):.2f}")
                                    with col2:
                                        st.metric("Upper (95%)", f"{ci.get('ci_upper', 0):.2f}")
                                    with col3:
                                        st.metric("Evidence", ci.get('evidence_count', 0))
                                    
                                    st.info(f"💾 Saved to: `{status_data.get('result_file', 'N/A')}`")
                                    
                                    with st.expander("📄 Full Results"):
                                        st.json(status_data)
                                    
                                    st.balloons()
                                    break
                                
                                elif current_status == "failed":
                                    progress_placeholder.empty()
                                    st.error(f"❌ Scoring failed!")
                                    st.code(status_data.get('error', 'Unknown error'))
                                    break
                        
                        except Exception as poll_error:
                            st.warning(f"Status check error: {poll_error}")
                            continue
                    
                    else:
                        progress_placeholder.empty()
                        st.error("⏱️ Scoring timed out (>5 minutes)")
                
                except requests.Timeout:
                    st.error("⏱️ Request timed out")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    import traceback
                    with st.expander("Error Details"):
                        st.code(traceback.format_exc())
            
            # Show existing results
            st.markdown("---")
            st.markdown("### 📁 Previously Scored Companies")
            
            try:
                import requests
                response = requests.get("http://localhost:8000/api/v1/scoring/results", timeout=5)
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get('results', [])
                    
                    if results:
                        results_df = pd.DataFrame(results)
                        if 'scored_at' in results_df.columns:
                            results_df['scored_at'] = pd.to_datetime(results_df['scored_at']).dt.strftime('%Y-%m-%d %H:%M')
                        
                        st.dataframe(
                            results_df[['ticker', 'company_name', 'final_score', 'vr_score', 'hr_score', 'scored_at']],
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info("No previous results. Score a company above!")
            except:
                st.info("No results available")
    
    except Exception as e:
        st.error(f"Error: {e}")







# ============================================
# 🔧 SYSTEM HEALTH
# ============================================
elif page == "🔧 System Health":
    st.markdown('<p class="main-header">🔧 System Health</p>', unsafe_allow_html=True)
    
    try:
        health = api.get_health()
        
        if health['status'] == 'healthy':
            st.success(f"## ✅ System: {health['status'].upper()}")
        else:
            st.warning(f"## ⚠️ System: {health['status'].upper()}")
        
        st.markdown("---")
        
        # Dependencies
        st.markdown("### Dependencies")
        
        deps = health.get('dependencies', {})
        cols = st.columns(len(deps))
        
        for i, (service, status) in enumerate(deps.items()):
            with cols[i]:
                if status == 'healthy':
                    st.success(f"✅ {service.capitalize()}")
                elif status == 'not_configured':
                    st.info(f"ℹ️ {service.capitalize()}")
                else:
                    st.error(f"❌ {service.capitalize()}")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Version:** {health.get('version', 'Unknown')}")
        with col2:
            st.info(f"**Checked:** {health.get('timestamp', 'Unknown')}")
        
        with st.expander("Full Response"):
            st.json(health)
    
    except Exception as e:
        st.error("❌ Cannot connect to API")
        st.error(f"Error: {e}")
        st.info("**Troubleshooting:**")
        st.code("poetry run uvicorn app.main:create_app --factory --reload")

elif page == "🔍 Evidence Search":
    st.markdown('<p class="main-header">🔍 Evidence Search</p>', unsafe_allow_html=True)
    st.caption("Search across SEC filings, job postings, patents and board signals using AI-powered hybrid search")

    # ------------------------------------------------------------------
    # Step 1: Seed Evidence
    # ------------------------------------------------------------------
    st.markdown("### Step 1: Seed Evidence")
    st.caption("Index a company's evidence from Snowflake into the search engine before searching.")

    seed_col1, seed_col2 = st.columns([2, 1])
    with seed_col1:
        seed_ticker = st.text_input(
            "Company Ticker to Seed",
            value="NVDA",
            placeholder="e.g. NVDA, AAPL, TSLA",
            key="seed_ticker"
        ).strip().upper()
    with seed_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        seed_btn = st.button("🌱 Seed Evidence", type="primary", use_container_width=True)

    if seed_btn:
        with st.spinner(f"Fetching and indexing evidence for {seed_ticker}..."):
            try:
                result = api.seed_evidence(seed_ticker)
                indexed = result.get("indexed", 0)
                if indexed > 0:
                    st.success(f"✅ Indexed **{indexed}** documents for **{seed_ticker}**. You can now search below.")
                    st.session_state["seeded_ticker"] = seed_ticker
                else:
                    st.warning(f"⚠️ {result.get('message', 'No evidence found in Snowflake for ' + seed_ticker)}")
            except Exception as e:
                st.error(f"Seeding failed: {e}")

    if "seeded_ticker" in st.session_state:
        st.info(f"ℹ️ Last seeded: **{st.session_state['seeded_ticker']}** — ready to search.")

    st.markdown("---")
    st.markdown("### Step 2: Search")

    # Search form
    with st.form("search_form"):
        query = st.text_input(
            "🔎 Search Query",
            placeholder="e.g. AI talent hiring machine learning engineers",
        )

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            company_filter = st.text_input(
                "Company Filter (ticker or 'All')",
                value="All",
                placeholder="e.g. NVDA or All",
            ).strip().upper()

        with col2:
            dimension_filter = st.selectbox(
                "Dimension",
                [
                    "All",
                    "data_infrastructure",
                    "ai_governance",
                    "technology_stack",
                    "talent",
                    "leadership",
                    "use_case_portfolio",
                    "culture"
                ]
            )

        with col3:
            top_k = st.slider("Results", 5, 50, 10)

        with col4:
            min_conf = st.slider("Min Confidence", 0.0, 1.0, 0.0, 0.1)

        source_types_filter = st.multiselect(
            "📂 Source Types (leave empty for all)",
            options=[
                "sec_10k_item_1", "sec_10k_item_1a", "sec_10k_item_7",
                "job_posting_linkedin", "job_posting_indeed", "patent_uspto",
                "glassdoor_review", "board_proxy_def14a", "analyst_interview", "dd_data_room"
            ],
            help="Filter results by evidence source type",
        )

        search_btn = st.form_submit_button("🔍 Search", use_container_width=True, type="primary")

    # Run search
    if search_btn:
        if not query:
            st.warning("Please enter a search query!")
        else:
            with st.spinner("Searching evidence..."):
                try:
                    results = api.search_evidence(
                        query=query,
                        company_id=company_filter if company_filter != "All" else None,
                        dimension=dimension_filter if dimension_filter != "All" else None,
                        top_k=top_k,
                        min_confidence=min_conf,
                        source_types=source_types_filter if source_types_filter else None,
                    )

                    if not results:
                        st.warning("No results found. Try a different query or broaden your filters.")
                    else:
                        st.success(f"Found **{len(results)}** evidence items")
                        st.markdown("---")

                        for i, result in enumerate(results, 1):
                            meta = result.get("metadata", {})
                            source_type = meta.get("source_type", "unknown")
                            company = meta.get("company_id", "N/A")
                            dimension = meta.get("dimension", "N/A")
                            confidence = meta.get("confidence", 0)
                            fiscal_year = meta.get("fiscal_year", "N/A")
                            score = result.get("score", 0)
                            method = result.get("retrieval_method", "hybrid")

                            # Source type emoji
                            source_emoji = {
                                "sec_10k_item_1": "📄",
                                "sec_10k_item_1a": "⚠️",
                                "sec_10k_item_7": "📊",
                                "job_posting_linkedin": "💼",
                                "job_posting_indeed": "💼",
                                "patent_uspto": "🔬",
                                "glassdoor_review": "⭐",
                                "board_proxy_def14a": "👔",
                            }.get(source_type, "📌")

                            with st.expander(
                                f"{source_emoji} [{company}] {dimension.replace('_',' ').title()} "
                                f"| {source_type} | Score: {score:.4f}"
                            ):
                                col1, col2, col3, col4 = st.columns(4)

                                with col1:
                                    st.metric("Company", company)
                                with col2:
                                    st.metric("Dimension", dimension.replace("_", " ").title())
                                with col3:
                                    st.metric("Confidence", f"{confidence:.2f}")
                                with col4:
                                    st.metric("Fiscal Year", str(fiscal_year) if fiscal_year else "N/A")

                                st.markdown("**Evidence Content:**")
                                st.text_area(
                                    "",
                                    value=result.get("content", "")[:800],
                                    height=150,
                                    key=f"content_{i}",
                                    disabled=True,
                                )

                                source_url = meta.get("source_url")
                                if source_url:
                                    st.markdown(f"[🔗 View Source]({source_url})")

                                st.caption(
                                    f"Doc ID: {result.get('doc_id','N/A')} | "
                                    f"Method: {method} | "
                                    f"RRF Score: {score:.6f}"
                                )

                except Exception as e:
                    st.error(f"Search failed: {e}")
                    st.info("Make sure the API is running and evidence has been indexed.")

    # Help section
    with st.expander("💡 Search Tips"):
        st.markdown("""
        **Good queries:**
        - `AI talent machine learning engineers` → finds hiring evidence
        - `cloud data pipeline infrastructure` → finds tech stack evidence
        - `board AI committee governance` → finds governance evidence
        - `patent innovation artificial intelligence` → finds patent evidence

        **Filters:**
        - **Company** → narrow to a specific ticker (NVDA, JPM, etc.)
        - **Dimension** → narrow to a specific AI-readiness dimension
        - **Min Confidence** → only show high-quality evidence (0.8+)

        **How it works:**
        The search combines vector search (finds by meaning) and BM25 keyword search,
        then merges results using Reciprocal Rank Fusion for the best results.
        """)


# ============================================
# 📝 SCORE JUSTIFICATION (CS4)
# ============================================
elif page == "📝 Score Justification":
    st.markdown('<p class="main-header">📝 Score Justification</p>', unsafe_allow_html=True)
    st.caption("Explain why a company scored a specific level on any AI-readiness dimension")

    DIMENSIONS = [
        "data_infrastructure",
        "ai_governance",
        "technology_stack",
        "talent",
        "leadership",
        "use_case_portfolio",
        "culture",
    ]

    tab1, tab2 = st.tabs(["🔍 Single Dimension", "📦 IC Meeting Prep (All 7)"])

    # ------------------------------------------------------------------
    # TAB 1: Single Dimension Justification
    # ------------------------------------------------------------------
    with tab1:
        st.markdown("### Justify a Single Dimension Score")
        st.caption("Get a cited, evidence-backed explanation for any dimension score")

        col1, col2 = st.columns(2)

        with col1:
            ticker = st.text_input(
                "Company Ticker",
                value="NVDA",
                placeholder="e.g. AAPL, MSFT, TSLA",
                key="just_ticker"
            ).strip().upper()

        with col2:
            dimension = st.selectbox(
                "Dimension",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
                key="just_dim"
            )

        if st.button("🔍 Generate Justification", type="primary", use_container_width=True):
            with st.spinner(f"Generating justification for {ticker} - {dimension}... (takes ~10 seconds)"):
                try:
                    result = api.get_justification(ticker, dimension)

                    # Score header
                    st.markdown("---")

                    col1, col2, col3, col4 = st.columns(4)

                    level_color = {
                        5: "🟢", 4: "🔵", 3: "🟡", 2: "🟠", 1: "🔴"
                    }.get(result.get("level", 1), "⚪")

                    with col1:
                        st.metric("Score", f"{result['score']:.1f}/100")
                    with col2:
                        st.metric("Level", f"{level_color} {result['level']} - {result['level_name']}")
                    with col3:
                        ci = result.get("confidence_interval", [0, 0])
                        st.metric("95% CI", f"{ci[0]:.1f} – {ci[1]:.1f}")
                    with col4:
                        strength_emoji = {"strong": "💪", "moderate": "👍", "weak": "⚠️"}.get(
                            result.get("evidence_strength"), "❓"
                        )
                        st.metric("Evidence", f"{strength_emoji} {result.get('evidence_strength','N/A').title()}")

                    st.markdown("---")

                    # Rubric
                    st.markdown("#### 📋 Rubric Criteria")
                    st.info(result.get("rubric_criteria", "N/A"))

                    keywords = result.get("rubric_keywords", [])
                    if keywords:
                        st.markdown("**Keywords matched against:**")
                        st.markdown(" ".join([f"`{kw}`" for kw in keywords]))

                    st.markdown("---")

                    # LLM Summary
                    st.markdown("#### 📝 IC-Ready Justification")
                    st.markdown(result.get("generated_summary", "No summary generated."))

                    st.markdown("---")

                    # Supporting evidence
                    evidence = result.get("supporting_evidence", [])
                    st.markdown(f"#### 📚 Supporting Evidence ({len(evidence)} items)")

                    if evidence:
                        for i, ev in enumerate(evidence, 1):
                            source_emoji = {
                                "sec_10k_item_1": "📄",
                                "sec_10k_item_1a": "⚠️",
                                "sec_10k_item_7": "📊",
                                "job_posting_linkedin": "💼",
                                "patent_uspto": "🔬",
                                "glassdoor_review": "⭐",
                                "board_proxy_def14a": "👔",
                            }.get(ev.get("source_type"), "📌")

                            with st.expander(
                                f"{source_emoji} Evidence {i} | "
                                f"{ev.get('source_type','N/A')} | "
                                f"Confidence: {ev.get('confidence',0):.2f} | "
                                f"Matched: {ev.get('matched_keywords',[])}"
                            ):
                                st.text_area(
                                    "",
                                    value=ev.get("content", "")[:600],
                                    height=120,
                                    key=f"ev_{i}",
                                    disabled=True,
                                )
                                if ev.get("source_url"):
                                    st.markdown(f"[🔗 View on SEC.gov]({ev['source_url']})")
                    else:
                        st.warning("No supporting evidence found. Make sure evidence is indexed.")

                    # Gaps
                    gaps = result.get("gaps_identified", [])
                    if gaps:
                        st.markdown("---")
                        st.markdown("#### 🚧 Gaps (What's missing for a higher score?)")
                        for gap in gaps:
                            st.warning(f"• {gap}")

                except Exception as e:
                    st.error(f"Failed to generate justification: {e}")
                    st.info("Make sure the API is running and evidence is indexed.")

    # ------------------------------------------------------------------
    # TAB 2: IC Meeting Prep (all 7 dimensions)
    # ------------------------------------------------------------------
    with tab2:
        st.markdown("### IC Meeting Preparation Package")
        st.caption("Generate a complete investment committee package covering all 7 dimensions")

        col1, col2 = st.columns(2)

        with col1:
            ic_ticker = st.text_input(
                "Company Ticker",
                value="NVDA",
                placeholder="e.g. AAPL, MSFT, TSLA",
                key="ic_ticker"
            ).strip().upper()

        with col2:
            focus = st.multiselect(
                "Focus Dimensions (optional — leave empty for all 7)",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
            )

        st.warning("⚠️ IC prep runs justification for all selected dimensions. This takes 1-2 minutes.")

        if st.button("📦 Generate IC Package", type="primary", use_container_width=True):
            with st.spinner(f"Generating IC package for {ic_ticker}... (1-2 minutes)"):
                try:
                    result = api.get_ic_prep(ic_ticker, focus if focus else None)

                    st.success("✅ IC Package generated!")
                    st.markdown("---")

                    # Header metrics
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Org-AI-R Score", f"{result.get('org_air_score', 0):.1f}")
                    with col2:
                        st.metric("VR Score", f"{result.get('vr_score', 0):.1f}")
                    with col3:
                        st.metric("HR Score", f"{result.get('hr_score', 0):.1f}")
                    with col4:
                        strength_emoji = {"strong": "💪", "moderate": "👍", "weak": "⚠️"}.get(
                            result.get("avg_evidence_strength"), "❓"
                        )
                        st.metric("Evidence", f"{strength_emoji} {result.get('avg_evidence_strength','').title()}")

                    st.markdown("---")

                    # Executive summary
                    st.markdown("#### 📋 Executive Summary")
                    st.info(result.get("executive_summary", "N/A"))

                    # Three columns: strengths, gaps, risks
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.markdown("#### 💪 Key Strengths")
                        for s in result.get("key_strengths", []):
                            st.success(f"• {s}")

                    with col2:
                        st.markdown("#### 🚧 Key Gaps")
                        for g in result.get("key_gaps", []):
                            st.warning(f"• {g}")

                    with col3:
                        st.markdown("#### ⚠️ Risk Factors")
                        for r in result.get("risk_factors", []):
                            st.error(f"• {r}")

                    # Recommendation
                    st.markdown("---")
                    rec = result.get("recommendation", "")
                    if "PROCEED" in rec and "CAUTION" not in rec:
                        st.success(f"## 🟢 Recommendation: {rec}")
                    elif "CAUTION" in rec:
                        st.warning(f"## 🟡 Recommendation: {rec}")
                    else:
                        st.error(f"## 🔴 Recommendation: {rec}")

                    # Dimension justifications
                    st.markdown("---")
                    st.markdown("#### 📊 Dimension Justifications")

                    dim_justs = result.get("dimension_justifications", {})

                    for dim_name, just in dim_justs.items():
                        level = just.get("level", 1)
                        score = just.get("score", 0)
                        level_color = {5: "🟢", 4: "🔵", 3: "🟡", 2: "🟠", 1: "🔴"}.get(level, "⚪")

                        with st.expander(
                            f"{level_color} {dim_name.replace('_',' ').title()} — "
                            f"Score: {score:.1f} | Level {level}: {just.get('level_name','')}"
                        ):
                            st.markdown(just.get("generated_summary", "No summary."))

                            evidence = just.get("supporting_evidence", [])
                            if evidence:
                                st.caption(f"📚 {len(evidence)} evidence items | "
                                          f"Strength: {just.get('evidence_strength','N/A')}")

                            gaps = just.get("gaps_identified", [])
                            if gaps:
                                st.markdown("**Gaps:**")
                                for gap in gaps[:3]:
                                    st.caption(f"• {gap}")

                    st.caption(
                        f"Generated: {result.get('generated_at','N/A')} | "
                        f"Total evidence: {result.get('total_evidence_count', 0)} items"
                    )

                except Exception as e:
                    st.error(f"IC prep failed: {e}")
                    st.info("This can fail if any dimension has no evidence. Try selecting fewer dimensions.")

# ============================================
# 🗒️ ANALYST NOTES (CS4)
# ============================================
elif page == "🗒️ Analyst Notes":
    st.markdown('<p class="main-header">🗒️ Analyst Notes</p>', unsafe_allow_html=True)
    st.caption("Submit primary-source due diligence evidence — indexed into ChromaDB with confidence=1.0")

    DIMENSIONS = [
        "data_infrastructure",
        "ai_governance",
        "technology_stack",
        "talent",
        "leadership",
        "use_case_portfolio",
        "culture",
    ]
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🎤 Interview",
        "🔍 DD Finding",
        "📂 Data Room",
        "🤝 Management Meeting",
        "🏭 Site Visit",
    ])

    # ------------------------------------------------------------------
    # TAB 1 — Interview Transcript
    # ------------------------------------------------------------------
    with tab1:
        st.markdown("### Submit Interview Transcript")
        st.caption("Index an interview with management, CTO, CDO, or other key stakeholders")

        col1, col2 = st.columns(2)
        with col1:
            iv_ticker = st.text_input("Company Ticker", value="NVDA", placeholder="e.g. AAPL, MSFT", key="iv_ticker").strip().upper()
            iv_interviewee = st.text_input("Interviewee Name", placeholder="e.g. Jensen Huang", key="iv_interviewee")
            iv_title = st.text_input("Interviewee Title", placeholder="e.g. CEO", key="iv_title")
        with col2:
            iv_assessor = st.text_input("Assessor (your name/email)", placeholder="analyst@firm.com", key="iv_assessor")
            iv_dims = st.multiselect(
                "Dimensions Discussed",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
                key="iv_dims"
            )

        iv_transcript = st.text_area(
            "Interview Transcript / Notes (min 50 chars)",
            height=200,
            placeholder="Paste the full transcript or detailed notes here...",
            key="iv_transcript"
        )
        iv_findings_raw = st.text_area("Key Findings (one per line, optional)", height=80, key="iv_findings")
        iv_risks_raw = st.text_area("Risk Flags (one per line, optional)", height=80, key="iv_risks")

        if st.button("🎤 Submit Interview", type="primary", use_container_width=True, key="iv_submit"):
            if not iv_interviewee or not iv_title or not iv_assessor:
                st.warning("Please fill in interviewee name, title, and assessor.")
            elif len(iv_transcript) < 50:
                st.warning("Transcript must be at least 50 characters.")
            elif not iv_dims:
                st.warning("Select at least one dimension.")
            else:
                with st.spinner("Indexing interview..."):
                    try:
                        result = api.submit_interview(iv_ticker, {
                            "interviewee": iv_interviewee,
                            "interviewee_title": iv_title,
                            "transcript": iv_transcript,
                            "assessor": iv_assessor,
                            "dimensions_discussed": iv_dims,
                            "key_findings": [f for f in iv_findings_raw.splitlines() if f.strip()],
                            "risk_flags": [r for r in iv_risks_raw.splitlines() if r.strip()],
                        })
                        st.success(f"✅ {result['message']}")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Note ID", result["note_id"])
                        col2.metric("Type", result["note_type"])
                        col3.metric("Primary Dimension", result["primary_dimension"].replace("_", " ").title())
                    except Exception as e:
                        st.error(f"Failed to submit interview: {e}")

    # ------------------------------------------------------------------
    # TAB 2 — DD Finding
    # ------------------------------------------------------------------
    with tab2:
        st.markdown("### Submit Due Diligence Finding")
        st.caption("Flag a specific risk or gap discovered during diligence")

        col1, col2 = st.columns(2)
        with col1:
            dd_ticker = st.text_input("Company Ticker", value="NVDA", placeholder="e.g. AAPL, MSFT", key="dd_ticker").strip().upper()
            dd_title = st.text_input("Finding Title", placeholder="e.g. No data quality monitoring", key="dd_title")
            dd_assessor = st.text_input("Assessor", placeholder="analyst@firm.com", key="dd_assessor")
        with col2:
            dd_dimension = st.selectbox(
                "Dimension",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
                key="dd_dimension"
            )
            dd_severity = st.selectbox("Severity", ["critical", "moderate", "low"], key="dd_severity")

        dd_finding = st.text_area(
            "Finding Description (min 20 chars)",
            height=180,
            placeholder="Describe the finding in detail...",
            key="dd_finding"
        )
        dd_findings_raw = st.text_area("Key Findings (one per line, optional)", height=80, key="dd_findings")
        dd_risks_raw = st.text_area("Risk Flags (one per line, optional)", height=80, key="dd_risks")

        if st.button("🔍 Submit DD Finding", type="primary", use_container_width=True, key="dd_submit"):
            if not dd_title or not dd_assessor:
                st.warning("Please fill in finding title and assessor.")
            elif len(dd_finding) < 20:
                st.warning("Finding description must be at least 20 characters.")
            else:
                with st.spinner("Indexing DD finding..."):
                    try:
                        result = api.submit_dd_finding(dd_ticker, {
                            "title": dd_title,
                            "finding": dd_finding,
                            "dimension": dd_dimension,
                            "severity": dd_severity,
                            "assessor": dd_assessor,
                            "key_findings": [f for f in dd_findings_raw.splitlines() if f.strip()],
                            "risk_flags": [r for r in dd_risks_raw.splitlines() if r.strip()],
                        })
                        severity_emoji = {"critical": "🔴", "moderate": "🟠", "low": "🟡"}.get(dd_severity, "⚪")
                        st.success(f"✅ {result['message']}")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Note ID", result["note_id"])
                        col2.metric("Severity", f"{severity_emoji} {dd_severity.title()}")
                        col3.metric("Dimension", result["primary_dimension"].replace("_", " ").title())
                    except Exception as e:
                        st.error(f"Failed to submit DD finding: {e}")

    # ------------------------------------------------------------------
    # TAB 3 — Data Room
    # ------------------------------------------------------------------
    with tab3:
        st.markdown("### Submit Data Room Document Summary")
        st.caption("Summarize a document from the virtual data room")

        col1, col2 = st.columns(2)
        with col1:
            dr_ticker = st.text_input("Company Ticker", value="NVDA", placeholder="e.g. AAPL, MSFT", key="dr_ticker").strip().upper()
            dr_doc_name = st.text_input("Document Name", placeholder="e.g. AI_Roadmap_2025.pdf", key="dr_doc_name")
            dr_assessor = st.text_input("Assessor", placeholder="analyst@firm.com", key="dr_assessor")
        with col2:
            dr_dimension = st.selectbox(
                "Primary Dimension",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
                key="dr_dimension"
            )

        dr_summary = st.text_area(
            "Document Summary (min 50 chars)",
            height=200,
            placeholder="Summarise the key contents of this data room document...",
            key="dr_summary"
        )
        dr_findings_raw = st.text_area("Key Findings (one per line, optional)", height=80, key="dr_findings")

        if st.button("📂 Submit Data Room Summary", type="primary", use_container_width=True, key="dr_submit"):
            if not dr_doc_name or not dr_assessor:
                st.warning("Please fill in document name and assessor.")
            elif len(dr_summary) < 50:
                st.warning("Summary must be at least 50 characters.")
            else:
                with st.spinner("Indexing data room document..."):
                    try:
                        result = api.submit_data_room(dr_ticker, {
                            "document_name": dr_doc_name,
                            "summary": dr_summary,
                            "dimension": dr_dimension,
                            "assessor": dr_assessor,
                            "key_findings": [f for f in dr_findings_raw.splitlines() if f.strip()],
                        })
                        st.success(f"✅ {result['message']}")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Note ID", result["note_id"])
                        col2.metric("Type", result["note_type"])
                        col3.metric("Dimension", result["primary_dimension"].replace("_", " ").title())
                    except Exception as e:
                        st.error(f"Failed to submit data room summary: {e}")

    # ------------------------------------------------------------------
    # TAB 4 — Management Meeting
    # ------------------------------------------------------------------
    with tab4:
        st.markdown("### Submit Management Meeting Notes")
        st.caption("Index notes from a management meeting or roadshow")

        col1, col2 = st.columns(2)
        with col1:
            mm_ticker = st.text_input("Company Ticker", value="NVDA", placeholder="e.g. AAPL, MSFT", key="mm_ticker").strip().upper()
            mm_title = st.text_input("Meeting Title", placeholder="e.g. Q1 2026 Roadshow - NVDA", key="mm_title")
            mm_assessor = st.text_input("Assessor", placeholder="analyst@firm.com", key="mm_assessor")
        with col2:
            mm_dims = st.multiselect(
                "Dimensions Discussed",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
                key="mm_dims"
            )

        mm_notes = st.text_area(
            "Meeting Notes (min 50 chars)",
            height=200,
            placeholder="Enter full meeting notes...",
            key="mm_notes"
        )
        mm_findings_raw = st.text_area("Key Findings (one per line, optional)", height=80, key="mm_findings")
        mm_risks_raw = st.text_area("Risk Flags (one per line, optional)", height=80, key="mm_risks")

        if st.button("🤝 Submit Meeting Notes", type="primary", use_container_width=True, key="mm_submit"):
            if not mm_title or not mm_assessor:
                st.warning("Please fill in meeting title and assessor.")
            elif len(mm_notes) < 50:
                st.warning("Notes must be at least 50 characters.")
            elif not mm_dims:
                st.warning("Select at least one dimension.")
            else:
                with st.spinner("Indexing meeting notes..."):
                    try:
                        result = api.submit_management_meeting(mm_ticker, {
                            "meeting_title": mm_title,
                            "notes": mm_notes,
                            "assessor": mm_assessor,
                            "dimensions_discussed": mm_dims,
                            "key_findings": [f for f in mm_findings_raw.splitlines() if f.strip()],
                            "risk_flags": [r for r in mm_risks_raw.splitlines() if r.strip()],
                        })
                        st.success(f"✅ {result['message']}")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Note ID", result["note_id"])
                        col2.metric("Type", result["note_type"])
                        col3.metric("Primary Dimension", result["primary_dimension"].replace("_", " ").title())
                    except Exception as e:
                        st.error(f"Failed to submit meeting notes: {e}")

    # ------------------------------------------------------------------
    # TAB 5 — Site Visit
    # ------------------------------------------------------------------
    with tab5:
        st.markdown("### Submit Site Visit Observations")
        st.caption("Record observations from an on-site company visit")

        col1, col2 = st.columns(2)
        with col1:
            sv_ticker = st.text_input("Company Ticker", value="NVDA", placeholder="e.g. AAPL, MSFT", key="sv_ticker").strip().upper()
            sv_location = st.text_input("Location", placeholder="e.g. NVIDIA HQ, Santa Clara", key="sv_location")
            sv_assessor = st.text_input("Assessor", placeholder="analyst@firm.com", key="sv_assessor")
        with col2:
            sv_dims = st.multiselect(
                "Dimensions Observed",
                DIMENSIONS,
                format_func=lambda x: x.replace("_", " ").title(),
                key="sv_dims"
            )

        sv_observations = st.text_area(
            "Observations (min 50 chars)",
            height=200,
            placeholder="Describe what you observed during the site visit...",
            key="sv_observations"
        )
        sv_findings_raw = st.text_area("Key Findings (one per line, optional)", height=80, key="sv_findings")
        sv_risks_raw = st.text_area("Risk Flags (one per line, optional)", height=80, key="sv_risks")

        if st.button("🏭 Submit Site Visit", type="primary", use_container_width=True, key="sv_submit"):
            if not sv_location or not sv_assessor:
                st.warning("Please fill in location and assessor.")
            elif len(sv_observations) < 50:
                st.warning("Observations must be at least 50 characters.")
            elif not sv_dims:
                st.warning("Select at least one dimension.")
            else:
                with st.spinner("Indexing site visit observations..."):
                    try:
                        result = api.submit_site_visit(sv_ticker, {
                            "location": sv_location,
                            "observations": sv_observations,
                            "assessor": sv_assessor,
                            "dimensions_discussed": sv_dims,
                            "key_findings": [f for f in sv_findings_raw.splitlines() if f.strip()],
                            "risk_flags": [r for r in sv_risks_raw.splitlines() if r.strip()],
                        })
                        st.success(f"✅ {result['message']}")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Note ID", result["note_id"])
                        col2.metric("Type", result["note_type"])
                        col3.metric("Dimension", result["primary_dimension"].replace("_", " ").title())
                    except Exception as e:
                        st.error(f"Failed to submit site visit: {e}")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### 🎓 Team Information")
st.sidebar.caption("BigDataIA - Spring 2026")
st.sidebar.caption("Team 03")
st.sidebar.caption(f"App Version: 2.0")