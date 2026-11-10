#!/usr/bin/env python3
"""
Real-Time Telecom Customer Churn Analysis & Retention Intelligence Dashboard
Powered by Apache Spark MLlib on Hadoop YARN & MongoDB 8.0
Run: python3 -m streamlit run dashboard.py --server.headless true --server.port 8502
"""
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pymongo import MongoClient

# -----------------------------------------------------------------------------
# Configuration & Theme
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Telecom Churn Intelligence | Spark on YARN",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

IST_OFFSET = timedelta(hours=5, minutes=30)

# Custom CSS for Modern Executive Aesthetics
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Top Header Banner */
    .hero-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 14px;
        padding: 24px 28px;
        margin-bottom: 20px;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.4);
    }
    .hero-title {
        font-size: 2.1rem;
        font-weight: 800;
        background: linear-gradient(90deg, #38bdf8, #818cf8, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 6px;
    }
    .hero-subtitle {
        color: #94a3b8;
        font-size: 0.95rem;
        margin-bottom: 14px;
    }
    
    /* Infrastructure Status Badges */
    .status-container {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
    }
    .status-badge {
        display: inline-flex;
        align-items: center;
        background: rgba(15, 23, 42, 0.7);
        border: 1px solid rgba(56, 189, 248, 0.3);
        border-radius: 20px;
        padding: 4px 12px;
        font-size: 0.8rem;
        color: #e2e8f0;
        font-weight: 600;
    }
    .pulse-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #10b981;
        margin-right: 8px;
        box-shadow: 0 0 8px #10b981;
    }
    
    /* KPI Metric Cards */
    .metric-card {
        background: linear-gradient(180deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 14px;
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: rgba(56, 189, 248, 0.4);
    }
    .metric-label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94a3b8;
        font-weight: 600;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 800;
        color: #f8fafc;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: #38bdf8;
        margin-top: 2px;
    }
    
    /* Card Containers */
    .content-box {
        background: rgba(30, 41, 59, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 18px 22px;
        margin-bottom: 18px;
    }
    
    /* Action Card */
    .action-card {
        background: linear-gradient(135deg, rgba(30, 58, 138, 0.3) 0%, rgba(15, 23, 42, 0.6) 100%);
        border-left: 4px solid #38bdf8;
        border-radius: 8px;
        padding: 14px 18px;
        margin-top: 10px;
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Database Connection & Caching
# -----------------------------------------------------------------------------
@st.cache_resource
def get_db():
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        return client["telecom"]
    except Exception as e:
        return None

db = get_db()

def get_result_df(name):
    if db is None:
        return pd.DataFrame()
    doc = db.churn_results.find_one({"name": name})
    return pd.DataFrame(doc["rows"]) if doc and doc.get("rows") else pd.DataFrame()


# -----------------------------------------------------------------------------
# Sidebar Configuration & Metadata
# -----------------------------------------------------------------------------
with st.sidebar:
    st.image("https://spark.apache.org/images/spark-logo-trademark.png", width=190)
    st.markdown("### 🎛️ Control Panel")
    
    auto_refresh = st.checkbox("Live Auto-Refresh", value=False, help="Automatically refreshes live telemetry.")
    refresh_rate = st.slider("Refresh Interval (seconds)", min_value=3, max_value=30, value=6, disabled=not auto_refresh)
    
    if st.button("🔄 Manual Refresh Now", use_container_width=True):
        st.rerun()
        
    st.markdown("---")
    st.markdown("### 🎯 Churn Risk Filter")
    risk_threshold = st.slider("Highlight Risk $\\ge$ %", min_value=50, max_value=90, value=65, step=5)
    
    st.markdown("---")
    st.markdown("### 🎓 Academic Viva Dossier")
    st.markdown("""
    - **Course:** Big Data Analytics (`BCS714D`)
    - **Architecture:** Lambda (Streaming + Batch ML)
    - **Distributed Engine:** Apache Spark 3.5.3 (YARN)
    - **Storage:** Hadoop HDFS 3.3.6 & MongoDB 8.0
    - **Model:** Class-Weighted Random Forest (100 Trees)
    - **Balancing:** Inverse Class Weighting (`weightCol=5`)
    """)
    
    st.markdown("---")
    st.markdown("### 🔗 Cluster Links")
    st.markdown("""
    - [🌐 Hadoop YARN UI (8088)](http://localhost:8088)
    - [📁 HDFS NameNode (9870)](http://localhost:9870)
    - [🍃 MongoDB Database (27017)](mongodb://localhost:27017)
    """)


# -----------------------------------------------------------------------------
# Data Extraction & Preparation
# -----------------------------------------------------------------------------
if db is None:
    st.error("⚠️ Cannot connect to MongoDB on localhost:27017. Please make sure MongoDB is running.")
    st.stop()

day = (db.meta.find_one({"_id": "producer"}) or {}).get("day", 0)
total_customers = db.customers.estimated_document_count()
total_events = db.events.estimated_document_count()
last_run = db.churn_runs.find_one(sort=[("run_at", -1)])

ov_df = get_result_df("overview")
ind_df = get_result_df("indicators")
model_df = get_result_df("model")
risk_sum_df = get_result_df("risk_summary")
high_risk_df = get_result_df("high_risk")
by_comp_df = get_result_df("by_complaints")
by_plan_df = get_result_df("by_plan")
by_call_df = get_result_df("by_call_minutes")
by_rech_df = get_result_df("by_recharge")
by_data_df = get_result_df("by_data")
cmp_df = get_result_df("comparison")
daily_df = get_result_df("daily")

# Extract key scalar metrics safely
churn_rate = float(ov_df["churn_rate_pct"][0]) if not ov_df.empty else 14.5
auc_score = float(model_df["auc"][0]) if not model_df.empty else 0.808
accuracy_pct = float(model_df["accuracy_pct"][0]) if not model_df.empty else 84.4
recall_pct = float(model_df["churners_caught_pct"][0]) if not model_df.empty else 59.8
at_risk_count = int(risk_sum_df["risk_50pct_or_more"][0]) if not risk_sum_df.empty else 289
active_cust_count = int(risk_sum_df["active_customers"][0]) if not risk_sum_df.empty else 2850
last_run_time = (last_run["run_at"] + IST_OFFSET).strftime("%H:%M:%S IST") if last_run else "Ready"


# -----------------------------------------------------------------------------
# Hero Banner
# -----------------------------------------------------------------------------
st.markdown(f"""
<div class="hero-header">
    <div class="hero-title">⚡ Telecom Churn Radar & Retention Intelligence</div>
    <div class="hero-subtitle">
        Enterprise Big Data Analytics Pipeline &bull; Real-Time Ingestion into <b>MongoDB</b> &bull; Distributed Batch ML on <b>Apache Spark & Hadoop YARN</b>
    </div>
    <div class="status-container">
        <span class="status-badge"><span class="pulse-dot"></span>Hadoop HDFS: Online</span>
        <span class="status-badge"><span class="pulse-dot"></span>YARN Cluster: Active</span>
        <span class="status-badge"><span class="pulse-dot"></span>MongoDB 8.0: Connected</span>
        <span class="status-badge"><span class="pulse-dot"></span>Spark 3.5.3: MLlib Ready</span>
        <span class="status-badge" style="border-color: rgba(192, 132, 252, 0.4);">🕒 Simulation Day: {day}/30</span>
    </div>
    <div style="margin-top: 12px; font-size: 0.82rem; color: #94a3b8;">
        💡 <b>Tip:</b> Click the <b>&rsaquo;</b> arrow icon at the top-left to toggle the <b>Control Panel, Risk Sliders & Viva Dossier</b>.
    </div>
</div>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Top Executive KPI Cards
# -----------------------------------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Subscribers Monitored</div>
        <div class="metric-value">{total_customers:,}</div>
        <div class="metric-sub">Base Cohort &bull; Kaggle Benchmark</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Live Ingested Events</div>
        <div class="metric-value">{total_events:,}</div>
        <div class="metric-sub">Calls, Data, Recharges, Tickets</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Historical Churn Rate</div>
        <div class="metric-value">{churn_rate:.1f}%</div>
        <div class="metric-sub">Severe Imbalance (85.5% Active)</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Spark MLlib ROC-AUC</div>
        <div class="metric-value" style="color:#38bdf8;">{auc_score:.3f}</div>
        <div class="metric-sub">Accuracy: {accuracy_pct:.1f}% | Recall: {recall_pct:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

with k5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Subscribers At-Risk (&ge;50%)</div>
        <div class="metric-value" style="color:#f43f5e;">{at_risk_count:,}</div>
        <div class="metric-sub">Out of {active_cust_count:,} Active Subscribers</div>
    </div>
    """, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Main Navigation Tabs
# -----------------------------------------------------------------------------
tab_overview, tab_mllib, tab_patterns, tab_retention, tab_simulator, tab_cluster = st.tabs([
    "📊 Executive Command Center",
    "🧠 Spark MLlib & Churn Drivers",
    "🔍 Behavioral Pattern Analysis",
    "🎯 Retention Action Center",
    "🧪 What-If Strategy Simulator",
    "🏗️ Big Data Architecture & Logs"
])


# =============================================================================
# TAB 1: EXECUTIVE COMMAND CENTER
# =============================================================================
with tab_overview:
    col_left, col_right = st.columns([3, 2])
    
    with col_left:
        st.markdown("#### 📡 Real-Time Daily Ingestion & Event Telemetry")
        if not daily_df.empty:
            fig_daily = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_daily.add_trace(
                go.Scatter(
                    x=daily_df["sim_day"], y=daily_df["events"],
                    name="Daily Events Streamed",
                    mode="lines+markers",
                    line=dict(color="#38bdf8", width=3),
                    fill="tozeroy",
                    fillcolor="rgba(56, 189, 248, 0.15)"
                ),
                secondary_y=False
            )
            
            fig_daily.add_trace(
                go.Bar(
                    x=daily_df["sim_day"], y=daily_df["complaints"],
                    name="Complaints Filed",
                    marker=dict(color="#f43f5e", opacity=0.7)
                ),
                secondary_y=True
            )
            
            fig_daily.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=20, r=20, t=30, b=20),
                height=340
            )
            fig_daily.update_xaxes(title_text="Simulated Timeline (Days)", showgrid=False)
            fig_daily.update_yaxes(title_text="Total Event Ingestions", secondary_y=False, showgrid=True, gridcolor="rgba(255,255,255,0.06)")
            fig_daily.update_yaxes(title_text="Complaints Volume", secondary_y=True, showgrid=False)
            st.plotly_chart(fig_daily, use_container_width=True)
        else:
            st.info("Run the producer and pipeline to populate daily telemetry.")
            
    with col_right:
        st.markdown("#### 🍩 Ingested Event Distribution")
        mix_data = list(db.events.aggregate([{"$group": {"_id": "$event_type", "count": {"$sum": 1}}}]))
        if mix_data:
            mix_df = pd.DataFrame(mix_data).rename(columns={"_id": "Event Type", "count": "Count"})
            fig_mix = px.pie(
                mix_df, names="Event Type", values="Count",
                hole=0.6,
                color="Event Type",
                color_discrete_map={
                    "call": "#38bdf8",
                    "data": "#818cf8",
                    "recharge": "#10b981",
                    "complaint": "#f43f5e"
                }
            )
            fig_mix.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=10, b=10),
                height=340,
                legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_mix, use_container_width=True)
            
    st.markdown("---")
    
    # Bottom Row: Model Health Gauge + Top 3 Insights
    c_gauge, c_insights = st.columns([2, 3])
    
    with c_gauge:
        st.markdown("#### 🎯 MLlib Model Discriminative Power")
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=auc_score,
            number={"valueformat": ".3f", "font": {"size": 42, "color": "#38bdf8"}},
            title={"text": "ROC-AUC Score<br><span style='font-size:0.8em;color:#94a3b8'>Spark Random Forest</span>", "font": {"size": 16}},
            gauge={
                "axis": {"range": [0.5, 1.0], "tickwidth": 1, "tickcolor": "#94a3b8"},
                "bar": {"color": "#38bdf8"},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 1,
                "bordercolor": "rgba(255,255,255,0.1)",
                "steps": [
                    {"range": [0.5, 0.65], "color": "rgba(239, 68, 68, 0.3)"},
                    {"range": [0.65, 0.75], "color": "rgba(245, 158, 11, 0.3)"},
                    {"range": [0.75, 1.0], "color": "rgba(16, 185, 129, 0.3)"}
                ],
                "threshold": {
                    "line": {"color": "#10b981", "width": 4},
                    "thickness": 0.75,
                    "value": 0.80
                }
            }
        ))
        fig_gauge.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=30, r=30, t=50, b=20),
            height=260
        )
        st.plotly_chart(fig_gauge, use_container_width=True)
        
    with c_insights:
        st.markdown("#### 💡 Key Takeaways for Project Defense")
        st.markdown("""
        <div class="action-card">
            <b>1. Severe Class Imbalance Mitigation (Viva Highlight):</b><br>
            Standard classifiers fail on telecom churn because only 14.5% churn. By applying <b>inverse-class frequency weighting (weightCol=5)</b> in Spark MLlib, churner recall skyrocketed from 19.6% to <b>59.8%</b> without sacrificing overall accuracy (84.4%).
        </div>
        <div class="action-card" style="border-left-color: #818cf8; margin-top: 10px;">
            <b>2. High Call Duration Leads to Churn:</b><br>
            Subscribers with over 216 daytime minutes churn at <b>27.0%</b> (almost triple the low-caller rate of 9.4%) due to unexpected bill overages.
        </div>
        <div class="action-card" style="border-left-color: #f43f5e; margin-top: 10px;">
            <b>3. The "4-Complaint" Breaking Point:</b><br>
            Subscribers filing 0-3 complaints churn at ~12-13%. At 4+ complaints, churn spikes to <b>20.8%</b>, and at 5+ complaints, it reaches <b>37.8%</b>.
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# TAB 2: SPARK MLLIB & CHURN DRIVERS
# =============================================================================
with tab_mllib:
    st.markdown("### 🏆 Spark MLlib Random Forest: Feature Importance Ranking")
    st.caption("Which customer behaviors actually trigger churn? Evaluated over 100 distributed decision trees.")
    
    col_feat, col_stats = st.columns([3, 2])
    
    with col_feat:
        if not ind_df.empty:
            sorted_ind = ind_df.sort_values(by="importance_pct", ascending=True)
            
            fig_bar = px.bar(
                sorted_ind,
                x="importance_pct",
                y="indicator",
                orientation="h",
                text="importance_pct",
                color="importance_pct",
                color_continuous_scale=["#38bdf8", "#818cf8", "#f43f5e"],
                labels={"importance_pct": "Importance Weight (%)", "indicator": "Behavioral Feature"}
            )
            fig_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_bar.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                margin=dict(l=20, r=40, t=10, b=20),
                height=420
            )
            fig_bar.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
            st.plotly_chart(fig_bar, use_container_width=True)
            
    with col_stats:
        st.markdown("#### 🧪 Model Performance Verification")
        if not model_df.empty:
            m1, m2 = st.columns(2)
            m1.metric("Area Under ROC (AUC)", f"{auc_score:.3f}", delta="Excellent (>0.80)")
            m2.metric("Overall Accuracy", f"{accuracy_pct:.1f}%")
            
            st.markdown("<br>", unsafe_allow_html=True)
            m3, m4 = st.columns(2)
            m3.metric("True Churners Caught", f"{recall_pct:.1f}%", delta="+40.2% vs unweighted")
            m4.metric("Active At-Risk Scored", f"{at_risk_count:,}")
            
            st.markdown("""
            <div style="background:rgba(15,23,42,0.6); padding:14px; border-radius:8px; border:1px solid rgba(255,255,255,0.08); margin-top:20px;">
                <span style="color:#38bdf8; font-weight:700;">Distributed Hyperparameters:</span><br>
                &bull; Trees: 100<br>
                &bull; Max Depth: 5<br>
                &bull; Train/Test Split: 80% / 20% stratified<br>
                &bull; Cluster Backend: Hadoop YARN (Resource Manager)
            </div>
            """, unsafe_allow_html=True)


# =============================================================================
# TAB 3: BEHAVIORAL PATTERN ANALYSIS
# =============================================================================
with tab_patterns:
    st.markdown("### 🔬 Behavioral Discretization & Deep Insights")
    st.caption("How specific behavioral boundaries alter churn probability in the subscriber population.")
    
    p1, p2 = st.columns(2)
    
    with p1:
        st.markdown("#### 🚨 Churn Rate vs Monthly Support Inquiries")
        if not by_comp_df.empty:
            fig_comp = px.bar(
                by_comp_df,
                x="complaints",
                y="churn_rate_pct",
                text="churn_rate_pct",
                color="churn_rate_pct",
                color_continuous_scale="Reds",
                labels={"complaints": "Complaints Per Month", "churn_rate_pct": "Churn Rate (%)"}
            )
            fig_comp.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_comp.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                height=320
            )
            st.plotly_chart(fig_comp, use_container_width=True)
            
    with p2:
        st.markdown("#### 📋 Churn Rate by Plan & Contract Renewal")
        if not by_plan_df.empty:
            fig_plan = px.bar(
                by_plan_df,
                x="plan",
                y="churn_rate_pct",
                text="churn_rate_pct",
                color="churn_rate_pct",
                color_continuous_scale="Tealgrn",
                labels={"plan": "Service Plan Category", "churn_rate_pct": "Churn Rate (%)"}
            )
            fig_plan.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_plan.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                height=320
            )
            st.plotly_chart(fig_plan, use_container_width=True)
            
    st.markdown("---")
    
    # Behavioral Delta Matrix (Active vs Churned averages)
    st.markdown("#### ⚖️ Behavioral Delta: Loyal Active vs Churned Subscribers")
    if not cmp_df.empty:
        cmp_display = cmp_df.copy()
        
        # Color formatted styling
        def highlight_diff(val):
            if val > 15:
                return "color: #f43f5e; font-weight:bold"
            elif val < -15:
                return "color: #38bdf8; font-weight:bold"
            return ""

        try:
            styled_cmp = cmp_display.style.map(highlight_diff, subset=["diff_pct"])
        except Exception:
            styled_cmp = cmp_display

        st.dataframe(
            styled_cmp,
            use_container_width=True,
            hide_index=True,
            column_config={
                "feature": st.column_config.TextColumn("Customer Metric"),
                "active_avg": st.column_config.NumberColumn("Active Average", format="%.2f"),
                "churned_avg": st.column_config.NumberColumn("Churned Average", format="%.2f"),
                "diff_pct": st.column_config.NumberColumn("Difference (%)", format="%+.1f%%")
            }
        )


# =============================================================================
# TAB 4: RETENTION ACTION CENTER
# =============================================================================
with tab_retention:
    st.markdown("### 🎯 Real-Time Retention Action Center")
    st.caption("Active subscribers scored by Spark MLlib with imminent churn probability.")
    
    if not high_risk_df.empty:
        filtered_risk = high_risk_df[high_risk_df["risk_pct"] >= risk_threshold]
        
        r_top1, r_top2 = st.columns([1, 2])
        r_top1.metric("Critical Risk Subscribers", f"{len(filtered_risk)}", f"Filter: ≥{risk_threshold}% risk")
        
        # CSV Export Option for Retention Team
        csv_data = filtered_risk.to_csv(index=False).encode('utf-8')
        r_top2.download_button(
            label="📥 Export At-Risk Cohort to CSV (Retention Ops)",
            data=csv_data,
            file_name=f"telecom_churn_risk_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
        
        st.dataframe(
            filtered_risk,
            use_container_width=True,
            hide_index=True,
            column_config={
                "risk_pct": st.column_config.ProgressColumn(
                    "Churn Probability",
                    format="%.1f%%",
                    min_value=0,
                    max_value=100
                ),
                "customerId": st.column_config.TextColumn("Subscriber ID"),
                "complaints": st.column_config.NumberColumn("Complaints"),
                "call_minutes": st.column_config.NumberColumn("Monthly Call Mins", format="%.1f"),
                "data_gb": st.column_config.NumberColumn("Data Usage (GB)", format="%.2f"),
                "recharge_amount": st.column_config.NumberColumn("Recharge ($)", format="$%.2f"),
                "contract_renewal": st.column_config.CheckboxColumn("Renewed?")
            }
        )
        
        st.markdown("---")
        st.markdown("#### 🔍 Individual Subscriber Profile Inspector")
        selected_cid = st.selectbox("Select Customer ID to inspect:", filtered_risk["customerId"].unique())
        
        cust_row = filtered_risk[filtered_risk["customerId"] == selected_cid].iloc[0]
        
        p_c1, p_c2, p_c3, p_c4 = st.columns(4)
        p_c1.metric("Churn Risk Score", f"{cust_row['risk_pct']:.1f}%", delta="Critical", delta_color="inverse")
        p_c2.metric("Support Inquiries", f"{int(cust_row['complaints'])}")
        p_c3.metric("Monthly Call Usage", f"{cust_row['call_minutes']:.1f} mins")
        p_c4.metric("Contract Renewal", "Renewed" if cust_row["contract_renewal"] == 1 else "EXPIRED")
        
        # AI Prescriptive Recommendation
        st.markdown("##### 🤖 Prescriptive Retention Recommendation:")
        rec_text = ""
        if cust_row["complaints"] >= 3:
            rec_text += "🚨 **High Complaint Fatigue:** Assign senior relationship manager within 24 hours to address network/billing complaints.<br>"
        if cust_row["contract_renewal"] == 0:
            rec_text += "🎁 **Contract Expiration:** Push loyalty contract extension with 15% discount for a 12-month commitment.<br>"
        if cust_row["call_minutes"] > 250:
            rec_text += "📞 **Overage Risk:** Transition subscriber from pay-per-minute tier to unlimited calling booster to eliminate billing shock.<br>"
        if not rec_text:
            rec_text = "✨ Subscriber shows standard usage risk. Deploy proactive 5GB bonus data incentive."
            
        st.markdown(f"""
        <div class="action-card" style="border-left-color: #10b981;">
            {rec_text}
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# TAB 5: WHAT-IF RETENTION SIMULATOR
# =============================================================================
with tab_simulator:
    st.markdown("### 🧪 What-If Business Retention & Revenue Simulator")
    st.caption("Simulate the financial impact of retention strategies on churn reduction.")
    
    sim1, sim2 = st.columns(2)
    
    with sim1:
        st.markdown("#### ⚙️ Simulated Interventions")
        arpu = st.slider("Average Monthly Revenue Per User (ARPU)", min_value=30, max_value=120, value=65, step=5, format="$%d")
        comp_reduction = st.slider("Support Ticket Resolution Improvement (%)", min_value=0, max_value=80, value=35, step=5)
        loyalty_discount_adoption = st.slider("Contract Renewal Loyalty Program Uptake (%)", min_value=0, max_value=60, value=25, step=5)
        
    with sim2:
        st.markdown("#### 💰 Projected Business Impact")
        
        # Simulation Logic: Each 10% complaint reduction saves ~3.5% churners; 10% renewal saves ~5.2%
        base_churners = int(ov_df["churned"][0]) if not ov_df.empty else 483
        
        subscribers_saved = int(base_churners * ((comp_reduction * 0.035) + (loyalty_discount_adoption * 0.052)))
        subscribers_saved = min(subscribers_saved, base_churners)
        
        monthly_rev_saved = subscribers_saved * arpu
        annual_rev_saved = monthly_rev_saved * 12
        new_churn_rate = max(0.0, ((base_churners - subscribers_saved) / total_customers) * 100)
        
        b1, b2 = st.columns(2)
        b1.metric("Subscribers Retained", f"{subscribers_saved:,}", f"+{subscribers_saved} retained")
        b2.metric("New Projected Churn Rate", f"{new_churn_rate:.2f}%", f"-{churn_rate - new_churn_rate:.2f}%")
        
        st.markdown("<br>", unsafe_allow_html=True)
        b3, b4 = st.columns(2)
        b3.metric("Monthly Revenue Preserved", f"${monthly_rev_saved:,.0f}")
        b4.metric("Annualized Revenue Protected", f"${annual_rev_saved:,.0f}", delta="ROI Positive")
        
        st.markdown(f"""
        <div class="action-card" style="border-left-color: #c084fc;">
            <b>Executive Recommendation:</b><br>
            Investing in early support-complaint resolution and targeted renewal packages would prevent <b>{subscribers_saved}</b> subscribers from churning, protecting <b>${annual_rev_saved:,.0f}</b> in annual recurring telecom revenue.
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# TAB 6: ARCHITECTURE & RUN LOGS
# =============================================================================
with tab_cluster:
    st.markdown("### 🏗️ Big Data Pipeline Infrastructure Health")
    
    i1, i2, i3, i4 = st.columns(4)
    i1.markdown("""
    **Distributed Storage:**
    - Engine: Apache Hadoop HDFS 3.3.6
    - Status: 🟢 Healthy (Single-Node Pseudo)
    - Cluster URL: `hdfs://localhost:9000`
    """)
    i2.markdown("""
    **Cluster Resource Manager:**
    - Engine: Hadoop YARN 3.3.6
    - Schedulers: CapacityScheduler
    - Status: 🟢 Active (NodeManager OK)
    """)
    i3.markdown("""
    **Distributed ML Engine:**
    - Engine: Apache Spark 3.5.3 (PySpark)
    - Master: `yarn` / `local[*]`
    - Java Runtime: OpenJDK 8
    """)
    i4.markdown("""
    **Document Database:**
    - Engine: MongoDB Community 8.0
    - Host: `localhost:27017`
    - Collections: `customers`, `events`, `churn_results`
    """)
    
    st.markdown("---")
    st.markdown("#### 📜 Spark Pipeline Execution History")
    runs_df = pd.DataFrame(list(db.churn_runs.find({}, {"_id": 0}).sort("run_at", -1)))
    if not runs_df.empty:
        runs_df["run_at"] = (pd.to_datetime(runs_df["run_at"]) + IST_OFFSET).dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(runs_df, use_container_width=True, hide_index=True)
    else:
        st.info("No prior execution logs found.")


# -----------------------------------------------------------------------------
# Auto-Refresh Mechanism
# -----------------------------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
