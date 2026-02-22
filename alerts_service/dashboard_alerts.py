import streamlit as st
import pandas as pd
import os
import time
import json
import plotly.express as px
import pyarrow.compute as pc
from deltalake import DeltaTable

from src import config as alert_config
from DTC_service.analyzer import DTCAdapter

ALERTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(ALERTS_DIR)
GOLD_ALERTS_DIR = os.path.join(ROOT_DIR, "data", "delta", "gold", "alerts")
CHECKPOINT_FILE = os.path.join(ALERTS_DIR, "state", "checkpoints.json")

PRIMARY_MODULE = alert_config.ENABLED_MODULES[0] if alert_config.ENABLED_MODULES else "engine"
SILVER_PRIMARY_DIR = os.path.join(ROOT_DIR, "data", "delta", "silver", PRIMARY_MODULE)

st.set_page_config(page_title="Alerts Dashboard", layout="wide", page_icon="🚨")

# --- UI CONTROLS ---
col_title, col_toggle = st.columns([3, 1])
with col_title:
    st.title("🚨 Operations Center: Fleet Alerts")
with col_toggle:
    st.write("")
    st.write("")
    auto_refresh = st.toggle("Enable Live Auto-Refresh", value=False, help="Disable during Deep Dive analysis to prevent graphs from closing.")

@st.cache_data(ttl=2 if auto_refresh else 60, show_spinner=False)
def load_dashboard_data():
    df_alerts = pd.DataFrame()
    lag_rows = 0

    if os.path.exists(GOLD_ALERTS_DIR) and DeltaTable.is_deltatable(GOLD_ALERTS_DIR):
        try:
            df_alerts = DeltaTable(GOLD_ALERTS_DIR).to_pandas()
            df_alerts['alert_start_ts'] = pd.to_datetime(df_alerts['alert_start_ts'])
            df_alerts['alert_end_ts'] = pd.to_datetime(df_alerts['alert_end_ts'])
        except Exception: pass

    try:
        if os.path.exists(CHECKPOINT_FILE) and os.path.exists(SILVER_PRIMARY_DIR):
            with open(CHECKPOINT_FILE, 'r') as f:
                ckpt = json.load(f)
            last_ts = ckpt.get(PRIMARY_MODULE, "1970-01-01T00:00:00")
            dt_silver = DeltaTable(SILVER_PRIMARY_DIR).to_pyarrow_dataset()
            if 'inference_ts' in dt_silver.schema.names:
                filtered = dt_silver.scanner(filter=pc.field("inference_ts") > last_ts).to_table()
                lag_rows = len(filtered)
    except Exception: pass

    return df_alerts, lag_rows

def render_plot(df_buildup, title, color_theme):
    """Helper to render the S-Curve buildup plotly graphs."""
    cols_to_plot = [c for c in df_buildup.columns if c != 'timestamp']
    if len(cols_to_plot) == 0: return None
    
    plot_df = df_buildup.melt(id_vars=['timestamp'], value_vars=cols_to_plot, var_name='DTC_Code', value_name='Risk_Level')
    fig = px.line(plot_df, x='timestamp', y='Risk_Level', color='DTC_Code', title=title, color_discrete_sequence=color_theme)
    fig.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="100% Failure Trigger")
    fig.update_yaxes(range=[0, 1.1])
    return fig

df, lag = load_dashboard_data()

# --- KPIs ---
open_alerts_count = len(df[df['status'] == "OPEN"]) if not df.empty else 0
c1, c2, c3 = st.columns(3)
c1.metric("🔴 Active Alerts", open_alerts_count)
c2.metric("⚠️ Critical Vehicles", df[df['status'] == "OPEN"]['source_id'].nunique() if not df.empty else 0)
c3.metric(f"Processing Lag ({PRIMARY_MODULE.capitalize()})", lag, delta_color="inverse")

st.markdown("---")

def render_alert_table(alert_df, section_title):
    st.subheader(section_title)
    if alert_df.empty:
        st.success("No records found.")
        return

    # Header Row
    st.markdown("""
    <div style="display: flex; font-weight: bold; border-bottom: 2px solid #444; padding-bottom: 5px;">
        <div style="flex: 2;">Alert ID</div>
        <div style="flex: 1;">Module</div>
        <div style="flex: 1;">Vehicle</div>
        <div style="flex: 2;">Peak TS</div>
        <div style="flex: 2;">Action</div>
    </div>
    """, unsafe_allow_html=True)
    st.write("")

    # Data Rows
    for idx, row in alert_df.iterrows():
        c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 2])
        alert_id = str(row['alert_id'])
        peak_ts_str = str(row['peak_anomaly_ts'])[:19]
        
        c1.write(f"`{alert_id[:8]}`")
        c2.write(row['module'].upper())
        c3.write(row['source_id'])
        c4.write(peak_ts_str)
        
        # The Deep Dive Button
        btn_clicked = c5.button("🔬 Root Cause DTC", key=f"btn_{alert_id}", use_container_width=True)
        
        if btn_clicked:
            if auto_refresh:
                st.warning("⚠️ Disable Auto-Refresh at the top to prevent the graph from closing on the next data tick.")
            
            with st.container(border=True):
                st.markdown(f"#### 🧠 Neural Deep Dive: {row['source_id']} ({row['module'].upper()})")
                
                with st.spinner(f"Loading {row['module']} artifacts and fetching Traceback data..."):
                    adapter = DTCAdapter(module_name=row['module'])
                    silver_df = adapter.fetch_traceback(row['source_id'], row['peak_anomaly_ts'])
                
                if silver_df.empty:
                    st.error("Traceback data not found in Silver table.")
                else:
                    with st.spinner("Running PyTorch Inference and mathematical integration..."):
                        df_crit, df_noncrit, triggers, diagnostics = adapter.run_diagnosis(silver_df)
                    
                    # --- NEW: PRINT DIAGNOSTICS FOR SILENT FAILURES ---
                    if diagnostics["models_loaded"] == 0:
                        st.error(f"⚠️ **CRITICAL ERROR:** No trained PyTorch models found in memory for the '{row['module'].upper()}' module. Check your `DTC/artifacts` folder.")
                    
                    if diagnostics["skipped_dtcs"]:
                        with st.expander("⚠️ Schema Mismatch: Some DTC codes were skipped because their required features are missing in the Silver table!", expanded=True):
                            for code, missing in diagnostics["skipped_dtcs"].items():
                                st.warning(f"**{code} Ignored:** Missing columns -> `{missing}`")
                    # ---------------------------------------------------

                    # 1. Print Master Messages
                    st.markdown("##### 📋 Diagnostic Output")
                    if triggers:
                        for t in triggers:
                            if t["severity"] == "CRITICAL": st.error(f"**🚨 {t['code']} (CRITICAL):** {t['message']}")
                            else: st.warning(f"**⚠️ {t['code']} (WARNING):** {t['message']}")
                    elif not diagnostics["skipped_dtcs"] and diagnostics["models_loaded"] > 0:
                        st.success("✅ Neural Network found no specific known DTC signatures. (Likely general physical wear).")
                    else:
                        st.error("❌ Diagnosis incomplete due to missing sensor data (see warnings above).")

                    # 2. Render Dual Plots
                    p1, p2 = st.columns(2)
                    with p1:
                        fig_crit = render_plot(df_crit, "Critical Fault Maturation", px.colors.qualitative.Set1)
                        if fig_crit: st.plotly_chart(fig_crit, use_container_width=True)
                        else: st.info("No Critical DTCs monitored for this module.")
                    
                    with p2:
                        fig_noncrit = render_plot(df_noncrit, "Non-Critical Fault Maturation", px.colors.qualitative.Pastel1)
                        if fig_noncrit: st.plotly_chart(fig_noncrit, use_container_width=True)
                        else: st.info("No Non-Critical DTCs monitored for this module.")
        st.write("")

# --- Render Live Triage ---
if not df.empty:
    open_alerts = df[df['status'] == "OPEN"].sort_values('peak_anomaly_ts', ascending=False)
    render_alert_table(open_alerts, "🔴 LIVE TRIAGE: Active Alerts")
else:
    render_alert_table(pd.DataFrame(), "🔴 LIVE TRIAGE: Active Alerts")

st.markdown("---")

# --- Render History ---
if not df.empty:
    closed_alerts = df[df['status'] == "CLOSED"].sort_values('alert_end_ts', ascending=False)
    render_alert_table(closed_alerts, "📜 Alerts History (Resolved)")
else:
    render_alert_table(pd.DataFrame(), "📜 Alerts History (Resolved)")

if auto_refresh:
    time.sleep(2)
    st.rerun()