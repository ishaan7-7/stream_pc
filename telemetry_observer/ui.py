import streamlit as st
import pandas as pd
import asyncio
import threading
import time
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# --- Path Setup ---
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# Import the Engine (Phase 1)
from telemetry_observer.observer_backend import HybridObserver

# ---------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------
st.set_page_config(
    page_title="Telemetry Command Center",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .status-box {
        padding: 8px; border-radius: 4px; text-align: center; 
        font-weight: 600; color: white; margin-bottom: 5px; font-size: 0.85em;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .status-up { background-color: #2e7d32; } 
    .status-down { background-color: #c62828; }
    div[data-testid="stMetricValue"] { font-size: 24px; color: #e0e0e0; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# Singleton Pattern: Background Engine
# ---------------------------------------------------------
@st.cache_resource
def get_running_observer():
    observer = HybridObserver()
    def start_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(observer.start())
    t = threading.Thread(target=start_loop, daemon=True)
    t.start()
    time.sleep(1)
    return observer

observer = get_running_observer()

# Fetch Data
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
snapshot = loop.run_until_complete(observer.get_snapshot())

# ---------------------------------------------------------
# TOP BAR
# ---------------------------------------------------------
st.title("📡 Telemetry Command Center")

ports = snapshot.get("system_health", {})
if ports:
    cols = st.columns(len(ports))
    for idx, (service_name, is_up) in enumerate(ports.items()):
        status_class = "status-up" if is_up else "status-down"
        icon = "ACTIVE" if is_up else "OFFLINE"
        with cols[idx]:
            st.markdown(
                f'<div class="status-box {status_class}">{service_name}<br><small>{icon}</small></div>', 
                unsafe_allow_html=True
            )

st.markdown("---")

# ---------------------------------------------------------
# GLOBAL KPIS
# ---------------------------------------------------------
stats = snapshot.get("global_stats", {})
k1, k2, k3, k4 = st.columns(4)

with k1: st.metric("Total Throughput", f"{stats.get('total_rows', 0):,}")
with k2: st.metric("Active Fleet Size", stats.get("active_vehicles", 0))
with k3: 
    lat = stats.get("avg_latency", 0)
    st.metric("Global Latency", f"{lat} ms", delta_color="inverse")
with k4: 
    dlq = stats.get("dlq_backlog", 0)
    st.metric("DLQ Backlog", dlq, delta_color="inverse")

# ---------------------------------------------------------
# MAIN INTERFACE
# ---------------------------------------------------------
vehicles = snapshot.get("vehicles", [])

if vehicles:
    df = pd.DataFrame(vehicles)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏆 Leaderboard", 
        "📊 Fleet Analytics", 
        "🛠️ Module Forensics", 
        "🔍 Live Inspector"
    ])

    # --- TAB 1: LEADERBOARD ---
    with tab1:
        display_df = df[[
            "vehicle_id", "rows_processed", "rejected_rows", 
            "validation_rate", "avg_latency", "last_seen_sec"
        ]].copy()
        
        st.dataframe(
            display_df,
            column_config={
                "vehicle_id": "Vehicle ID",
                "rows_processed": st.column_config.NumberColumn("Rows", format="%d"),
                "validation_rate": st.column_config.ProgressColumn(
                    "Quality Score", format="%.1f%%", min_value=0, max_value=100
                ),
                "avg_latency": st.column_config.NumberColumn("Latency", format="%.1f ms"),
                "last_seen_sec": st.column_config.NumberColumn("Last Seen", format="%.1f s")
            },
            use_container_width=True,
            hide_index=True
        )

    # --- TAB 2: FLEET ANALYTICS ---
    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Latency Heatmap")
            fig_lat = px.bar(
                df, x="vehicle_id", y="avg_latency", 
                color="avg_latency", color_continuous_scale="RdYlGn_r"
            )
            st.plotly_chart(fig_lat, use_container_width=True)
            
        with c2:
            st.subheader("Data Quality Distribution")
            chart_data = df.melt(id_vars="vehicle_id", value_vars=["rows_processed", "rejected_rows"], var_name="Type", value_name="Count")
            fig_qual = px.bar(
                chart_data, x="vehicle_id", y="Count", color="Type",
                color_discrete_map={"rows_processed": "#2e7d32", "rejected_rows": "#c62828"},
                barmode="stack"
            )
            st.plotly_chart(fig_qual, use_container_width=True)

    # --- TAB 3: MODULE FORENSICS ---
    with tab3:
        v_ids = sorted(df["vehicle_id"].tolist())
        col_sel1, col_sel2 = st.columns([1, 3])
        
        with col_sel1:
            selected_v_forensics = st.selectbox("Select Vehicle:", v_ids, key="forensics_v")
        
        v_data = next((v for v in vehicles if v["vehicle_id"] == selected_v_forensics), None)
        
        if v_data and "history" in v_data and v_data["history"]:
            available_modules = list(v_data["history"].keys())
            if available_modules:
                with col_sel1:
                    selected_mod = st.radio("Select Module:", available_modules)
                
                with col_sel2:
                    mod_hist = v_data["history"][selected_mod]
                    timestamps = mod_hist["timestamps"]
                    metrics = mod_hist["metrics"]
                    
                    if not metrics:
                        st.warning("No numeric metrics found in this module yet.")
                    else:
                        st.subheader(f"Analyzing: {selected_v_forensics} ➡️ {selected_mod.upper()}")
                        
                        kpi_cols = st.columns(min(len(metrics), 4))
                        for idx, (metric_name, values) in enumerate(metrics.items()):
                            if idx < 4:
                                latest_val = values[-1] if values else 0
                                with kpi_cols[idx]:
                                    st.metric(metric_name.replace("_", " ").title(), f"{latest_val}")
                        
                        st.markdown("---")
                        
                        for metric_name, values in metrics.items():
                            min_len = min(len(timestamps), len(values))
                            chart_df = pd.DataFrame({
                                "Time": timestamps[-min_len:],
                                "Value": values[-min_len:]
                            })
                            fig = px.line(chart_df, x="Time", y="Value", title=f"{metric_name.replace('_', ' ').title()}")
                            fig.update_traces(line_color='#00acc1')
                            fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                            st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No module history available yet.")

    # --- TAB 4: LIVE INSPECTOR (UPDATED) ---
    with tab4:
        # Selector
        selected_v = st.selectbox("Inspect Vehicle:", v_ids, key="inspect_v")
        
        if selected_v:
            v_data = next((v for v in vehicles if v["vehicle_id"] == selected_v), None)
            
            if v_data:
                # 1. Module Selector Logic
                module_map = v_data.get("module_payloads", {})
                
                # Create list of options: [ALL (Latest), engine, tyre, ...]
                available_mods = sorted(list(module_map.keys()))
                options = ["ALL (Latest)"] + available_mods
                
                col_i1, col_i2 = st.columns([1, 3])
                
                with col_i1:
                    # Health Card
                    st.markdown(f"### 🩺 Status: {selected_v}")
                    st.success(f"Accepted: {v_data['rows_processed']}")
                    if v_data['rejected_rows'] > 0:
                        st.error(f"Rejected: {v_data['rejected_rows']}")
                    
                    st.markdown("---")
                    selected_payload_mod = st.radio("Select Payload Source:", options)
                
                with col_i2:
                    # JSON Display Logic
                    if selected_payload_mod == "ALL (Latest)":
                        payload_to_show = v_data.get("latest_payload", {})
                        source_mod = payload_to_show.get("metadata", {}).get("module", "Unknown")
                        st.subheader(f"Incoming Stream (Last Source: {source_mod})")
                    else:
                        payload_to_show = module_map[selected_payload_mod]
                        st.subheader(f"Latest Packet: {selected_payload_mod.upper()}")

                    if payload_to_show:
                        st.json(payload_to_show, expanded=True)
                    else:
                        st.warning("No data packet received yet.")

else:
    st.info("🚀 Waiting for Telemetry Stream... Please start your Replay Service!")

# Auto-Refresh
time.sleep(1)
st.rerun()