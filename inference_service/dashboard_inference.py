
import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from deltalake import DeltaTable
import pyarrow.compute as pc
import time

# --- CONFIGURATION ---
INFERENCE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(INFERENCE_DIR)
SILVER_DIR = os.path.join(ROOT_DIR, "data", "delta", "silver")
MODULES = ["engine", "battery", "body", "transmission", "tyre"]

st.set_page_config(page_title="Inference Ops Dashboard", layout="wide", page_icon="🧠")

# Auto-refresh logic (every 2 seconds)
st_autorefresh = st.empty()

# --- HELPER FUNCTIONS ---
@st.cache_data(ttl=2, show_spinner=False)
def load_system_alerts():
    all_alerts = []
    for mod in MODULES:
        alerts_file = os.path.join(INFERENCE_DIR, "state", f"system_alerts_{mod}.json")
        if os.path.exists(alerts_file):
            try:
                with open(alerts_file, 'r') as f:
                    all_alerts.extend(json.load(f))
            except Exception: pass
    return sorted(all_alerts, key=lambda x: x['timestamp'], reverse=True)

@st.cache_data(ttl=2, show_spinner=False)
def load_silver_metrics():
    metrics = {"sims": [], "latencies": [], "module_stats": {}}
    latest_rows = {}
    read_errors = []

    # Time window: Only fetch rows written in the last 5 minutes
    cutoff_dt = datetime.utcnow() - timedelta(minutes=5)
    cutoff_str = cutoff_dt.isoformat()

    for mod in MODULES:
        path = os.path.join(SILVER_DIR, mod)
        if not os.path.exists(path):
            continue
            
        try:
            dt = DeltaTable(path)
            dataset = dt.to_pyarrow_dataset()
            
            # OPTIMIZATION: Do not load the whole table. Filter directly at the Parquet level.
            if 'inference_ts' in dataset.schema.names:
                filtered_table = dataset.scanner(filter=pc.field("inference_ts") > cutoff_str).to_table()
                df = filtered_table.to_pandas()
            else:
                df = dt.to_pandas() # Fallback if schema doesn't have it yet

            if df.empty: 
                continue

            # Ensure datetime objects
            df['inference_ts'] = pd.to_datetime(df['inference_ts'], utc=True)
            df['ingest_ts'] = pd.to_datetime(df['ingest_ts'], utc=True)
            
            # Compute Latencies (in milliseconds)
            df['e2e_latency_ms'] = (df['inference_ts'] - df['ingest_ts']).dt.total_seconds() * 1000
            
            if 'writer_ts' in df.columns and not df['writer_ts'].isnull().all():
                df['writer_ts'] = pd.to_datetime(df['writer_ts'], utc=True)
                df['inference_latency_ms'] = (df['inference_ts'] - df['writer_ts']).dt.total_seconds() * 1000
            else:
                df['inference_latency_ms'] = df['e2e_latency_ms'] 
            
            # Aggregate Sims (Using Lists instead of Sets to prevent Streamlit cache choking)
            if 'source_id' in df.columns:
                current_sims = df['source_id'].unique().tolist()
                metrics["sims"] = list(set(metrics["sims"] + current_sims))
                
            avg_e2e = df['e2e_latency_ms'].mean()
            avg_inf = df['inference_latency_ms'].mean()
            
            metrics["latencies"].append((avg_e2e, avg_inf))
            metrics["module_stats"][mod] = {
                "e2e": avg_e2e,
                "inf": avg_inf,
                "rows_5m": len(df)
            }
                
            # Grab the last 10 rows for the "Tail" view
            tail_df = df.sort_values('inference_ts', ascending=False).head(10)
            latest_rows[mod] = tail_df
            
        except Exception as e:
            read_errors.append(f"[{mod.upper()}]: {str(e)}")

    return metrics, latest_rows, read_errors

# --- DASHBOARD UI ---

st.title("🧠 Inference Service Operations")

# 1. READ ERRORS (If DeltaLake is locked or throwing errors, show it!)
metrics, latest_rows, read_errors = load_silver_metrics()

if read_errors:
    st.error("❌ **Storage Read Errors Detected:**")
    for err in read_errors:
        st.code(err)

# 2. ALERTS BANNER
alerts = load_system_alerts()
recent_alerts = [a for a in alerts if (datetime.utcnow() - datetime.fromisoformat(a['timestamp'])).total_seconds() < 300]

if recent_alerts:
    st.warning("⚠️ **SYSTEM ALERTS DETECTED (Last 5 Mins)**")
    for a in recent_alerts[:5]:
        icon = "🚨" if a['level'] == "CRITICAL" else "⚠️"
        st.caption(f"{icon} **{a['timestamp']} | {a['sim_id']} ({a['module']}):** {a['message']}")

# 3. KPI METRICS
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Active Streams (Sims)", len(metrics["sims"]))
with col2:
    st.metric("Active Modules", len(metrics["module_stats"]))
with col3:
    global_e2e = sum([x[0] for x in metrics["latencies"]]) / len(metrics["latencies"]) if metrics["latencies"] else 0
    st.metric("Global E2E Latency", f"{global_e2e:.0f} ms")
with col4:
    global_inf = sum([x[1] for x in metrics["latencies"]]) / len(metrics["latencies"]) if metrics["latencies"] else 0
    st.metric("Global Inference Latency", f"{global_inf:.0f} ms")

st.markdown("---")

# 4. MODULE LATENCY BREAKDOWN
st.subheader("⏱️ Latency by Module")
if metrics["module_stats"]:
    mod_df = pd.DataFrame.from_dict(metrics["module_stats"], orient='index')
    mod_df.columns = ["E2E Latency (ms)", "Inference Latency (ms)", "Rows Processed (Last 5m)"]
    st.dataframe(mod_df.style.format("{:.0f}"), use_container_width=True)
else:
    st.info("No inference data processed in the last 5 minutes. Waiting for stream...")

st.markdown("---")

# 5. LIVE SILVER TAIL
st.subheader("🔍 Live Silver Data (Tail)")

if latest_rows:
    col_mod, col_sim = st.columns(2)
    with col_mod:
        selected_mod = st.selectbox("Select Module", list(latest_rows.keys()))
    
    view_df = latest_rows[selected_mod]
    
    with col_sim:
        sims_available = ["All"]
        if 'source_id' in view_df.columns:
            sims_available += view_df['source_id'].unique().tolist()
        selected_sim = st.selectbox("Filter by Sim", sims_available)
        
    if selected_sim != "All" and 'source_id' in view_df.columns:
        view_df = view_df[view_df['source_id'] == selected_sim]
        
    # Safely select columns that actually exist
    desired_cols = ['source_id', 'timestamp', 'inference_ts', 'composite_score', 'severity', 'top_features']
    display_cols = [c for c in desired_cols if c in view_df.columns]
    
    st.dataframe(view_df[display_cols], use_container_width=True)
else:
    st.caption("Silver Delta tables are empty.")

# Auto-refresh hook
time.sleep(2)
st.rerun()
