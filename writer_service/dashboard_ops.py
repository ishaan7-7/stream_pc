# File: C:\streaming_emulator\writer_service\dashboard_ops.py
import streamlit as st
import json
import time
import pandas as pd
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Writer Ops | Mission Control", 
    page_icon="⚡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- STYLING ---
st.markdown("""
<style>
    .main { background-color: #0E1117; }
    .metric-container {
        background-color: #262730;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #4CAF50;
        margin-bottom: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .status-box {
        padding: 2px 8px; border-radius: 4px; font-weight: bold; font-size: 0.8em; float: right;
    }
    .status-running { background-color: #1B5E20; color: #A5D6A7; border: 1px solid #2E7D32; }
    .status-stalled { background-color: #E65100; color: #FFCC80; border: 1px solid #EF6C00; }
    .status-stopped { background-color: #B71C1C; color: #FFCDD2; border: 1px solid #C62828; }
    .metric-label { font-size: 0.8em; color: #9E9E9E; }
    .metric-value { font-size: 1.2em; font-weight: bold; color: #E0E0E0; margin-top: 5px;}
    div[data-testid="stMetricValue"] { font-size: 24px; color: #e0e0e0; }
</style>
""", unsafe_allow_html=True)

# --- CONFIG ---
ROOT_DIR = Path(__file__).parent
METRICS_DIR = ROOT_DIR / "state"
DELTA_ROOT = ROOT_DIR.parent / "data" / "delta" / "bronze"
MODULES = ["engine", "body", "battery", "transmission", "tyre"]
KAFKA_BROKER = "localhost:9092"

# --- PERSISTENT BACKEND (The Fix) ---
class TelemetryBackend:
    """
    Holds the Kafka Consumer in memory to prevent slow re-connections 
    on every refresh.
    """
    def __init__(self):
        self.consumer = None
        self._connect()
        
    def _connect(self):
        try:
            conf = {
                'bootstrap.servers': KAFKA_BROKER, 
                'group.id': 'dashboard_ops_v2', 
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            self.consumer = Consumer(conf)
        except Exception as e:
            st.error(f"Kafka Connection Failed: {e}")

    def get_kafka_counts(self):
        counts = {}
        if not self.consumer:
            self._connect()
            
        for m in MODULES:
            topic = f"telemetry.{m}"
            total = 0
            try:
                # Fast metadata fetch (reusing connection)
                meta = self.consumer.list_topics(topic, timeout=0.5)
                if topic in meta.topics:
                    parts = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
                    for p in parts:
                        low, high = self.consumer.get_watermark_offsets(p, timeout=0.5, cached=False)
                        total += high
            except: 
                # If connection died, try once to reconnect
                pass
            counts[m] = total
        return counts

    def close(self):
        if self.consumer:
            self.consumer.close()

# --- SESSION STATE MANAGEMENT ---
if "backend" not in st.session_state:
    st.session_state.backend = TelemetryBackend()
if "inspector_df" not in st.session_state:
    st.session_state.inspector_df = None

# --- HELPER FUNCTIONS ---
def get_delta_counts():
    delta_counts = {}
    for m in MODULES:
        log_path = DELTA_ROOT / m / "_delta_log"
        total = 0
        if log_path.exists():
            # Quick scan of JSON logs
            json_files = list(log_path.glob("*.json"))
            for jf in json_files:
                try:
                    with open(jf, "r") as f:
                        for line in f:
                            if "numRecords" in line:
                                action = json.loads(line)
                                if "add" in action:
                                    stats = json.loads(action["add"].get("stats", "{}"))
                                    total += int(stats.get("numRecords", 0))
                except: pass
        delta_counts[m] = total
    return delta_counts

def load_writer_metrics():
    data = {}
    for m in MODULES:
        p = METRICS_DIR / f"writer_metrics_{m}.json"
        if p.exists():
            try:
                with open(p, 'r') as f: data[m] = json.load(f)
            except: data[m] = {"status": "OFFLINE"}
        else: data[m] = {"status": "OFFLINE"}
    return data

def get_latest_parquet_rows(module):
    path = DELTA_ROOT / module
    if not path.exists(): return None
    files = list(path.rglob("*.parquet"))
    if not files: return None
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    try:
        for f in files[:3]: 
            try:
                df = pd.read_parquet(f)
                if not df.empty:
                    if "ingest_ts" in df.columns:
                        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"])
                        df = df.sort_values("ingest_ts", ascending=False)
                    return df.head(5)
            except: continue
    except: pass
    return None

# --- UI LOGIC ---

st.title("⚡ Writer Ops | Mission Control")

# NAVIGATION
st.sidebar.header("Navigation")
view_mode = st.sidebar.radio("Select View:", ["📊 Operations Board", "🔍 Data Inspector"])

# FETCH DATA (Using Persistent Backend)
k_counts = st.session_state.backend.get_kafka_counts() # <--- FAST NOW
d_counts = get_delta_counts()
w_metrics = load_writer_metrics()

# PROCESS DATA
comp_data = []
for m in MODULES:
    k_total = k_counts.get(m, 0)
    d_total = d_counts.get(m, 0)
    true_lag = k_total - d_total
    
    spark_data = w_metrics.get(m, {})
    stream_data = spark_data.get("streams", {}).get(m, {})
    speed = stream_data.get("process_rate", 0.0)
    latency = stream_data.get("duration_ms", 0)
    
    status = spark_data.get("status", "OFFLINE")
    if status == "RUNNING" and (time.time() - spark_data.get("last_updated", 0) > 10):
        status = "STALLED"
        
    comp_data.append({
        "Module": m.upper(),
        "Status": status,
        "Kafka Total": k_total,
        "Delta Total": d_total,
        "True Lag": true_lag,
        "Speed (r/s)": speed,
        "Latency (ms)": latency
    })

df_comp = pd.DataFrame(comp_data)

# --- VIEW 1: OPERATIONS BOARD ---
if view_mode == "📊 Operations Board":
    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("⚡ Auto-Refresh", value=True)
    refresh_rate = st.sidebar.slider("Rate (s)", 2, 10, 3)

    # HEADLINE METRICS
    total_written = df_comp["Delta Total"].sum()
    total_lag = df_comp["True Lag"].sum()
    active_df = df_comp[df_comp["Status"] == "RUNNING"]
    active = len(active_df)
    avg_latency = active_df["Latency (ms)"].mean() if not active_df.empty else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active Writers", f"{active}/5")
    c2.metric("Total Written", f"{total_written:,.0f}")
    c3.metric("True System Lag", f"{total_lag:,.0f}", delta_color="inverse")
    c4.metric("Avg Latency", f"{avg_latency:.0f} ms")
    c5.metric("System Health", "OK" if total_lag < 5000 else "LAGGING")

    st.markdown("---")
    
    # TABLE
    st.subheader("Kafka vs. Delta Sync Status")
    def style_lag(val):
        color = 'green' if val < 100 else 'orange' if val < 1000 else 'red'
        return f'color: {color}; font-weight: bold'

    try:
        st.dataframe(
            df_comp.style.map(style_lag, subset=['True Lag'])
                   .format({"Kafka Total": "{:,}", "Delta Total": "{:,}", "True Lag": "{:,}", "Speed (r/s)": "{:.1f}"}),
            use_container_width=True
        )
    except AttributeError:
        st.dataframe(df_comp) # Fallback

    # CARDS (Using stable columns, no placeholders)
    st.markdown("### Module Telemetry")
    cols = st.columns(5)
    for i, row in df_comp.iterrows():
        with cols[i]:
            css = "status-stopped"
            if row["Status"] == "RUNNING": css = "status-running"
            elif row["Status"] == "STALLED": css = "status-stalled"
            
            st.markdown(f"""
            <div class="metric-container">
                <div>
                    <b>{row['Module']}</b>
                    <span class="status-box {css}">{row['Status']}</span>
                </div>
                <div class="metric-value">{row['True Lag']:,}</div>
                <div class="metric-label">Current Lag</div>
            </div>
            """, unsafe_allow_html=True)

    # REFRESH
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()

# --- VIEW 2: DATA INSPECTOR ---
elif view_mode == "🔍 Data Inspector":
    st.markdown("### 🔍 Partition Inspector")
    st.info("ℹ️ Auto-refresh is **PAUSED** in Inspector mode.")
    
    c_sel, c_btn = st.columns([3, 1])
    with c_sel:
        sel_mod = st.selectbox("Select Module", MODULES)
    with c_btn:
        st.write("") 
        st.write("")
        if st.button("Fetch Latest Rows"):
            st.session_state.inspector_df = get_latest_parquet_rows(sel_mod)
    
    if st.session_state.inspector_df is not None:
        st.dataframe(st.session_state.inspector_df, use_container_width=True)
    else:
        st.warning("Select a module and click 'Fetch Latest Rows'.")