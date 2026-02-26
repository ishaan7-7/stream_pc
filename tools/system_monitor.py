# File: C:\streaming_emulator\tools\system_monitor.py
import streamlit as st
import psutil
import pandas as pd
import time
import os
import json
import altair as alt

st.set_page_config(page_title="System Watchtower", page_icon="🛡️", layout="wide")

# --- UI THEME INJECTION (Matching React theme.ts) ---
st.markdown("""
<style>
    /* Base Theme */
    .stApp { background-color: #f4f6f8; color: #2c3e50; font-family: "Roboto", "Helvetica", "Arial", sans-serif; }
    
    /* Paper Components (Cards) */
    .metric-card { 
        background-color: #ffffff; 
        padding: 20px; 
        border: 1px solid #e0e0e0; 
        border-radius: 0px; /* Flat industrial look */
        margin-bottom: 15px; 
    }
    
    /* Typography */
    .card-title { font-size: 0.9rem; font-weight: 600; letter-spacing: 0.5px; text-transform: uppercase; color: #666; margin-bottom: 10px; }
    .card-value { font-size: 1.8rem; font-weight: bold; color: #2c3e50; }
    .card-sub { font-size: 0.85rem; color: #888; margin-top: 5px; }
    
    /* Status Indicators */
    .status-ok { border-left: 4px solid #2e7d32; }
    .status-warn { border-left: 4px solid #ed6c02; background-color: #fff8e1; }
    .status-crit { border-left: 4px solid #d32f2f; background-color: #ffebee; color: #d32f2f !important; }
    .status-crit .card-value { color: #d32f2f !important; }
    
    /* Table overriding */
    [data-testid="stDataFrame"] { border-radius: 0px !important; border: 1px solid #e0e0e0; }
</style>
""", unsafe_allow_html=True)


# --- CONSTANTS & CONFIG ---
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TARGET_PROCESSES = ['python', 'java', 'cmd', 'powershell']

# --- STORAGE & I/O OPERATIONS (DELAYED 20 MINS) ---

def format_bytes(size):
    """Formats bytes to KB, MB, GB adhering strictly to base 1024."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def get_dir_size(path):
    """Fast directory size calculation using os.scandir for Windows NTFS."""
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    except Exception:
        pass
    return total

def get_delta_row_count(delta_path):
    """
    Lightweight JSON parser to read row counts directly from Delta transaction logs.
    Bypasses PySpark entirely to save JVM RAM overhead.
    """
    log_path = os.path.join(delta_path, "_delta_log")
    if not os.path.exists(log_path):
        return 0
        
    total_rows = 0
    try:
        for entry in os.scandir(log_path):
            if entry.name.endswith('.json'):
                with open(entry.path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            # Append-only fast extraction
                            if 'add' in data and 'stats' in data['add']:
                                stats = json.loads(data['add']['stats'])
                                total_rows += stats.get('numRecords', 0)
                        except Exception:
                            continue
    except Exception:
        pass
    return total_rows

# Cache TTL set to 1200 seconds (20 minutes) to prevent disk queue maxing
@st.cache_data(ttl=1200)
def fetch_storage_metrics():
    paths = {
        "Root": ROOT_DIR,
        "Data": os.path.join(ROOT_DIR, "data"),
        "Vehicles": os.path.join(ROOT_DIR, "vehicles"),
        "Bronze": os.path.join(ROOT_DIR, "data", "delta", "bronze"),
        "Silver": os.path.join(ROOT_DIR, "data", "delta", "silver"),
        "Gold Base": os.path.join(ROOT_DIR, "data", "delta", "gold")
    }
    
    delta_tables = {
        "Bronze Rows": os.path.join(ROOT_DIR, "data", "delta", "bronze"),
        "Silver Rows": os.path.join(ROOT_DIR, "data", "delta", "silver"),
        "Gold (Health) Rows": os.path.join(ROOT_DIR, "data", "delta", "gold", "vehicle_health"),
        "Gold (Alerts) Rows": os.path.join(ROOT_DIR, "data", "delta", "gold", "alerts")
    }
    
    sizes = {name: get_dir_size(p) for name, p in paths.items()}
    rows = {name: get_delta_row_count(p) for name, p in delta_tables.items()}
    
    return sizes, rows


# --- SYSTEM & PROCESS OPERATIONS (REAL-TIME) ---

def get_optimized_process_data():
    matches = []
    emu_handles = 0
    
    for p in psutil.process_iter(['pid', 'name']):
        try:
            name = p.info['name'].lower()
            if any(t in name for t in TARGET_PROCESSES):
                p_full = psutil.Process(p.info['pid'])
                cmdline = p_full.cmdline()
                cmd_str = " ".join(cmdline).lower()
                
                if 'streaming_emulator' in cmd_str or ('spark' in cmd_str and 'java' in name):
                    label = "Unknown"
                    p_type = "System"
                    
                    if 'java' in name:
                        label = "Spark Executor"
                        p_type = "JVM"
                    elif 'python' in name:
                        p_type = "Python Engine"
                        label = "Script"
                        for arg in cmdline:
                            if arg.endswith('.py'):
                                label = arg.split("\\")[-1]
                                break
                    
                    try: handles = p_full.num_handles() 
                    except: handles = 0
                    
                    emu_handles += handles
                    
                    matches.append({
                        "PID": p.info['pid'],
                        "Component": label,
                        "Type": p_type,
                        "Handles": handles,
                        "CPU%": p_full.cpu_percent(interval=None), 
                        "Memory (MB)": p_full.memory_info().rss / 1024 / 1024
                    })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
    return matches, emu_handles


# --- MAIN RENDER ---

st.title("System Watchtower")

# Real-time execution
cpu_total = psutil.cpu_percent()
ram = psutil.virtual_memory()

# Evaluate System Constraints 
cpu_status = "status-ok"
if cpu_total > 95: cpu_status = "status-crit"
elif cpu_total > 85: cpu_status = "status-warn"

ram_status = "status-ok"
if ram.percent > 92: ram_status = "status-crit"
elif ram.percent > 80: ram_status = "status-warn"

emulator_data, emu_handles = get_optimized_process_data()
df_procs = pd.DataFrame(emulator_data)

emu_ram = df_procs['Memory (MB)'].sum() if not df_procs.empty else 0

handle_status = "status-ok"
if emu_handles > 15000: handle_status = "status-crit" # Windows starts freezing apps here
elif emu_handles > 10000: handle_status = "status-warn"

# Top Row: Real-time Resources
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="metric-card {cpu_status}">
        <div class="card-title">System CPU</div>
        <div class="card-value">{cpu_total}%</div>
        <div class="card-sub">Hardware Utilization</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="metric-card {ram_status}">
        <div class="card-title">System RAM</div>
        <div class="card-value">{ram.percent}%</div>
        <div class="card-sub">{ram.available/1024/1024/1024:.1f} GB Free</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="metric-card status-ok">
        <div class="card-title">Emulator RAM</div>
        <div class="card-value">{emu_ram:.0f} MB</div>
        <div class="card-sub">Pipeline Footprint</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="metric-card {handle_status}">
        <div class="card-title">Emulator OS Handles</div>
        <div class="card-value">{emu_handles}</div>
        <div class="card-sub">OS Resource Locks</div>
    </div>
    """, unsafe_allow_html=True)


# Bottom Rows: Delayed Storage Metrics & Process Table
st.markdown("---")
col_disk, col_procs = st.columns([1, 2])

with col_disk:
    st.markdown("<div class='card-title'>Storage & Data Volume (20m Cache)</div>", unsafe_allow_html=True)
    
    sizes, rows = fetch_storage_metrics()
    
    # Format for display
    disk_df = pd.DataFrame([
        {"Metric": k, "Value": format_bytes(v)} for k, v in sizes.items()
    ])
    
    row_df = pd.DataFrame([
        {"Table": k, "Record Count": f"{v:,}"} for k, v in rows.items()
    ])
    
    st.markdown("**Directory Footprints**")
    st.dataframe(disk_df, hide_index=True, use_container_width=True)
    
    st.markdown("**Delta Commit Rows**")
    st.dataframe(row_df, hide_index=True, use_container_width=True)

with col_procs:
    st.markdown("<div class='card-title'>Active Processes</div>", unsafe_allow_html=True)
    if not df_procs.empty:
        st.dataframe(
            df_procs.sort_values("Memory (MB)", ascending=False)
              .style.format({"CPU%": "{:.1f}", "Memory (MB)": "{:.0f}"}),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("No Emulator Processes Running")

time.sleep(1)
st.rerun()