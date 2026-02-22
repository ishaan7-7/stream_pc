# File: C:\streaming_emulator\tools\system_monitor.py
import streamlit as st
import psutil
import pandas as pd
import time
import altair as alt

st.set_page_config(page_title="System Watchtower", page_icon="🛡️", layout="wide")

st.markdown("""
<style>
    .main { background-color: #0E1117; }
    .metric-card { background-color: #262730; padding: 15px; border-radius: 8px; border-left: 5px solid #666; margin-bottom: 10px; }
    .status-ok { border-left-color: #4CAF50; }
    .status-warn { border-left-color: #FFA726; }
    .status-crit { border-left-color: #F44336; }
    .big-stat { font-size: 1.8em; font-weight: bold; color: #FFF; }
    .sub-stat { font-size: 0.9em; color: #AAA; }
</style>
""", unsafe_allow_html=True)

# --- CONFIG ---
# Optimized: Don't scan everything. Only scan relevant PIDs if possible, 
# or fast-filter.
TARGET_PROCESSES = ['python', 'java', 'cmd', 'powershell']

def get_optimized_process_data():
    """
    Fast-Scan: Only query heavy stats for processes that match our target names.
    """
    matches = []
    # 1. Fast Iteration (Only PID and Name)
    for p in psutil.process_iter(['pid', 'name']):
        try:
            name = p.info['name'].lower()
            if any(t in name for t in TARGET_PROCESSES):
                # 2. Deep Dive only on matches
                try:
                    p_full = psutil.Process(p.info['pid'])
                    cmdline = p_full.cmdline()
                    cmd_str = " ".join(cmdline).lower()
                    
                    # Filter for Emulator specific components
                    if 'streaming_emulator' in cmd_str or ('spark' in cmd_str and 'java' in name):
                        
                        # Determine Component Label
                        label = "Unknown"
                        p_type = "System"
                        
                        if 'java' in name:
                            label = "Spark Executor (Writer)"
                            p_type = "Engine (Java)"
                        elif 'python' in name:
                            p_type = "Logic (Python)"
                            label = "Python Script"
                            # Try to find script name
                            for arg in cmdline:
                                if arg.endswith('.py'):
                                    label = arg.split("\\")[-1]
                                    break
                        
                        matches.append({
                            "PID": p.info['pid'],
                            "Component": label,
                            "Type": p_type,
                            "Threads": p_full.num_threads(),
                            "CPU%": p_full.cpu_percent(interval=None), # Non-blocking
                            "Memory (MB)": p_full.memory_info().rss / 1024 / 1024
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
    return matches

# --- UI ---
st.title("🛡️ Resource Watchtower (Optimized)")

# 1. Global Stats (Fast)
cpu_total = psutil.cpu_percent()
ram = psutil.virtual_memory()
total_threads = 0 # Approximate, expensive to count all, so we skip global thread count for speed

# 2. Emulator Stats
emulator_data = get_optimized_process_data()
df = pd.DataFrame(emulator_data)

if not df.empty:
    emu_threads = df['Threads'].sum()
    emu_ram = df['Memory (MB)'].sum()
else:
    emu_threads = 0
    emu_ram = 0

# --- METRICS ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Global CPU", f"{cpu_total}%")
c2.metric("Global RAM", f"{ram.percent}%", f"{ram.available/1024/1024/1024:.1f} GB Free")
c3.metric("Emulator RAM", f"{emu_ram:.0f} MB", "Footprint")
c4.metric("Emulator Threads", f"{emu_threads}")

st.markdown("---")

if not df.empty:
    st.subheader("Process Audit")
    
    # Visual
    chart = alt.Chart(df).mark_bar().encode(
        x='Memory (MB)',
        y=alt.Y('Component', sort='-x'),
        color='Type',
        tooltip=['PID', 'Threads', 'Memory (MB)']
    )
    st.altair_chart(chart, use_container_width=True)
    
    # Table
    st.dataframe(
        df.sort_values("Memory (MB)", ascending=False)
          .style.format({"CPU%": "{:.1f}", "Memory (MB)": "{:.0f}"}),
        use_container_width=True
    )
else:
    st.info("No Emulator Processes Running")

time.sleep(2)
st.rerun()