import streamlit as st
import pandas as pd
import os
import time
from deltalake import DeltaTable
from src import config as gold_config

# Config
GOLD_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(GOLD_DIR)
# Dynamically select the first enabled module to calculate lag against
PRIMARY_MODULE = gold_config.ENABLED_MODULES[0] if gold_config.ENABLED_MODULES else "engine"
SILVER_PRIMARY_DIR = os.path.join(ROOT_DIR, "data", "delta", "silver", PRIMARY_MODULE)
GOLD_TABLE_DIR = os.path.join(ROOT_DIR, "data", "delta", "gold", "vehicle_health")

st.set_page_config(page_title="Gold Layer: Vehicle Health", layout="wide", page_icon="🥇")
st_autorefresh = st.empty()

@st.cache_data(ttl=2, show_spinner=False)
def load_gold_data():
    try:
        gold_dt = DeltaTable(GOLD_TABLE_DIR)
        gold_df = gold_dt.to_pandas()
        gold_count = len(gold_df)
        
        silver_count = 0
        if os.path.exists(SILVER_PRIMARY_DIR):
            silver_count = len(DeltaTable(SILVER_PRIMARY_DIR).to_pandas())
        
        lag_rows = max(0, silver_count - gold_count)
        
        if gold_df.empty: return None, 0, lag_rows
        
        gold_df['gold_window_ts'] = pd.to_datetime(gold_df['gold_window_ts'])
        gold_df = gold_df.sort_values('gold_window_ts', ascending=True)
        
        return gold_df, gold_count, lag_rows
    except Exception:
        return None, 0, 0

st.title("🥇 Gold Layer: Fused Vehicle Health")

df, total_gold, lag = load_gold_data()

if df is not None and not df.empty:
    active_sims = df['source_id'].unique().tolist()
    
    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Active Sims", len(active_sims))
    c2.metric("Total Gold Rows (Raw)", f"{total_gold:,}")
    c3.metric(f"Processing Lag (vs {PRIMARY_MODULE.capitalize()})", lag, delta_color="inverse")
    
    st.markdown("---")
    
    # Real-Time Plot
    col_sel, col_chart = st.columns([1, 4])
    with col_sel:
        selected_sim = st.selectbox("Select Vehicle:", sorted(active_sims))
        sim_df_all = df[df['source_id'] == selected_sim]
        sim_df_all = sim_df_all.drop_duplicates(subset=['gold_window_ts'], keep='last')
        
        sim_df_latest = sim_df_all.tail(1).iloc[0]
        st.info(f"**Latest Overall Health:** {sim_df_latest['vehicle_health_score']}%")
        
        # Dynamically check for ANY configured Tier 1 penalties
        for penalty_mod, threshold in gold_config.TIER_1_PENALTIES.items():
            col_name = f"{penalty_mod}_contrib"
            if col_name in sim_df_latest and sim_df_latest[col_name] < threshold:
                st.error(f"⚠️ CRITICAL {penalty_mod.upper()} PENALTY ACTIVE (<{threshold}%)")
            
        st.markdown("**Top 5 Active Anomalies:**")
        st.json(sim_df_latest['top_5_features'])

    with col_chart:
        # Dynamically find all '_contrib' columns present in the Gold table
        contrib_cols = [c for c in sim_df_all.columns if c.endswith("_contrib")]
        plot_cols = ['vehicle_health_score'] + contrib_cols
        
        st.line_chart(
            sim_df_all.set_index('gold_window_ts')[plot_cols], 
            height=350
        )

    st.markdown("---")
    
    st.subheader(f"🔍 Latest 10 Fused Records: {selected_sim}")
    tail_10_df = sim_df_all.sort_values('gold_window_ts', ascending=False).head(10)
    
    # Ensure columns exist before displaying to prevent crashes if schema evolved
    base_display_cols = ['gold_window_ts', 'vehicle_health_score']
    tail_cols = base_display_cols + contrib_cols + ['top_5_features']
    tail_cols = [c for c in tail_cols if c in tail_10_df.columns]
    
    st.dataframe(tail_10_df[tail_cols], use_container_width=True)

else:
    st.info("Waiting for Gold records to be generated...")

time.sleep(2)
st.rerun()