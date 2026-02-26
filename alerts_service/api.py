import os
import json
import asyncio
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import ProcessPoolExecutor

# --- Paths & Constants ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
SILVER_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "silver")
ALERTS_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "gold", "alerts")
ALERTS_CHECKPOINT = os.path.join(CURRENT_DIR, "state", "checkpoints.json")

VEHICLE_MODULES = ["engine", "transmission", "battery", "body", "tyre"]

# --- Utils ---
def safe_read_json(file_path):
    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return None

# --- Cache & Process Pool ---
ALERTS_METRICS_CACHE = {
    "active_alerts_count": 0,
    "critical_vehicles": 0,
    "processing_lag": 0,
    "open_alerts": [],
    "closed_alerts": []
}

# Max workers = 2 to prevent RAM exhaustion on GTX 1650 laptop
process_pool = ProcessPoolExecutor(max_workers=2) 

# --- App Definition ---
app = FastAPI(title="Alerts & DTC Service Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Background Logic ---
def _sync_update_alerts():
    """Synchronous file I/O operations executed in a separate thread"""
    global ALERTS_METRICS_CACHE
    try:
        lag_rows = 0
        # 1. Calculate Lag using Checkpoints vs Silver
        try:
            if os.path.exists(ALERTS_CHECKPOINT) and os.path.exists(SILVER_ROOT):
                ckpt = safe_read_json(ALERTS_CHECKPOINT) or {}
                primary_mod = VEHICLE_MODULES[0]
                last_ts = ckpt.get(primary_mod, "1970-01-01T00:00:00")
                
                silver_primary = os.path.join(SILVER_ROOT, primary_mod)
                if os.path.exists(silver_primary):
                    s_files = [os.path.join(r, f) for r, d, f in os.walk(silver_primary) for f in f if f.endswith(".parquet")]
                    for f in s_files:
                        try:
                            df = pd.read_parquet(f)
                            if 'inference_ts' in df.columns:
                                df['inference_ts'] = pd.to_datetime(df['inference_ts'], utc=True)
                                lag_rows += len(df[df['inference_ts'] > pd.to_datetime(last_ts, utc=True)])
                        except: pass
        except: pass

        # 2. Read Gold Alerts Table
        df_alerts = pd.DataFrame()
        if os.path.exists(ALERTS_ROOT):
            files = [os.path.join(r, f) for r, d, f in os.walk(ALERTS_ROOT) for f in f if f.endswith(".parquet")]
            dfs = []
            for f in files:
                try: dfs.append(pd.read_parquet(f))
                except: pass
            if dfs:
                df_alerts = pd.concat(dfs, ignore_index=True)
        
        active_alerts = 0
        crit_vehicles = 0
        open_alerts = []
        closed_alerts = []

        if not df_alerts.empty:
            df_alerts = df_alerts.fillna(0)
            for col in df_alerts.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
                df_alerts[col] = df_alerts[col].astype(str)

            open_df = df_alerts[df_alerts['status'] == "OPEN"].sort_values('peak_anomaly_ts', ascending=False)
            closed_df = df_alerts[df_alerts['status'] == "CLOSED"].sort_values('alert_end_ts', ascending=False)

            active_alerts = len(open_df)
            crit_vehicles = open_df['source_id'].nunique() if not open_df.empty else 0
            open_alerts = open_df.to_dict(orient="records")
            closed_alerts = closed_df.to_dict(orient="records")

        # 3. Update Cache Exactly as React Expects
        ALERTS_METRICS_CACHE["active_alerts_count"] = active_alerts
        ALERTS_METRICS_CACHE["critical_vehicles"] = crit_vehicles
        ALERTS_METRICS_CACHE["processing_lag"] = lag_rows
        ALERTS_METRICS_CACHE["open_alerts"] = open_alerts
        ALERTS_METRICS_CACHE["closed_alerts"] = closed_alerts

    except Exception as e:
        print(f"Alerts metrics loop failed: {e}")

async def update_alerts_metrics_loop():
    while True:
        await asyncio.to_thread(_sync_update_alerts)
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_alerts_metrics_loop())

@app.on_event("shutdown")
def shutdown_event():
    process_pool.shutdown(wait=True)

# --- DTC Core Engine Logic ---
def _execute_dtc_pipeline(module: str, source_id: str, peak_ts: str):
    """Executes heavy PyTorch inference in an isolated process to prevent GIL locking"""
    try:
        # Imports are local to survive Windows Process Spawning
        import pandas as pd
        import json
        import plotly.express as px
        from DTC_service.analyzer import DTCAdapter
        
        adapter = DTCAdapter(module_name=module)
        
        peak_datetime = pd.to_datetime(peak_ts)
        bronze_df = adapter.fetch_traceback(source_id, peak_datetime)
        
        if bronze_df.empty:
            return {"error": f"Traceback data not found in Bronze table for {source_id} at {peak_ts}."}
            
        df_crit, df_noncrit, triggers, diagnostics = adapter.run_diagnosis(bronze_df)
        
        def render_plot_to_json(df_buildup, title, color_theme):
            if df_buildup.empty: return None
            cols_to_plot = [c for c in df_buildup.columns if c != 'timestamp']
            if len(cols_to_plot) == 0: return None
            
            plot_df = df_buildup.melt(id_vars=['timestamp'], value_vars=cols_to_plot, var_name='DTC_Code', value_name='Risk_Level')
            fig = px.line(plot_df, x='timestamp', y='Risk_Level', color='DTC_Code', title=title, color_discrete_sequence=color_theme)
            fig.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="100% Failure Trigger")
            fig.update_yaxes(range=[0, 1.1])
            # Strict UI matching for React
            fig.update_layout(template="plotly_white", margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            return json.loads(fig.to_json())
        
        return {
            "success": True,
            "triggers": triggers,
            "diagnostics": diagnostics,
            "critical_plot": render_plot_to_json(df_crit, "Critical Fault Maturation", px.colors.qualitative.Set1),
            "non_critical_plot": render_plot_to_json(df_noncrit, "Non-Critical Fault Maturation", px.colors.qualitative.Pastel1)
        }
    except Exception as e:
        return {"error": f"DTC Analysis computation failed: {str(e)}"}

# --- Endpoints ---
@app.get("/api/alerts/metrics")
def get_alerts_metrics():
    return ALERTS_METRICS_CACHE

@app.get("/api/dtc/analyze")
def analyze_dtc(module: str, source_id: str, peak_ts: str):
    try:
        # Submit the heavily synchronous PyTorch processing to the background pool
        future = process_pool.submit(_execute_dtc_pipeline, module, source_id, peak_ts)
        result = future.result(timeout=60)
        return result
    except Exception as e:
        return {"error": f"DTC Analysis execution failed: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    # Alerts & DTC runs on port 8004
    uvicorn.run("api:app", host="127.0.0.1", port=8004, reload=True)