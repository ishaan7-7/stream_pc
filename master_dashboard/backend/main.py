import os
import sys
import json
import requests
import pandas as pd
import plotly.express as px
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import ProcessPoolExecutor
from utils import safe_read_pickle, safe_read_json

app = FastAPI(
    title="Master Dashboard API",
    description="Read-only data aggregator and execution layer"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
VEHICLE_MODULES = ["battery", "body", "engine", "transmission", "tyre"]

# --- DYNAMIC IMPORTS FOR DTC ANALYZER ---
DTC_DIR = os.path.join(PROJECT_ROOT, "alerts_service", "DTC_service")
ALERTS_SRC_DIR = os.path.join(PROJECT_ROOT, "alerts_service")

# Ensure both paths are available for internal imports within the adapter
sys.path.append(DTC_DIR)
sys.path.append(ALERTS_SRC_DIR)

# Process pool restricted to 2 workers for heavy PyTorch inference
process_pool = ProcessPoolExecutor(max_workers=2)

@app.get("/health")
def health_check():
    return {"status": "Master Dashboard Backend is Online", "port": 8005}

# --- READ-ONLY ENDPOINTS (Writer, Inference, Gold, Alerts) ---
@app.get("/api/writer/metrics")
def get_writer_metrics():
    data = {}
    for module in VEHICLE_MODULES:
        file_path = os.path.join(PROJECT_ROOT, "writer_service", "state", f"writer_metrics_{module}.json")
        res = safe_read_json(file_path)
        if res: data[module] = res
    return data

@app.get("/api/inference/state")
def get_inference_state():
    data = {}
    for module in VEHICLE_MODULES:
        file_path = os.path.join(PROJECT_ROOT, "inference_service", "state", f"ml_state_{module}.pkl")
        res = safe_read_pickle(file_path)
        if res: data[module] = res
    return data

@app.get("/api/inference/checkpoints")
def get_inference_checkpoints():
    data = {}
    for module in VEHICLE_MODULES:
        file_path = os.path.join(PROJECT_ROOT, "inference_service", "state", f"checkpoints_{module}.json")
        res = safe_read_json(file_path)
        if res: data[module] = res
    return data

@app.get("/api/gold/vehicles")
def get_gold_vehicles():
    file_path = os.path.join(PROJECT_ROOT, "gold_service", "state", "vehicle_cache.pkl")
    res = safe_read_pickle(file_path)
    return res if res else {}

@app.get("/api/alerts/active")
def get_alerts_active():
    file_path = os.path.join(PROJECT_ROOT, "alerts_service", "state", "alert_state_cache.pkl")
    res = safe_read_pickle(file_path)
    return res if res else {}

# --- REPLAY CONTROLS ---
@app.post("/api/replay/{action}")
def control_replay(action: str):
    if action not in ["start", "stop"]:
        return {"error": "Invalid action. Must be 'start' or 'stop'."}
    try:
        response = requests.post(f"http://127.0.0.1:8001/{action}", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": f"Failed to contact Replay Service: {str(e)}"}

# --- ON-DEMAND DTC ANALYSIS ---
def _render_plot_to_json(df_buildup, title, color_theme):
    """Replicates the render_plot logic from dashboard_alerts.py"""
    if df_buildup.empty:
        return None
    cols_to_plot = [c for c in df_buildup.columns if c != 'timestamp']
    if len(cols_to_plot) == 0: 
        return None
    
    plot_df = df_buildup.melt(id_vars=['timestamp'], value_vars=cols_to_plot, var_name='DTC_Code', value_name='Risk_Level')
    fig = px.line(plot_df, x='timestamp', y='Risk_Level', color='DTC_Code', title=title, color_discrete_sequence=color_theme)
    fig.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="100% Failure Trigger")
    fig.update_yaxes(range=[0, 1.1])
    
    # Strip background for the "Industrial/Professional" look you requested
    fig.update_layout(template="plotly_white")
    
    return json.loads(fig.to_json())

def _execute_dtc_pipeline(module: str, source_id: str, peak_ts: str):
    """Executes the full PyTorch pipeline in an isolated process."""
    try:
        from analyzer import DTCAdapter
        
        adapter = DTCAdapter(module_name=module)
        bronze_df = adapter.fetch_traceback(source_id, peak_ts)
        
        if bronze_df.empty:
            return {"error": "Traceback data not found in Bronze table."}
            
        df_crit, df_noncrit, triggers, diagnostics = adapter.run_diagnosis(bronze_df)
        
        # Build the Plotly JSONs
        fig_crit_json = _render_plot_to_json(df_crit, "Critical Fault Maturation", px.colors.qualitative.Set1)
        fig_noncrit_json = _render_plot_to_json(df_noncrit, "Non-Critical Fault Maturation", px.colors.qualitative.Pastel1)
        
        return {
            "success": True,
            "critical_plot": fig_crit_json,
            "non_critical_plot": fig_noncrit_json,
            "triggers": triggers,
            "diagnostics": diagnostics
        }
    except Exception as e:
        return {"error": f"Inference pipeline failed: {str(e)}"}

@app.get("/api/dtc/analyze")
def analyze_dtc(module: str, source_id: str, peak_ts: str):
    """
    Endpoint that React will call when a user clicks 'Root Cause DTC'.
    Example: /api/dtc/analyze?module=engine&source_id=V-123&peak_ts=2026-02-24T12:00:00
    """
    try:
        # Submit to the process pool to prevent blocking the UI
        future = process_pool.submit(_execute_dtc_pipeline, module, source_id, peak_ts)
        result = future.result(timeout=60) # Generous timeout for PyTorch loading
        return result
    except Exception as e:
        return {"error": f"DTC Analysis computation failed: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8005, reload=True)