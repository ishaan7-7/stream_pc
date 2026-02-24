import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from utils import safe_read_pickle, safe_read_json

app = FastAPI(
    title="Master Dashboard API",
    description="Read-only data aggregator for the streaming emulator"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dynamically find the root 'streaming_emulator' directory
# Assuming path is: streaming_emulator/master_dashboard/backend/main.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

VEHICLE_MODULES = ["battery", "body", "engine", "transmission", "tyre"]

@app.get("/health")
def health_check():
    return {"status": "Master Dashboard Backend is Online", "port": 8005}

# --- 1. WRITER METRICS ---
@app.get("/api/writer/metrics")
def get_writer_metrics():
    """Reads throughput and latency JSONs dumped by the Spark Writer."""
    data = {}
    for module in VEHICLE_MODULES:
        file_path = os.path.join(PROJECT_ROOT, "writer_service", "state", f"writer_metrics_{module}.json")
        result = safe_read_json(file_path)
        if result:
            data[module] = result
    return data

# --- 2. INFERENCE STATE ---
@app.get("/api/inference/state")
def get_inference_state():
    """Reads the machine learning state pickles (predictions, anomalies)."""
    data = {}
    for module in VEHICLE_MODULES:
        file_path = os.path.join(PROJECT_ROOT, "inference_service", "state", f"ml_state_{module}.pkl")
        result = safe_read_pickle(file_path)
        if result:
            data[module] = result
    return data

@app.get("/api/inference/checkpoints")
def get_inference_checkpoints():
    """Reads the offset checkpoints for the inference consumer."""
    data = {}
    for module in VEHICLE_MODULES:
        file_path = os.path.join(PROJECT_ROOT, "inference_service", "state", f"checkpoints_{module}.json")
        result = safe_read_json(file_path)
        if result:
            data[module] = result
    return data

# --- 3. VEHICLE HEALTH (GOLD) ---
@app.get("/api/gold/vehicles")
def get_gold_vehicles():
    """Reads the aggregated vehicle cache (Gold tier)."""
    file_path = os.path.join(PROJECT_ROOT, "gold_service", "state", "vehicle_cache.pkl")
    data = safe_read_pickle(file_path)
    return data if data else {}

# --- 4. ALERTS ACTIVE ---
@app.get("/api/alerts/active")
def get_alerts_active():
    """Reads the active alert and DTC state cache."""
    file_path = os.path.join(PROJECT_ROOT, "alerts_service", "state", "alert_state_cache.pkl")
    data = safe_read_pickle(file_path)
    return data if data else {}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8005, reload=True)