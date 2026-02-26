import os
import sys
import asyncio
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- Path Resolution ---
# Ensure the backend can find the ingest config just like observer_backend.py does
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import your existing, working engine!
from telemetry_observer.observer_backend import HybridObserver

# --- App Definition ---
app = FastAPI(title="Telemetry Observer Service Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instantiate the singleton observer
observer_instance = HybridObserver()

# --- Application Lifecycle ---
@app.on_event("startup")
async def startup_event():
    # Start the background Kafka and Port Monitoring loops natively in asyncio
    asyncio.create_task(observer_instance.start())

@app.on_event("shutdown")
async def shutdown_event():
    # Cleanly close Kafka consumers and HTTP sessions
    await observer_instance.stop()

# --- Endpoints ---
@app.get("/api/observer/snapshot")
async def get_observer_snapshot():
    """
    Delivers the exact same JSON structure that powers the Streamlit ui.py,
    allowing the React frontend to build the 4 required tabs.
    """
    try:
        snapshot = await observer_instance.get_snapshot()
        return snapshot
    except Exception as e:
        return {
            "system_health": {},
            "global_stats": {"total_rows": 0, "active_vehicles": 0, "avg_latency": 0.0, "dlq_backlog": 0},
            "vehicles": [],
            "error": "Observer Engine Error",
            "details": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    # Telemetry Observer runs on port 8006
    uvicorn.run("api:app", host="127.0.0.1", port=8006, reload=True)