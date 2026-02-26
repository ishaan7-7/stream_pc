import os
import sys
import asyncio
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- Path spoofing to protect observer_backend.py ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

# 1. Add root to Python path for imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# 2. MAGIC FIX: Change the OS working directory to the project root.
# This allows observer_backend.py's relative path Path("ingest/config/ingest_config.json") to work natively!
os.chdir(PROJECT_ROOT)

# Now we safely import the untouched engine
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

observer_instance = HybridObserver()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(observer_instance.start())

@app.on_event("shutdown")
async def shutdown_event():
    await observer_instance.stop()

@app.get("/api/observer/snapshot")
async def get_observer_snapshot():
    try:
        return await observer_instance.get_snapshot()
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
    uvicorn.run("api:app", host="127.0.0.1", port=8006, reload=True)