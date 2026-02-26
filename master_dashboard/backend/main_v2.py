import logging
import aiohttp
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("API_Gateway")

# --- App Definition ---
app = FastAPI(
    title="Master Dashboard API Gateway",
    description="Routes React frontend requests to isolated backend microservices"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:5173"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Microservice Registry ---
SERVICES = {
    "writer": "http://127.0.0.1:8001",
    "inference": "http://127.0.0.1:8002",
    "gold": "http://127.0.0.1:8003",
    "alerts": "http://127.0.0.1:8004",
    "dtc": "http://127.0.0.1:8004",  # DTC is handled by the alerts service
    "observer": "http://127.0.0.1:8006"
}

# --- Generic Proxy Forwarder ---
async def proxy_request(service_key: str, endpoint: str, request: Request, fallback_data: dict):
    """Forwards requests to the underlying microservice and handles offline states gracefully."""
    base_url = SERVICES.get(service_key)
    if not base_url:
        logger.error(f"Routing failed: Unknown service '{service_key}'")
        return JSONResponse(status_code=500, content={"error": "Service routing not configured"})

    # Reconstruct the full URL including query parameters (vital for DTC)
    target_url = f"{base_url}{endpoint}"
    query_params = dict(request.query_params)
    
    logger.info(f"Routing -> {target_url} | Params: {query_params}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(target_url, params=query_params, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    error_text = await resp.text()
                    logger.error(f"Microservice {service_key} returned {resp.status}: {error_text}")
                    raise HTTPException(status_code=resp.status, detail=error_text)
                    
    except aiohttp.ClientConnectorError:
        logger.warning(f"Microservice Offline: {service_key} at {base_url}")
        # Return fallback data so React UI doesn't crash, just shows empty/offline state
        return fallback_data
    except asyncio.TimeoutError:
        logger.error(f"Microservice Timeout: {service_key} took too long to respond.")
        return JSONResponse(status_code=504, content={"error": "Microservice timeout"})
    except Exception as e:
        logger.error(f"Proxy Error on {service_key}: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# --- Gateway Routes ---

@app.get("/health")
def health_check():
    return {"status": "Master Gateway V2 is Online", "port": 8005}

# 1. Writer Ops
@app.get("/api/writer/metrics")
async def get_writer_metrics(request: Request):
    return await proxy_request("writer", "/api/writer/metrics", request, fallback_data={})

@app.get("/api/writer/inspector/{module}")
async def get_writer_inspector(module: str, request: Request):
    return await proxy_request("writer", f"/api/writer/inspector/{module}", request, fallback_data={"data": []})

# 2. Inference Ops
@app.get("/api/inference/metrics")
async def get_inference_metrics(request: Request):
    fallback = {
        "active_sims": 0, "active_modules": 0, "global_e2e_ms": 0,
        "global_inf_ms": 0, "module_stats": {}, "recent_alerts": []
    }
    return await proxy_request("inference", "/api/inference/metrics", request, fallback_data=fallback)

@app.get("/api/inference/tail/{module}")
async def get_inference_tail(module: str, request: Request):
    return await proxy_request("inference", f"/api/inference/tail/{module}", request, fallback_data={"data": []})

# 3. Gold Health
@app.get("/api/gold/metrics")
async def get_gold_metrics(request: Request):
    fallback = {"active_sims": [], "total_gold_rows": 0, "processing_lags": {}}
    return await proxy_request("gold", "/api/gold/metrics", request, fallback_data=fallback)

@app.get("/api/gold/config")
async def get_gold_config(request: Request):
    return await proxy_request("gold", "/api/gold/config", request, fallback_data={})

@app.get("/api/gold/history/{sim_id}")
async def get_gold_history(sim_id: str, request: Request):
    return await proxy_request("gold", f"/api/gold/history/{sim_id}", request, fallback_data={"data": []})

# 4. Alerts & DTC
@app.get("/api/alerts/metrics")
async def get_alerts_metrics(request: Request):
    fallback = {"active_alerts_count": 0, "critical_vehicles": 0, "processing_lag": 0, "open_alerts": [], "closed_alerts": []}
    return await proxy_request("alerts", "/api/alerts/metrics", request, fallback_data=fallback)

@app.get("/api/dtc/analyze")
async def analyze_dtc(request: Request):
    # Relies on query params being automatically forwarded by the proxy_request function
    return await proxy_request("dtc", "/api/dtc/analyze", request, fallback_data={"error": "Alerts Service Offline"})

# 5. Telemetry Observer
@app.get("/api/observer/snapshot")
async def get_observer_snapshot(request: Request):
    fallback = {
        "system_health": {}, 
        "global_stats": {"total_rows": 0, "active_vehicles": 0, "avg_latency": 0.0, "dlq_backlog": 0}, 
        "vehicles": []
    }
    return await proxy_request("observer", "/api/observer/snapshot", request, fallback_data=fallback)

if __name__ == "__main__":
    import uvicorn
    # The API Gateway runs on 8005, preserving the React frontend's current configuration
    uvicorn.run("main_v2:app", host="127.0.0.1", port=8005, reload=True)