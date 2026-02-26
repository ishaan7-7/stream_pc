import os
import json
import asyncio
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# --- Paths & Constants ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
SILVER_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "silver")
STATE_DIR = os.path.join(CURRENT_DIR, "state")

VEHICLE_MODULES = ["battery", "body", "engine", "transmission", "tyre"]

# --- Utils ---
def safe_read_json(file_path):
    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return None

# --- Cache ---
INFERENCE_METRICS_CACHE = {
    "active_sims": 0,
    "active_modules": 0,
    "global_e2e_ms": 0,
    "global_inf_ms": 0,
    "module_stats": {},
    "recent_alerts": []
}

# --- App Definition ---
app = FastAPI(title="Inference Service Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Background Logic ---
def _sync_update_metrics():
    """Synchronous function to safely crunch pandas dataframes off the main thread"""
    global INFERENCE_METRICS_CACHE
    try:
        # 1. Load System Alerts
        all_alerts = []
        for mod in VEHICLE_MODULES:
            alerts_file = os.path.join(STATE_DIR, f"system_alerts_{mod}.json")
            alerts = safe_read_json(alerts_file)
            if alerts:
                all_alerts.extend(alerts)

        # 2. Determine "Virtual Now" to handle offline viewing
        latest_ts = pd.Timestamp("1970-01-01", tz="UTC")
        for a in all_alerts:
            try:
                ts = pd.to_datetime(a['timestamp'], utc=True)
                if ts > latest_ts: latest_ts = ts
            except: pass

        dfs_by_mod = {}
        for mod in VEHICLE_MODULES:
            path = os.path.join(SILVER_ROOT, mod)
            if not os.path.exists(path): continue

            files = []
            for r, d, f in os.walk(path):
                for file in f:
                    if file.endswith(".parquet"):
                        files.append(os.path.join(r, file))
            files.sort(key=os.path.getmtime, reverse=True)

            dfs = []
            for f in files[:5]: # Check top 5 recent parquets
                try:
                    df = pd.read_parquet(f)
                    if not df.empty and 'inference_ts' in df.columns:
                        df['inference_ts'] = pd.to_datetime(df['inference_ts'], utc=True)
                        max_df_ts = df['inference_ts'].max()
                        if max_df_ts > latest_ts: 
                            latest_ts = max_df_ts
                        dfs.append(df)
                except: pass
            if dfs:
                dfs_by_mod[mod] = pd.concat(dfs, ignore_index=True)

        # Apply the 5-minute window anchored to the latest recorded data
        cutoff_dt = latest_ts - pd.Timedelta(minutes=5)
        
        recent_alerts = []
        for a in all_alerts:
            try:
                if pd.to_datetime(a['timestamp'], utc=True) >= cutoff_dt:
                    recent_alerts.append(a)
            except: pass
        recent_alerts.sort(key=lambda x: x['timestamp'], reverse=True)

        # 3. Compute Silver Metrics
        sims = set()
        module_stats = {}
        e2e_list = []
        inf_list = []

        for mod, combined_df in dfs_by_mod.items():
            combined_df = combined_df[combined_df['inference_ts'] >= cutoff_dt]
            if combined_df.empty: continue

            combined_df['ingest_ts'] = pd.to_datetime(combined_df.get('ingest_ts', pd.NaT), utc=True)
            e2e = (combined_df['inference_ts'] - combined_df['ingest_ts']).dt.total_seconds() * 1000
            
            if 'writer_ts' in combined_df.columns:
                combined_df['writer_ts'] = pd.to_datetime(combined_df['writer_ts'], utc=True)
                inf = (combined_df['inference_ts'] - combined_df['writer_ts']).dt.total_seconds() * 1000
            else:
                inf = e2e

            e2e_mean = e2e.mean()
            inf_mean = inf.mean()
            e2e_list.append(e2e_mean)
            inf_list.append(inf_mean)

            if 'source_id' in combined_df.columns:
                sims.update(combined_df['source_id'].unique().tolist())

            module_stats[mod.upper()] = {
                "e2e_latency": round(e2e_mean, 1) if pd.notna(e2e_mean) else 0,
                "inf_latency": round(inf_mean, 1) if pd.notna(inf_mean) else 0,
                "rows_5m": len(combined_df)
            }

        # Update Cache Exactly as React Expects
        INFERENCE_METRICS_CACHE["active_sims"] = len(sims)
        INFERENCE_METRICS_CACHE["active_modules"] = len(module_stats)
        INFERENCE_METRICS_CACHE["global_e2e_ms"] = round(sum(e2e_list)/len(e2e_list), 1) if e2e_list else 0
        INFERENCE_METRICS_CACHE["global_inf_ms"] = round(sum(inf_list)/len(inf_list), 1) if inf_list else 0
        INFERENCE_METRICS_CACHE["module_stats"] = module_stats
        INFERENCE_METRICS_CACHE["recent_alerts"] = recent_alerts[:10]

    except Exception as e:
        print(f"Inference metrics computation failed: {e}")

async def update_inference_metrics_loop():
    while True:
        await asyncio.to_thread(_sync_update_metrics)
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_inference_metrics_loop())

# --- Endpoints ---
@app.get("/api/inference/metrics")
def get_inference_metrics():
    return INFERENCE_METRICS_CACHE

@app.get("/api/inference/tail/{module}")
def get_inference_tail(module: str):
    if module not in VEHICLE_MODULES:
        raise HTTPException(status_code=400, detail="Invalid module")
        
    path = os.path.join(SILVER_ROOT, module)
    if not os.path.exists(path): return {"data": []}
        
    files = []
    for root, _, filenames in os.walk(path):
        for f in filenames:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
                
    if not files: return {"data": []}
    files.sort(key=os.path.getmtime, reverse=True)
    
    data_frames = []
    try:
        for f in files[:10]: 
            df = pd.read_parquet(f)
            if not df.empty:
                data_frames.append(df)
                
        if not data_frames:
            return {"data": []}
            
        combined_df = pd.concat(data_frames, ignore_index=True)
        if "inference_ts" in combined_df.columns:
            combined_df["inference_ts"] = pd.to_datetime(combined_df["inference_ts"]).astype(str)
            combined_df = combined_df.sort_values("inference_ts", ascending=False)
            
        combined_df = combined_df.fillna(0)
        # Handle timezone-aware/naive datetimes correctly
        for col in combined_df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            combined_df[col] = combined_df[col].astype(str)
            
        return {"data": combined_df.head(100).to_dict(orient="records")}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Inference runs on port 8002
    uvicorn.run("api:app", host="127.0.0.1", port=8002, reload=True)