import os
import sys
import json
import time
import asyncio
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, TopicPartition
from utils import safe_read_json

app = FastAPI(
    title="Master Dashboard API",
    description="Read-only data aggregator and execution layer"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:5173"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
VEHICLE_MODULES = ["battery", "body", "engine", "transmission", "tyre"]
DELTA_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "bronze")
KAFKA_BROKER = "localhost:9092"

# --- CACHE FOR HIGH-FREQUENCY POLLING ---
WRITER_METRICS_CACHE = {
    module: {
        "module": module.upper(),
        "status": "OFFLINE",
        "kafka_total": 0,
        "delta_total": 0,
        "true_lag": 0,
        "throughput": "0.0",
        "processed": "0.0",
        "latency_ms": 0
    } for module in VEHICLE_MODULES
}

class TelemetryBackend:
    def __init__(self):
        self.consumer = None
        self._connect()
        
    def _connect(self):
        try:
            conf = {
                'bootstrap.servers': KAFKA_BROKER, 
                'group.id': 'master_dashboard_monitor', 
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            self.consumer = Consumer(conf)
        except Exception:
            pass

    def get_kafka_counts(self):
        counts = {m: 0 for m in VEHICLE_MODULES}
        if not self.consumer:
            self._connect()
            return counts
            
        for m in VEHICLE_MODULES:
            topic = f"telemetry.{m}"
            total = 0
            try:
                meta = self.consumer.list_topics(topic, timeout=0.5)
                if topic in meta.topics:
                    parts = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
                    for p in parts:
                        _, high = self.consumer.get_watermark_offsets(p, timeout=0.5, cached=False)
                        if high > 0:
                            total += high
            except: 
                pass
            counts[m] = total
        return counts

def get_delta_counts():
    delta_counts = {m: 0 for m in VEHICLE_MODULES}
    for m in VEHICLE_MODULES:
        log_path = os.path.join(DELTA_ROOT, m, "_delta_log")
        total = 0
        if os.path.exists(log_path):
            json_files = [os.path.join(log_path, f) for f in os.listdir(log_path) if f.endswith(".json")]
            for jf in json_files:
                try:
                    with open(jf, "r") as f:
                        for line in f:
                            if "numRecords" in line:
                                action = json.loads(line)
                                if "add" in action:
                                    stats = json.loads(action["add"].get("stats", "{}"))
                                    total += int(stats.get("numRecords", 0))
                except:
                    pass
        delta_counts[m] = total
    return delta_counts

async def update_writer_metrics_loop():
    kafka_monitor = TelemetryBackend()
    while True:
        try:
            k_counts = kafka_monitor.get_kafka_counts()
            d_counts = get_delta_counts()

            for module in VEHICLE_MODULES:
                k_total = k_counts.get(module, 0)
                d_total = d_counts.get(module, 0)
                
                file_path = os.path.join(PROJECT_ROOT, "writer_service", "state", f"writer_metrics_{module}.json")
                spark_data = safe_read_json(file_path) or {}
                stream_data = spark_data.get("streams", {}).get(module, {})
                
                status = spark_data.get("status", "OFFLINE")
                if status == "RUNNING" and (time.time() - spark_data.get("last_updated", 0) > 10):
                    status = "STALLED"

                WRITER_METRICS_CACHE[module] = {
                    "module": module.upper(),
                    "status": status,
                    "kafka_total": k_total,
                    "delta_total": d_total,
                    "true_lag": max(0, k_total - d_total),
                    "throughput": str(round(stream_data.get("input_rate", 0.0), 1)),
                    "processed": str(round(stream_data.get("process_rate", 0.0), 1)),
                    "latency_ms": stream_data.get("duration_ms", 0)
                }
        except Exception as e:
            print(f"Background metric update failed: {e}")
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_writer_metrics_loop())

@app.get("/health")
def health_check():
    return {"status": "Master Dashboard Backend is Online", "port": 8005}

# --- WRITER OPS ENDPOINTS ---

@app.get("/api/writer/metrics")
def get_writer_metrics():
    """Powers the 'Operations Board' view"""
    return WRITER_METRICS_CACHE

@app.get("/api/writer/inspector/{module}")
def get_writer_inspector(module: str):
    """Powers the 'Data Inspector' view - fetches exact 100 rows across multiple partitions"""
    if module not in VEHICLE_MODULES:
        raise HTTPException(status_code=400, detail="Invalid module")
        
    path = os.path.join(DELTA_ROOT, module)
    if not os.path.exists(path): 
        return {"data": []}
        
    files = []
    for root, _, filenames in os.walk(path):
        for f in filenames:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
                
    if not files: 
        return {"data": []}
        
    # Sort files by newest modified time
    files.sort(key=os.path.getmtime, reverse=True)
    
    data_frames = []
    try:
        # Read the last 10 parquet files to guarantee we have enough rows
        for f in files[:10]: 
            df = pd.read_parquet(f)
            if not df.empty:
                data_frames.append(df)
                
        if not data_frames:
            return {"data": []}
            
        # Concatenate them all together into one giant dataframe
        combined_df = pd.concat(data_frames, ignore_index=True)
        
        if "ingest_ts" in combined_df.columns:
            combined_df["ingest_ts"] = pd.to_datetime(combined_df["ingest_ts"]).astype(str)
            combined_df = combined_df.sort_values("ingest_ts", ascending=False)
            
        # Convert timestamps and NaNs for JSON serialization
        combined_df = combined_df.fillna(0)
        for col in combined_df.select_dtypes(include=['datetime64[ns]']).columns:
            combined_df[col] = combined_df[col].astype(str)
            
        # Return exactly 100 rows from the combined dataset
        return {"data": combined_df.head(100).to_dict(orient="records")}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    return {"data": []}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8005, reload=True)