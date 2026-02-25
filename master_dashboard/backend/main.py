import os
import sys
import json
import time
import asyncio
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, TopicPartition
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from utils import safe_read_json, safe_read_pickle
from concurrent.futures import ProcessPoolExecutor
import plotly.express as px
from pydantic import BaseModel
import uuid
import datetime
from collections import defaultdict, deque
import aiohttp
import re
from collections import defaultdict, deque

# --- OBSERVER & NETWORK STATE ---
PORTS_TO_CHECK = {
    "Zookeeper": 2181,
    "Kafka": 9092,
    "Ingest": 8000,
    "Replay/Dashboard API": 8005, 
    "React UI": 5173
}

VALIDATION_REGEX = re.compile(r'ingest_rows_validation_detail(?:_total)?\{.*vehicle_id="([^"]+)".*status="([^"]+)".*\}\s+(\d+\.?\d*)')
DLQ_GAUGE_REGEX = re.compile(r'dlq_size_files\s+(\d+\.?\d*)')

OBSERVER_HISTORY_LEN = 300

OBSERVER_CACHE = {
    "system_health": {k: False for k in PORTS_TO_CHECK},
    "global_stats": {
        "total_rows": 0,
        "active_vehicles": 0,
        "avg_latency": 0.0,
        "dlq_backlog": 0
    },
    "vehicles": {}
}

# --- OBSERVER STATE ---
# 1. DEFINE APP FIRST
app = FastAPI(
    title="Master Dashboard API",
    description="Read-only data aggregator and execution layer"
)

# 2. THEN ADD MIDDLEWARE
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:5173"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
VEHICLE_MODULES = ["battery", "body", "engine", "transmission", "tyre"]
DELTA_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "bronze")
SILVER_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "silver")
KAFKA_BROKER = "localhost:9092"
GOLD_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "gold", "vehicle_health")
sys.path.append(os.path.join(PROJECT_ROOT, "gold_service"))
ALERTS_ROOT = os.path.join(PROJECT_ROOT, "data", "delta", "gold", "alerts")
ALERTS_CHECKPOINT = os.path.join(PROJECT_ROOT, "alerts_service", "state", "checkpoints.json")
sys.path.append(os.path.join(PROJECT_ROOT, "alerts_service"))
process_pool = ProcessPoolExecutor(max_workers=2)
try:
    from src import config as gold_config
    GOLD_ENABLED_MODULES = gold_config.ENABLED_MODULES
    GOLD_WEIGHTS = gold_config.NORMALIZED_WEIGHTS
    GOLD_PENALTIES = gold_config.TIER_1_PENALTIES
except ImportError:
    # Fallback if config is inaccessible
    GOLD_ENABLED_MODULES = ["engine", "transmission", "battery", "body", "tyre"]
    GOLD_WEIGHTS = {"engine": 0.35, "transmission": 0.25, "battery": 0.20, "body": 0.10, "tyre": 0.10}
    GOLD_PENALTIES = {"engine": 30.0, "transmission": 25.0, "battery": 20.0}
# --- REPLAY STATE ---
REPLAY_SCENARIOS_DIR = os.path.join(PROJECT_ROOT, "data", "scenarios")
os.makedirs(REPLAY_SCENARIOS_DIR, exist_ok=True) # Ensure directory exists

ACTIVE_REPLAYS = {}

replay_producer: AIOKafkaProducer = None

# --- CACHES FOR HIGH-FREQUENCY POLLING ---
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

INFERENCE_METRICS_CACHE = {
    "active_sims": 0,
    "active_modules": 0,
    "global_e2e_ms": 0,
    "global_inf_ms": 0,
    "module_stats": {},
    "recent_alerts": []
}

GOLD_METRICS_CACHE = {
    "active_sims": [],
    "total_gold_rows": 0,
    "processing_lags": {mod: 0 for mod in GOLD_ENABLED_MODULES}
}

ALERTS_METRICS_CACHE = {
    "active_alerts_count": 0,
    "critical_vehicles": 0,
    "processing_lag": 0,
    "open_alerts": [],
    "closed_alerts": []
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
            print(f"Writer metric update failed: {e}")
        await asyncio.sleep(2)

async def update_inference_metrics_loop():
    """Profiles the Silver layer and alerts for the ML Engine Dashboard"""
    while True:
        try:
            # 1. Load System Alerts (Last 5 mins)
            all_alerts = []
            for mod in VEHICLE_MODULES:
                alerts_file = os.path.join(PROJECT_ROOT, "inference_service", "state", f"system_alerts_{mod}.json")
                alerts = safe_read_json(alerts_file)
                if alerts:
                    all_alerts.extend(alerts)

            cutoff_dt = pd.Timestamp.utcnow() - pd.Timedelta(minutes=5)
            recent_alerts = []
            for a in all_alerts:
                try:
                    alert_time = pd.to_datetime(a['timestamp'], utc=True)
                    if alert_time >= cutoff_dt:
                        recent_alerts.append(a)
                except: pass
            recent_alerts.sort(key=lambda x: x['timestamp'], reverse=True)

            # 2. Compute Silver Metrics
            sims = set()
            module_stats = {}
            e2e_list = []
            inf_list = []

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
                for f in files[:5]: # Check recent parquets for 5-minute window
                    try:
                        df = pd.read_parquet(f)
                        if not df.empty and 'inference_ts' in df.columns:
                            df['inference_ts'] = pd.to_datetime(df['inference_ts'], utc=True)
                            df = df[df['inference_ts'] >= cutoff_dt]
                            if not df.empty:
                                dfs.append(df)
                    except: pass

                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
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

            # Update Cache
            INFERENCE_METRICS_CACHE["active_sims"] = len(sims)
            INFERENCE_METRICS_CACHE["active_modules"] = len(module_stats)
            INFERENCE_METRICS_CACHE["global_e2e_ms"] = round(sum(e2e_list)/len(e2e_list), 1) if e2e_list else 0
            INFERENCE_METRICS_CACHE["global_inf_ms"] = round(sum(inf_list)/len(inf_list), 1) if inf_list else 0
            INFERENCE_METRICS_CACHE["module_stats"] = module_stats
            INFERENCE_METRICS_CACHE["recent_alerts"] = recent_alerts[:10]

        except Exception as e:
            print(f"Inference metrics loop failed: {e}")
            
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    global replay_producer
    replay_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    try:
        await replay_producer.start()
    except Exception as e:
        print(f"Replay Producer failed to start: {e}")

    asyncio.create_task(update_writer_metrics_loop())
    asyncio.create_task(update_inference_metrics_loop())
    asyncio.create_task(update_gold_metrics_loop())
    asyncio.create_task(update_alerts_metrics_loop())
    asyncio.create_task(observer_health_loop())      
    asyncio.create_task(observer_kafka_loop())

@app.on_event("shutdown")
async def shutdown_event():
    global replay_producer
    if replay_producer:
        await replay_producer.stop()

@app.get("/health")
def health_check():
    return {"status": "Master Dashboard Backend is Online", "port": 8005}

# --- WRITER OPS ENDPOINTS ---
@app.get("/api/writer/metrics")
def get_writer_metrics():
    return WRITER_METRICS_CACHE

@app.get("/api/writer/inspector/{module}")
def get_writer_inspector(module: str):
    if module not in VEHICLE_MODULES:
        raise HTTPException(status_code=400, detail="Invalid module")
        
    path = os.path.join(DELTA_ROOT, module)
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
        if "ingest_ts" in combined_df.columns:
            combined_df["ingest_ts"] = pd.to_datetime(combined_df["ingest_ts"]).astype(str)
            combined_df = combined_df.sort_values("ingest_ts", ascending=False)
            
        combined_df = combined_df.fillna(0)
        for col in combined_df.select_dtypes(include=['datetime64[ns]']).columns:
            combined_df[col] = combined_df[col].astype(str)
            
        return {"data": combined_df.head(100).to_dict(orient="records")}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- INFERENCE OPS ENDPOINTS ---
@app.get("/api/inference/metrics")
def get_inference_metrics():
    """Powers the KPI metrics, alerts, and latency breakdown."""
    return INFERENCE_METRICS_CACHE

@app.get("/api/inference/tail/{module}")
def get_inference_tail(module: str):
    """Powers the Live Silver Data (Tail) view"""
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
        # Handle both timezone-naive and timezone-aware datetimes generated by PyTorch/Spark
        for col in combined_df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            combined_df[col] = combined_df[col].astype(str)
            
        return {"data": combined_df.head(100).to_dict(orient="records")}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
async def update_gold_metrics_loop():
    while True:
        try:
            silver_counts = {m: 0 for m in GOLD_ENABLED_MODULES}
            for mod in GOLD_ENABLED_MODULES:
                silver_path = os.path.join(SILVER_ROOT, mod)
                if os.path.exists(silver_path):
                    s_files = [os.path.join(r, f) for r, d, f in os.walk(silver_path) for f in f if f.endswith(".parquet")]
                    for f in s_files:
                        try: silver_counts[mod] += len(pd.read_parquet(f))
                        except: pass
            
            gold_count = 0
            active_sims = set()
            if os.path.exists(GOLD_ROOT):
                g_files = [os.path.join(r, f) for r, d, f in os.walk(GOLD_ROOT) for f in f if f.endswith(".parquet")]
                for f in g_files:
                    try:
                        df = pd.read_parquet(f)
                        gold_count += len(df)
                        if 'source_id' in df.columns: active_sims.update(df['source_id'].unique().tolist())
                    except: pass
            
            GOLD_METRICS_CACHE["active_sims"] = sorted(list(active_sims))
            GOLD_METRICS_CACHE["total_gold_rows"] = gold_count
            GOLD_METRICS_CACHE["processing_lags"] = {mod: max(0, silver_counts[mod] - gold_count) for mod in GOLD_ENABLED_MODULES}
            
        except Exception as e: 
            print(f"Gold metrics loop failed: {e}")
        await asyncio.sleep(2)

# --- GOLD ENDPOINTS ---

@app.get("/api/gold/metrics")
def get_gold_metrics():
    return GOLD_METRICS_CACHE

@app.get("/api/gold/config")
def get_gold_config():
    """Provides the aggregator config for the React Experimentation UI"""
    return {
        "enabled_modules": GOLD_ENABLED_MODULES,
        "default_weights": GOLD_WEIGHTS,
        "tier_1_penalties": GOLD_PENALTIES
    }

@app.get("/api/gold/history/{sim_id}")
def get_gold_history(sim_id: str):
    """Fetches history of a specific sim, or the entire fleet if sim_id='ALL'"""
    if not os.path.exists(GOLD_ROOT): return {"data": []}
        
    files = [os.path.join(r, f) for r, d, f in os.walk(GOLD_ROOT) for f in f if f.endswith(".parquet")]
    if not files: return {"data": []}
        
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if 'source_id' in df.columns:
                if sim_id.upper() == "ALL":
                    dfs.append(df)
                else:
                    sim_df = df[df['source_id'] == sim_id]
                    if not sim_df.empty: dfs.append(sim_df)
        except Exception: pass
        
    if not dfs: return {"data": []}
    combined_df = pd.concat(dfs, ignore_index=True)
    
    if 'gold_window_ts' in combined_df.columns:
        combined_df['gold_window_ts'] = pd.to_datetime(combined_df['gold_window_ts'])
        combined_df = combined_df.sort_values('gold_window_ts', ascending=True)
        # Only drop duplicates if we are looking at a single sim
        if sim_id.upper() != "ALL":
            combined_df = combined_df.drop_duplicates(subset=['gold_window_ts'], keep='last')
            
    combined_df = combined_df.fillna(0)
    for col in combined_df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        combined_df[col] = combined_df[col].astype(str)
        
    # Limit to last 1000 to prevent browser crash when viewing entire fleet
    return {"data": combined_df.tail(1000).to_dict(orient="records")}

# --- ALERTS & DTC ENDPOINTS ---

async def update_alerts_metrics_loop():
    while True:
        try:
            lag_rows = 0
            # 1. Calculate Lag using Checkpoints vs Silver
            try:
                if os.path.exists(ALERTS_CHECKPOINT) and os.path.exists(SILVER_ROOT):
                    ckpt = safe_read_json(ALERTS_CHECKPOINT) or {}
                    primary_mod = GOLD_ENABLED_MODULES[0] if GOLD_ENABLED_MODULES else "engine"
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

            # 3. Update Cache
            ALERTS_METRICS_CACHE["active_alerts_count"] = active_alerts
            ALERTS_METRICS_CACHE["critical_vehicles"] = crit_vehicles
            ALERTS_METRICS_CACHE["processing_lag"] = lag_rows
            ALERTS_METRICS_CACHE["open_alerts"] = open_alerts
            ALERTS_METRICS_CACHE["closed_alerts"] = closed_alerts

        except Exception as e:
            print(f"Alerts metrics loop failed: {e}")
        await asyncio.sleep(2)

@app.get("/api/alerts/metrics")
def get_alerts_metrics():
    return ALERTS_METRICS_CACHE

def _execute_dtc_pipeline(module: str, source_id: str, peak_ts: str):
    """Executes heavy PyTorch inference in an isolated process"""
    try:
        import pandas as pd
        from DTC_service.analyzer import DTCAdapter
        
        adapter = DTCAdapter(module_name=module)
        
        # 1. Convert the raw web string into a native Pandas Datetime object
        # This allows the analyzer to do time-window math (e.g. peak_ts +/- 2 minutes)
        peak_datetime = pd.to_datetime(peak_ts)
        
        # 2. Architecturally, this is fetching from the Bronze layer
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
            # Apply strictly industrial white theme formatting
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

@app.get("/api/dtc/analyze")
def analyze_dtc(module: str, source_id: str, peak_ts: str):
    try:
        # Offload to process pool to prevent blocking the FastAPI event loop
        future = process_pool.submit(_execute_dtc_pipeline, module, source_id, peak_ts)
        result = future.result(timeout=60)
        return result
    except Exception as e:
        return {"error": f"DTC Analysis computation failed: {str(e)}"}
    
# --- TELEMETRY REPLAY ENDPOINTS ---

class ReplayRequest(BaseModel):
    scenario_file: str
    target_sim_id: str = "sim_replay_01"
    speed_multiplier: float = 1.0

async def _replay_worker(job_id: str, req: ReplayRequest):
    try:
        file_path = os.path.join(REPLAY_SCENARIOS_DIR, req.scenario_file)
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            raise ValueError("Unsupported file format")

        ACTIVE_REPLAYS[job_id]["total_rows"] = len(df)
        
        for idx, row in df.iterrows():
            if ACTIVE_REPLAYS[job_id]["status"] == "STOPPING":
                ACTIVE_REPLAYS[job_id]["status"] = "STOPPED"
                break
                
            payload = row.dropna().to_dict()
            payload["source_id"] = req.target_sim_id 
            
            for k, v in payload.items():
                if isinstance(v, (pd.Timestamp, datetime.datetime)):
                    payload[k] = v.isoformat()
                    
            module = payload.get("module", "engine").lower()
            topic = f"telemetry.{module}"
            
            if replay_producer:
                await replay_producer.send_and_wait(
                    topic, 
                    key=req.target_sim_id.encode('utf-8'), 
                    value=json.dumps(payload).encode('utf-8')
                )
                
            ACTIVE_REPLAYS[job_id]["progress"] = idx + 1
            await asyncio.sleep(0.1 / req.speed_multiplier)
            
        if ACTIVE_REPLAYS[job_id]["status"] != "STOPPED":
            ACTIVE_REPLAYS[job_id]["status"] = "COMPLETED"
            
    except Exception as e:
        ACTIVE_REPLAYS[job_id]["status"] = f"FAILED: {str(e)}"

@app.get("/api/replay/scenarios")
def get_replay_scenarios():
    """Powers the dropdown menu to select a historical file"""
    if not os.path.exists(REPLAY_SCENARIOS_DIR):
        return {"scenarios": []}
    files = [f for f in os.listdir(REPLAY_SCENARIOS_DIR) if f.endswith('.parquet') or f.endswith('.csv')]
    return {"scenarios": files}

@app.get("/api/replay/active")
def get_active_replays():
    """Powers the Active Replay Jobs table"""
    return ACTIVE_REPLAYS

@app.post("/api/replay/start")
def start_replay(req: ReplayRequest):
    """Fires up an asynchronous injection task"""
    if not replay_producer:
        return {"error": "Kafka Producer not connected."}
        
    job_id = str(uuid.uuid4())[:8]
    ACTIVE_REPLAYS[job_id] = {
        "job_id": job_id,
        "scenario": req.scenario_file,
        "sim_id": req.target_sim_id,
        "status": "RUNNING",
        "progress": 0,
        "total_rows": 0,
        "speed": req.speed_multiplier,
        "start_time": datetime.datetime.utcnow().isoformat()
    }
    asyncio.create_task(_replay_worker(job_id, req))
    return {"success": True, "job_id": job_id}

@app.post("/api/replay/stop/{job_id}")
def stop_replay(job_id: str):
    """Gracefully halts an active injection"""
    if job_id in ACTIVE_REPLAYS:
        if ACTIVE_REPLAYS[job_id]["status"] == "RUNNING":
            ACTIVE_REPLAYS[job_id]["status"] = "STOPPING"
        return {"success": True}
    return {"error": "Job not found"}

# --- TELEMETRY OBSERVER & REPLAY DASHBOARD ENDPOINTS ---

async def observer_health_loop():
    """Scans local ports and scrapes the Ingest /metrics endpoint for Data Quality stats"""
    while True:
        try:
            # 1. Port Scanner
            for name, port in PORTS_TO_CHECK.items():
                try:
                    _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=0.2)
                    writer.close()
                    await writer.wait_closed()
                    OBSERVER_CACHE["system_health"][name] = True
                except:
                    OBSERVER_CACHE["system_health"][name] = False
            
            # 2. HTTP Metrics Poller (DLQ and Rejected Rows)
            if OBSERVER_CACHE["system_health"].get("Ingest", False):
                async with aiohttp.ClientSession() as session:
                    async with session.get("http://127.0.0.1:8000/metrics", timeout=1) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            temp_dlq = 0
                            for line in text.splitlines():
                                if line.startswith("#"): continue
                                
                                v_match = VALIDATION_REGEX.search(line)
                                if v_match:
                                    v_id, status, val = v_match.groups()
                                    if status == "rejected" and v_id in OBSERVER_CACHE["vehicles"]:
                                        OBSERVER_CACHE["vehicles"][v_id]["rejected"] = int(float(val))
                                    continue

                                d_match = DLQ_GAUGE_REGEX.search(line)
                                if d_match:
                                    temp_dlq = int(float(d_match.group(1)))
                                    
                            OBSERVER_CACHE["global_stats"]["dlq_backlog"] = temp_dlq
        except Exception:
            pass
        await asyncio.sleep(2)

async def observer_kafka_loop():
    topics = [f"telemetry.{m}" for m in VEHICLE_MODULES]
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f'master_dashboard_observer_{uuid.uuid4().hex[:8]}',
        auto_offset_reset='latest'
    )
    
    try:
        await consumer.start()
    except Exception as e:
        print(f"Observer Consumer failed to start: {e}")
        return

    try:
        async for msg in consumer:
            try:
                val = msg.value
                if not val: continue
                
                payload = json.loads(val.decode('utf-8'))
                meta = payload.get("metadata", payload)
                data_body = payload.get("data", payload)
                
                v_id = meta.get("vehicle_id") or payload.get("source_id", "unknown_sim")
                module = meta.get("module", payload.get("module", "unknown")).lower()
                ingest_ts_str = meta.get("ingest_ts", payload.get("ingest_ts"))

                if v_id not in OBSERVER_CACHE["vehicles"]:
                    OBSERVER_CACHE["vehicles"][v_id] = {
                        "accepted": 0, "rejected": 0, "latency_sum": 0.0, "latency_count": 0,
                        "last_seen": time.time(), "latest_payload": {}, "module_payloads": {},
                        "history": defaultdict(lambda: {
                            "timestamps": deque(maxlen=OBSERVER_HISTORY_LEN),
                            "metrics": defaultdict(lambda: deque(maxlen=OBSERVER_HISTORY_LEN))
                        })
                    }

                entry = OBSERVER_CACHE["vehicles"][v_id]
                entry["accepted"] += 1
                entry["last_seen"] = time.time()

                latency_ms = 0.0
                if ingest_ts_str:
                    try:
                        ts = pd.to_datetime(ingest_ts_str, utc=True)
                        latency_ms = max(0, (pd.Timestamp.utcnow() - ts).total_seconds() * 1000)
                    except: pass

                entry["latency_sum"] += latency_ms
                entry["latency_count"] += 1
                entry["latest_payload"] = payload
                entry["module_payloads"][module] = payload

                now_str = datetime.datetime.utcnow().strftime("%H:%M:%S")
                mod_hist = entry["history"][module]
                mod_hist["timestamps"].append(now_str)

                for k, v in data_body.items():
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        mod_hist["metrics"][k].append(v)

            except Exception:
                pass
    finally:
        await consumer.stop()

@app.get("/api/observer/snapshot")
def get_observer_snapshot():
    """Serves the live buffered data to the React Observer UI"""
    total_rows = 0
    global_lat_sum = 0
    global_lat_count = 0
    vehicle_list = []
    
    current_time = time.time()

    # Iterate safely to package the nested deques into pure JSON lists
    for v_id, data in OBSERVER_CACHE["vehicles"].items():
        acc = data["accepted"]
        rej = data["rejected"]
        total = acc + rej
        val_rate = (acc / total * 100.0) if total > 0 else 100.0
        
        v_lat = (data["latency_sum"] / data["latency_count"]) if data["latency_count"] > 0 else 0
        global_lat_sum += data["latency_sum"]
        global_lat_count += data["latency_count"]
        
        ago = round(current_time - data["last_seen"], 1)
        
        clean_history = {}
        for mod, h_data in data["history"].items():
            clean_history[mod] = {
                "timestamps": list(h_data["timestamps"]),
                "metrics": {k: list(v) for k, v in h_data["metrics"].items()}
            }
            
        vehicle_list.append({
            "vehicle_id": v_id,
            "rows_processed": acc,
            "rejected_rows": rej,
            "validation_rate": round(val_rate, 1),
            "avg_latency": round(v_lat, 1),
            "last_seen_sec": ago,
            "latest_payload": data["latest_payload"],
            "module_payloads": data["module_payloads"],
            "history": clean_history
        })
        total_rows += acc
        
    global_avg_lat = (global_lat_sum / global_lat_count) if global_lat_count > 0 else 0.0
    
    OBSERVER_CACHE["global_stats"]["total_rows"] = total_rows
    OBSERVER_CACHE["global_stats"]["active_vehicles"] = len(vehicle_list)
    OBSERVER_CACHE["global_stats"]["avg_latency"] = round(global_avg_lat, 1)

    return {
        "system_health": OBSERVER_CACHE["system_health"],
        "global_stats": OBSERVER_CACHE["global_stats"],
        "vehicles": vehicle_list
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8005, reload=True)