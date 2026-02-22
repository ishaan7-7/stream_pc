# File: C:\streaming_emulator\writer_service\src\metrics_listener.py
import json
import time
import logging
from pathlib import Path
from pyspark.sql.streaming import StreamingQueryListener

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MetricsListener")

class WriterMetricsListener(StreamingQueryListener):
    def __init__(self, module_name="unknown"):
        # Go up two levels from src/ to get to writer_service/ root
        root = Path(__file__).parent.parent
        self.state_dir = root / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Unique file per module
        self.metrics_file = self.state_dir / f"writer_metrics_{module_name}.json"
        
        self.state = {
            "status": "INITIALIZING",
            "last_updated": time.time(),
            "streams": {} 
        }
        self.total_rows_accumulator = {}

    def onQueryStarted(self, event):
        print(f"▶️ STREAM STARTED: {event.name}")
        self.state["status"] = "RUNNING"
        self.state["streams"][event.name] = {
            "status": "STARTING",
            "id": str(event.id),
            "runId": str(event.runId),
            "total_rows_processed": 0,
            "backlog": 0
        }
        self.total_rows_accumulator[str(event.runId)] = 0
        self._flush()

    def onQueryProgress(self, event):
        p = event.progress
        name = p.name
        run_id = str(p.runId)
        
        # 1. Update Lifetime Total
        current_total = self.total_rows_accumulator.get(run_id, 0)
        new_total = current_total + p.numInputRows
        self.total_rows_accumulator[run_id] = new_total

        # 2. Extract Kafka Backlog (Lag)
        # Spark provides 'sources' list. We look for the Kafka source metrics.
        backlog = 0
        if p.sources:
            for src in p.sources:
                # Look for typical lag metrics (offsetsBehindLatest)
                # Note: These keys can vary slightly by Spark version/Config
                metrics = src.metrics
                if metrics:
                    # Try 'maxOffsetsBehindLatest' (Standard) or fallback
                    lag = metrics.get("maxOffsetsBehindLatest") or metrics.get("avgOffsetsBehindLatest")
                    if lag:
                        try:
                            backlog += int(float(lag))
                        except: pass

        if p.numInputRows > 0:
            print(f"⚡ BATCH: {name} | Rows: {p.numInputRows} | Total: {new_total} | Lag: {backlog}")

        metrics = {
            "status": "ACTIVE",
            "timestamp": p.timestamp,
            "batch_id": p.batchId,
            "num_input_rows": p.numInputRows,
            "total_rows_processed": new_total,
            "backlog": backlog,  # <--- NEW METRIC
            "input_rate": p.inputRowsPerSecond,
            "process_rate": p.processedRowsPerSecond,
            "duration_ms": p.durationMs.get("triggerExecution", 0)
        }
        
        self.state["streams"][name] = metrics
        self.state["last_updated"] = time.time()
        self._flush()

    def onQueryTerminated(self, event):
        print(f"🛑 STREAM STOPPED: {event.runId}")
        if event.exception:
            self.state["status"] = "ERROR"
            logger.error(f"Stream Error: {event.exception}")
        self._flush()

    def _flush(self):
        try:
            temp_file = self.metrics_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(self.state, f, indent=2)
            temp_file.replace(self.metrics_file)
        except Exception as e:
            logger.error(f"Write failed: {e}")