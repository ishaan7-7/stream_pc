from pathlib import Path
from typing import List, Optional
import threading
import signal
import sys
import time
import shutil
from datetime import datetime, timezone

# --- NEW IMPORTS START ---
from prometheus_client import start_http_server
# --- NEW IMPORTS END ---

from replay.service.schema_loader import MasterSchema
from replay.service.source_discovery import discover_sources
from replay.service.validator import SchemaValidator
from replay.service.checkpoint import CheckpointStore
from replay.service.dlq import DLQWriter
from replay.service.http_client import HttpClient
from replay.service.worker import run_worker
# We must import REGISTRY to expose the correct metrics
from replay.service.metrics import active_sources, REGISTRY


class ReplayService:
    def __init__(
        self,
        *,
        pipeline_root: Path,
        enabled_sims: Optional[List[str]],
        replay_mode: str,
        rows_per_second: Optional[float] = None,
        batch_size: Optional[int] = None,
        batch_interval_seconds: Optional[float] = None,
        http_endpoint: str = "http://127.0.0.1:8000/ingest",
    ):
        self.pipeline_root = pipeline_root
        self.enabled_sims = enabled_sims
        self.replay_mode = replay_mode
        self.rows_per_second = rows_per_second
        self.batch_size = batch_size
        self.batch_interval_seconds = batch_interval_seconds

        self._threads: List[threading.Thread] = []
        self._shutdown_event = threading.Event()

        # -----------------------------
        # Core dependencies
        # -----------------------------
        self.schema = MasterSchema(
            pipeline_root / "contracts" / "master.json"
        )

        self.schema_validator = SchemaValidator(self.schema)

        self.checkpoint_store = CheckpointStore(
            pipeline_root / "replay" / "checkpoints"
        )

        self.dlq_writer = DLQWriter(
            pipeline_root / "replay" / "dlq"
        )

        self.http_client = HttpClient(endpoint=http_endpoint)

        self.sources = discover_sources(
            pipeline_root=pipeline_root,
            schema=self.schema,
            enabled_sims=enabled_sims,
        )

    # -------------------------------------------------
    # Internal thread target
    # -------------------------------------------------
    def _run_source(self, source):
        try:
            run_worker(
                source=source,
                schema_validator=self.schema_validator,
                checkpoint_store=self.checkpoint_store,
                dlq_writer=self.dlq_writer,
                http_client=self.http_client,
                replay_mode=self.replay_mode,
                shutdown_event=self._shutdown_event,
                rows_per_second=self.rows_per_second,
                batch_size=self.batch_size,
                batch_interval_seconds=self.batch_interval_seconds,
            )
        finally:
            active_sources.dec()

    # -------------------------------------------------
    # Lifecycle
    # -------------------------------------------------
    def start(self, *, reset: bool = False, archive_dlq: bool = True):
        # --- NEW: Start Metrics Server on Port 9001 ---
        # This matches "replay_metrics_url" in your dashboard config.
        # We pass `registry=REGISTRY` to ensure it serves YOUR custom metrics.
        try:
            start_http_server(9001, registry=REGISTRY)
            print("Replay metrics server started on port 9001")
        except OSError:
            print("Warning: Metrics server port 9001 might already be in use.")
        # ----------------------------------------------

        if reset:
            self.reset(archive_dlq=archive_dlq)

        self._shutdown_event.clear()
        active_sources.set(len(self.sources))

        for source in self.sources:
            t = threading.Thread(
                target=self._run_source,
                args=(source,),
                daemon=True,
            )
            self._threads.append(t)
            t.start()

        self._install_signal_handlers()

    def stop(self):
        self.stop_workers()
        sys.exit(0)

    def stop_workers(self):
        self._shutdown_event.set()

        for t in self._threads:
            t.join(timeout=10)

        self._threads.clear()
        active_sources.set(0)

    def wait(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    # -------------------------------------------------
    # Reset
    # -------------------------------------------------
    def reset(self, *, archive_dlq: bool = True):
        self.stop_workers()

        for source in self.sources:
            self.checkpoint_store.reset(source["source_id"])

        if archive_dlq:
            dlq_root = self.pipeline_root / "replay" / "dlq"
            if dlq_root.exists():
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                archive_dir = dlq_root.parent / f"dlq_archive_{ts}"
                shutil.move(str(dlq_root), str(archive_dir))
                dlq_root.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------
    # Signals
    # -------------------------------------------------
    def _install_signal_handlers(self):
        def handle_signal(signum, frame):
            self.stop()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
