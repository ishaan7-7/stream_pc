import csv
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, Iterator, Tuple, Optional
from threading import Event

from replay.service.validator import SchemaValidator, RowValidationError
from replay.service.checkpoint import CheckpointStore
from replay.service.dlq import DLQWriter
from replay.service.http_client import HttpClient
from replay.service.metrics import (
    rows_attempted_total,
    rows_sent_total,
    rows_failed_validation_total,
    batch_latency_ms,
)


# -------------------------------------------------
# CSV reader (resume-safe)
# -------------------------------------------------
def read_csv_rows(
    *,
    csv_path: Path,
    start_row_index: int,
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    if start_row_index < -1:
        raise ValueError("start_row_index must be >= -1")

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if idx <= start_row_index:
                continue
            yield idx, row


# -------------------------------------------------
# Deterministic row hashing
# -------------------------------------------------
def compute_row_hash(
    *,
    vehicle_id: str,
    module: str,
    timestamp: str,
    features: Dict[str, Any],
) -> str:
    canonical_payload = {
        "vehicle_id": vehicle_id,
        "module": module,
        "timestamp": timestamp,
        "features": {k: features[k] for k in sorted(features)},
    }

    serialized = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# -------------------------------------------------
# Main worker (single source)
# -------------------------------------------------
def run_worker(
    *,
    source: Dict[str, Any],
    schema_validator: SchemaValidator,
    checkpoint_store: CheckpointStore,
    dlq_writer: DLQWriter,
    http_client: HttpClient,
    replay_mode: str,
    shutdown_event: Event,
    rows_per_second: Optional[float] = None,
    batch_size: Optional[int] = None,
    batch_interval_seconds: Optional[float] = None,
) -> None:
    vehicle_id = source["vehicle_id"]
    module = source["module"]
    source_id = source["source_id"]
    csv_path: Path = source["csv_path"]

    checkpoint = checkpoint_store.load(source_id)
    start_index = checkpoint["last_row_index"] if checkpoint else -1

    batch = []
    batch_start_ts = None

    for row_index, raw_row in read_csv_rows(
        csv_path=csv_path,
        start_row_index=start_index,
    ):
        if shutdown_event.is_set():
            break

        rows_attempted_total.inc()

        # -------------------------
        # Hash + validate
        # -------------------------
        try:
            row_hash = compute_row_hash(
                vehicle_id=vehicle_id,
                module=module,
                timestamp=raw_row["timestamp"],
                features=raw_row,
            )

            validated = schema_validator.validate_row(
                module=module,
                row=raw_row,
            )

        except RowValidationError as e:
            rows_failed_validation_total.inc()
            dlq_writer.write(
                source_id=source_id,
                vehicle_id=vehicle_id,
                module=module,
                row_index=row_index,
                error_type="schema_validation",
                error_message=e.message,
                error_details=e.details,
                raw_row=raw_row,
            )
            continue

        except Exception as e:
            rows_failed_validation_total.inc()
            dlq_writer.write(
                source_id=source_id,
                vehicle_id=vehicle_id,
                module=module,
                row_index=row_index,
                error_type="hash_error",
                error_message=str(e),
                raw_row=raw_row,
            )
            continue

        payload = {
            "metadata": {
                "row_hash": row_hash,
                "vehicle_id": vehicle_id,
                "module": module,
                "source_file": csv_path.name,
            },
            "data": validated,
        }

        # -------------------------
        # Delivery
        # -------------------------
        try:
            if replay_mode == "fixed_rate":
                http_client.post_json(payload)
                rows_sent_total.inc()

                if rows_per_second:
                    time.sleep(1.0 / rows_per_second)

            elif replay_mode == "batch":
                if not batch:
                    batch_start_ts = time.monotonic()

                batch.append(payload)

                flush_due = (
                    (batch_size and len(batch) >= batch_size)
                    or (
                        batch_interval_seconds
                        and batch_start_ts
                        and (time.monotonic() - batch_start_ts >= batch_interval_seconds)
                    )
                )

                if flush_due:
                    with batch_latency_ms.time():
                        for item in batch:
                            http_client.post_json(item)
                            rows_sent_total.inc()
                    batch.clear()
                    batch_start_ts = None

            else:
                raise ValueError(f"Unknown replay mode: {replay_mode}")

        except Exception as e:
            dlq_writer.write(
                source_id=source_id,
                vehicle_id=vehicle_id,
                module=module,
                row_index=row_index,
                error_type="http_delivery",
                error_message=str(e),
                raw_row=raw_row,
            )
            continue

        # -------------------------
        # Checkpoint (only on success)
        # -------------------------
        checkpoint_store.save(
            source_id=source_id,
            last_row_index=row_index,
            last_row_hash=row_hash,
        )

    # Final batch flush on shutdown
    if replay_mode == "batch" and batch:
        with batch_latency_ms.time():
            for item in batch:
                http_client.post_json(item)
                rows_sent_total.inc()
