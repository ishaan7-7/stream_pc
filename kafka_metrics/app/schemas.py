import json
from typing import Any, Dict, Optional
from datetime import datetime


class KafkaMessageParseError(RuntimeError):
    """Raised when a Kafka message cannot be parsed."""
    pass


class ParsedKafkaMessage:
    """
    Minimal parsed representation of a Kafka message value.

    - Extracts only required metadata
    - Preserves full JSON payload untouched
    - Does NOT validate schema
    """

    def __init__(
        self,
        *,
        raw_json: Dict[str, Any],
        vehicle_id: str,
        module: str,
        ingest_ts_ms: Optional[int],
    ):
        self.raw_json = raw_json
        self.vehicle_id = vehicle_id
        self.module = module
        self.ingest_ts_ms = ingest_ts_ms


def parse_message(payload: Dict[str, Any]) -> ParsedKafkaMessage:
    """
    Parse a decoded Kafka JSON payload.

    Required:
      - metadata.vehicle_id
      - metadata.module

    Optional:
      - metadata.ingest_ts (ISO-8601 → epoch ms)

    Full JSON is preserved untouched.
    """

    if not isinstance(payload, dict):
        raise KafkaMessageParseError("Kafka payload must be a JSON object")

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise KafkaMessageParseError("Missing or invalid metadata")

    vehicle_id = metadata.get("vehicle_id")
    module = metadata.get("module")
    ingest_ts = metadata.get("ingest_ts")

    if not isinstance(vehicle_id, str):
        raise KafkaMessageParseError("metadata.vehicle_id is required")

    if not isinstance(module, str):
        raise KafkaMessageParseError("metadata.module is required")

    ingest_ts_ms: Optional[int] = None
    if isinstance(ingest_ts, str):
        try:
            dt = datetime.fromisoformat(ingest_ts.replace("Z", "+00:00"))
            ingest_ts_ms = int(dt.timestamp() * 1000)
        except Exception:
            ingest_ts_ms = None  # graceful degradation

    return ParsedKafkaMessage(
        raw_json=payload,
        vehicle_id=vehicle_id,
        module=module,
        ingest_ts_ms=ingest_ts_ms,
    )
