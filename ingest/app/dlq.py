from pathlib import Path
from datetime import datetime, timezone
import json
from typing import Dict, Any, Optional
from threading import Lock


class IngestDLQError(RuntimeError):
    pass


class IngestDLQWriter:
    """
    DLQ writer for ingest service.
    Append-only JSONL, one file per day.
    """

    def __init__(self, dlq_root: Path):
        self.dlq_root = dlq_root
        self._lock = Lock()
        self.dlq_root.mkdir(parents=True, exist_ok=True)

    def _today_file(self) -> Path:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        return self.dlq_root / f"ingest_dlq_{day}.jsonl"

    def write(
        self,
        *,
        error_type: str,
        error_message: str,
        event: Dict[str, Any],
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if error_details is None:
            error_details = {}

        record = {
            "error_type": error_type,
            "error_message": error_message,
            "error_details": error_details,
            "event": event,
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
        }

        try:
            with self._lock:
                with self._today_file().open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False))
                    f.write("\n")
        except Exception as e:
            raise IngestDLQError("Failed to write ingest DLQ record") from e
