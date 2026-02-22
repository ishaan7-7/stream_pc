from pathlib import Path
from datetime import datetime, timezone
import json
from typing import Dict, Any


class DLQError(RuntimeError):

    pass


class DLQWriter:

    def __init__(self, dlq_root: Path):
        self.dlq_root = dlq_root

        if not self.dlq_root.exists():
            self.dlq_root.mkdir(parents=True, exist_ok=True)

    def _today_dir(self) -> Path:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        day_dir = self.dlq_root / day
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir

    def write(
        self,
        *,
        source_id: str,
        vehicle_id: str,
        module: str,
        row_index: int,
        error_type: str,
        error_message: str,
        raw_row: Dict[str, Any],
        error_details: Dict[str, Any] | None = None,
    ) -> None:
   

        if error_details is None:
            error_details = {}

        record = {
            "source_id": source_id,
            "vehicle_id": vehicle_id,
            "module": module,
            "row_index": row_index,
            "error_type": error_type,
            "error_message": error_message,
            "error_details": error_details,
            "raw_row": raw_row,
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
        }

        day_dir = self._today_dir()
        dlq_file = day_dir / f"{source_id}.jsonl"

        try:
            with dlq_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False))
                f.write("\n")
        except Exception as e:
            raise DLQError(
                f"Failed to write DLQ record for source_id={source_id}"
            ) from e
