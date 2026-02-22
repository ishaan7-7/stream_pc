from pathlib import Path
from datetime import datetime, timezone
import json
import shutil
from typing import Optional, Dict, Any


class CheckpointError(RuntimeError):
    
    pass


class CheckpointStore:
    

    def __init__(self, checkpoint_root: Path):
        self.checkpoint_root = checkpoint_root
        self.archive_root = checkpoint_root / "_archive"

        self.checkpoint_root.mkdir(parents=True, exist_ok=True)
        self.archive_root.mkdir(parents=True, exist_ok=True)

    def _checkpoint_path(self, source_id: str) -> Path:
        return self.checkpoint_root / f"{source_id}.json"

    
    def load(self, source_id: str) -> Optional[Dict[str, Any]]:
        
        path = self._checkpoint_path(source_id)

        if not path.exists():
            return None

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            raise CheckpointError(
                f"Failed to read checkpoint for {source_id}"
            ) from e

        self._validate_checkpoint(data, source_id)
        return data

    
    def save(
        self,
        *,
        source_id: str,
        last_row_index: int,
        last_row_hash: str,
    ) -> None:
        
        record = {
            "source_id": source_id,
            "last_row_index": int(last_row_index),
            "last_row_hash": str(last_row_hash),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        path = self._checkpoint_path(source_id)
        tmp_path = path.with_suffix(".json.tmp")

        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False)
            tmp_path.replace(path)  # atomic on same filesystem
        except Exception as e:
            raise CheckpointError(
                f"Failed to write checkpoint for {source_id}"
            ) from e

    
    def reset(self, source_id: str) -> None:
        
        path = self._checkpoint_path(source_id)

        if not path.exists():
            return

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archive_path = self.archive_root / f"{ts}_{source_id}.json"

        try:
            shutil.move(str(path), str(archive_path))
        except Exception as e:
            raise CheckpointError(
                f"Failed to archive checkpoint for {source_id}"
            ) from e

    
    def _validate_checkpoint(self, data: Dict[str, Any], source_id: str) -> None:
        required_keys = {
            "source_id",
            "last_row_index",
            "last_row_hash",
            "updated_at",
        }

        if set(data.keys()) != required_keys:
            raise CheckpointError(
                f"Invalid checkpoint schema for {source_id}"
            )

        if data["source_id"] != source_id:
            raise CheckpointError(
                f"Checkpoint source_id mismatch: expected {source_id}, "
                f"found {data['source_id']}"
            )
