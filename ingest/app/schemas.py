from typing import Dict, Any
from pydantic import BaseModel, Field
from pydantic.config import ConfigDict



class Metadata(BaseModel):
    row_hash: str = Field(..., min_length=1)
    vehicle_id: str = Field(..., min_length=1)
    module: str = Field(..., min_length=1)
    source_file: str = Field(..., min_length=1)
    ingest_ts: str | None = None

    model_config = ConfigDict(extra="forbid")



class IngestRequest(BaseModel):
    metadata: Metadata
    data: Dict[str, Any]

    model_config = ConfigDict(extra="forbid")
