# File: C:\streaming_emulator\writer_service\src\schema_loader.py
import json
import logging
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, TimestampType
)

# Setup logging
logger = logging.getLogger("SchemaLoader")
logger.setLevel(logging.INFO)

def get_project_root():
    """Finds the root C:/streaming_emulator directory."""
    current = Path(__file__).parent.absolute()
    for parent in [current] + list(current.parents):
        if parent.name == "streaming_emulator":
            return parent
    return Path(r"C:\streaming_emulator")

def _map_dtype_to_spark(dtype_str):
    """
    Maps Pandas/JSON strings from master.json to Spark SQL Types.
    """
    dtype_str = str(dtype_str).lower()
    
    if 'float' in dtype_str:
        return DoubleType()
    elif 'int' in dtype_str:
        return LongType()
    elif 'bool' in dtype_str:
        return BooleanType()
    elif 'datetime' in dtype_str or 'iso8601' in dtype_str:
        return TimestampType()
    else:
        return StringType()

def load_all_schemas():
    """
    Reads contracts/master.json and returns a dictionary of Spark Schemas.
    Target Structure: { "engine": StructType(...), ... }
    """
    root = get_project_root()
    master_path = root / "contracts" / "master.json"
    
    if not master_path.exists():
        logger.error(f"❌ Contract missing at: {master_path}")
        raise FileNotFoundError(f"master.json not found at {master_path}")

    try:
        with open(master_path, 'r') as f:
            contract = json.load(f)
            
        schemas = {}
        
        # 1. Get the Modules Definition
        # This fixes the previous error by looking inside the "modules" key
        modules_config = contract.get("modules", {})
        
        if not modules_config:
            logger.warning("⚠️ No modules found in master.json!")

        for module_name, module_def in modules_config.items():
            
            # A. Build 'data' struct (Dynamic from columns)
            columns = module_def.get("columns", {})
            data_fields = []
            
            for col_name, col_props in columns.items():
                # master.json structure: "rpm": { "dtype": "float64", ... }
                dtype = col_props.get("dtype", "string")
                spark_type = _map_dtype_to_spark(dtype)
                data_fields.append(StructField(col_name, spark_type, True))
            
            # B. Build 'metadata' struct (Fixed Protocol)
            # We strictly define this to ensure we capture row_hash and ingest_ts
            # This is what enables your Writer Latency calculations later.
            metadata_fields = [
                StructField("row_hash", StringType(), False),      # Idempotency Key
                StructField("ingest_ts", TimestampType(), True),   # T1: Latency Start Time
                StructField("vehicle_id", StringType(), True),
                StructField("module", StringType(), True),
                StructField("source_file", StringType(), True)
            ]
            
            # C. Composite Schema
            # Matches Kafka Payload: { "metadata": {...}, "data": {...} }
            composite_schema = StructType([
                StructField("metadata", StructType(metadata_fields), True),
                StructField("data", StructType(data_fields), True)
            ])
            
            schemas[module_name] = composite_schema
            logger.debug(f"✅ Loaded schema for: {module_name}")
            
        logger.info(f"✅ Successfully loaded {len(schemas)} schemas from master.json")
        return schemas

    except Exception as e:
        logger.error(f"❌ Failed to parse master.json: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig()
    try:
        s = load_all_schemas()
        print(f"Test Load: Found {len(s)} modules.")
    except Exception as e:
        print(f"Test Failed: {e}")