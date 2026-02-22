
import os
import json
import duckdb
import pandas as pd
import sys

# --- CONFIGURATION ---
# 1. Calculate Paths relative to this script
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(TOOLS_DIR) # C:\streaming_emulator
DATA_ROOT = os.path.join(BASE_DIR, "data")
ARTIFACTS_DIR = os.path.join(BASE_DIR, "inference_service", "artifacts")
BRONZE_DIR = os.path.join(DATA_ROOT, "delta", "bronze")

# 2. Point to the SAME extension folder used by Inference Service
EXT_DIR = os.path.join(BASE_DIR, "inference_service", "duckdb_extensions")

# Modules to check
MODULES = ["engine", "body", "battery", "transmission", "tyre"]

def get_db_connection():
    """
    Creates a restricted DuckDB connection that stays inside streaming_emulator.
    """
    os.makedirs(EXT_DIR, exist_ok=True)
    
    conn = duckdb.connect(config={
        'extension_directory': EXT_DIR,
        'allow_unsigned_extensions': 'true'
    })
    
    try:
        # Quietly load Delta
        conn.execute("INSTALL delta; LOAD delta;")
    except Exception:
        pass
    return conn

def get_base_feature_name(feature_name):
    """
    Reverse-engineers the raw column name from the trained feature name.
    Ex: 'rpm_mean' -> 'rpm', 'speed_std' -> 'speed', 'temp' -> 'temp'
    """
    if feature_name.endswith("_mean"):
        return feature_name[:-5]
    elif feature_name.endswith("_std"):
        return feature_name[:-4]
    return feature_name

def check_module(module):
    print(f"\n🔍 CHECKING MODULE: {module.upper()}")
    print("=" * 60)

    # 1. Load Artifacts (Expectations)
    feat_path = os.path.join(ARTIFACTS_DIR, module, "features.json")
    if not os.path.exists(feat_path):
        print(f"❌ Artifact Missing: {feat_path}")
        return
    
    with open(feat_path, 'r') as f:
        trained_features = json.load(f)
    
    # Extract unique base columns required from the artifacts
    required_base_cols = set(get_base_feature_name(f) for f in trained_features)
    print(f"   📘 Model Expects: {len(required_base_cols)} raw columns (Derived into {len(trained_features)} features)")

    # 2. Load Bronze Schema (Reality)
    bronze_path = os.path.join(BRONZE_DIR, module)
    if not os.path.exists(bronze_path):
        print(f"❌ Bronze Table Missing: {bronze_path}")
        print("   (Run the Writer Service first to generate data)")
        return

    try:
        conn = get_db_connection()
            
        # Get schema without loading data (limit 0)
        query = f"SELECT * FROM delta_scan('{bronze_path}') LIMIT 0"
        df_schema = conn.execute(query).df()
        bronze_columns = set(df_schema.columns)
        
        print(f"   📙 Bronze Table:  {len(bronze_columns)} columns found")

        # 3. Analyze Differences
        missing_cols = []
        extra_cols = []
        
        # Check Missing
        for req in required_base_cols:
            if req not in bronze_columns:
                missing_cols.append(req)
        
        # Check Extra
        # We assume standard identity columns are "allowed" extras, but we list them anyway
        system_cols = {'source_id', 'timestamp', 'ingest_ts', 'row_hash', 'vehicle_id'}
        
        for col in bronze_columns:
            if col not in required_base_cols and col not in system_cols:
                extra_cols.append(col)

        # 4. REPORTING
        if missing_cols:
            print(f"\n   ❌ CRITICAL ERROR: Bronze is MISSING these required columns:")
            for m in sorted(missing_cols):
                print(f"      - {m} (Model will see ZEROS)")
        else:
            print(f"\n   ✅ PASSED: All required columns are present.")

        if extra_cols:
            print(f"\n   ℹ️  UNUSED COLUMNS: Present in Bronze but NOT used by Model:")
            for e in sorted(extra_cols):
                print(f"      - {e}")
        else:
             print(f"\n   ✨ CLEAN: No unused extra columns found.")

    except Exception as e:
        print(f"   ❌ Error reading Bronze table: {e}")

if __name__ == "__main__":
    print(f"📂 Root: {BASE_DIR}")
    
    for mod in MODULES:
        check_module(mod)
        
    print("\nDone.")
