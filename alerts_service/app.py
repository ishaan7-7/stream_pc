
import time
import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from src.state_manager import AlertStateManager
from src.alert_engine import AlertEngine
from src import config

def is_valid_delta_table(path):
    """Safely checks if a path is a true Delta Table, avoiding Windows empty-folder traps."""
    if not os.path.exists(path): return False
    try:
        return DeltaTable.is_deltatable(path)
    except:
        return False

def main():
    print("=====================================================")
    print("🚨 STARTING GOLD AGGREGATOR (ALERTS & FAULTS)")
    print(f"📦 Global Batch Limit: {config.BATCH_SIZE} rows per loop")
    print(f"⚖️  Leaky Bucket Math: CRITICAL(+{config.SCORE_DELTAS.get('CRITICAL',0)}) | WARNING(+{config.SCORE_DELTAS.get('WARNING',0)}) | NORMAL({config.SCORE_DELTAS.get('NORMAL',0)})")
    print("=====================================================")
    
    state = AlertStateManager()
    engine = AlertEngine(state)
    
    # ⚠️ STRICT SCHEMA
    ALERT_SCHEMA = pa.schema([
        ("alert_id", pa.string()),
        ("source_id", pa.string()),
        ("module", pa.string()),
        ("status", pa.string()),
        ("alert_start_ts", pa.string()),
        ("alert_end_ts", pa.string()),
        ("peak_anomaly_ts", pa.string()),
        ("max_composite_score", pa.float64()),
        ("top_10_features", pa.string()),
        ("last_updated_ts", pa.string())
    ])
    
    while True:
        raw_frames = []
        new_checkpoints = {}
        
        # Fair batching: distribute the limit evenly across all enabled modules
        per_module_limit = max(1, config.BATCH_SIZE // len(config.ENABLED_MODULES))

        for mod in config.ENABLED_MODULES:
            path = os.path.join(config.SILVER_DIR, mod)
            if not os.path.exists(path): continue
            
            try:
                dt = DeltaTable(path)
                last_ts = state.checkpoints.get(mod, "1970-01-01")
                dataset = dt.to_pyarrow_dataset()
                
                if 'inference_ts' in dataset.schema.names:
                    filtered = dataset.scanner(filter=pc.field("inference_ts") > last_ts).to_table()
                    df = filtered.to_pandas()
                    
                    if not df.empty:
                        df = df.sort_values('inference_ts', ascending=True).head(per_module_limit)
                        df['module_name'] = mod
                        raw_frames.append(df)
                        new_checkpoints[mod] = str(df['inference_ts'].max())
            except Exception as e:
                pass # Delta locked for writing
                
        if not raw_frames:
            time.sleep(config.POLL_INTERVAL)
            continue

        combined_df = pd.concat(raw_frames, ignore_index=True)
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        combined_df = combined_df.sort_values('timestamp', ascending=True)

        print(f"⏳ Evaluating batch of {len(combined_df)} rows through Leaky Bucket...")

        alert_updates = {}
        
        for _, row in combined_df.iterrows():
            payload = engine.process_row(row)
            if payload:
                alert_updates[payload['alert_id']] = payload

        if alert_updates:
            updates_df = pd.DataFrame(list(alert_updates.values()))
            pa_table = pa.Table.from_pandas(updates_df, schema=ALERT_SCHEMA)
            
            # Robust write logic against Windows folder-lock corruption
            if not is_valid_delta_table(config.GOLD_ALERTS_DIR):
                if os.path.exists(config.GOLD_ALERTS_DIR):
                    shutil.rmtree(config.GOLD_ALERTS_DIR, ignore_errors=True)
                write_deltalake(config.GOLD_ALERTS_DIR, pa_table, mode="append")
            else:
                dt_alerts = DeltaTable(config.GOLD_ALERTS_DIR)
                dt_alerts.merge(
                    source=pa_table,
                    predicate="target.alert_id = source.alert_id",
                    source_alias="source",
                    target_alias="target"
                ).when_matched_update_all().when_not_matched_insert_all().execute()

        for mod, ts in new_checkpoints.items():
            state.checkpoints[mod] = ts
        state.save_state()
        
        if alert_updates:
            print(f"✅ Upserted {len(alert_updates)} Alert states.")

        time.sleep(config.POLL_INTERVAL)

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
