import time
import os
import pandas as pd
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from src.state_manager import GoldStateManager
from src.aggregator import HealthAggregator
from src import config

def main():
    print("=====================================================")
    print("🥇 STARTING GOLD AGGREGATOR (VEHICLE HEALTH)")
    print(f"📦 Batch Limit: {config.BATCH_SIZE} rows per module")
    print(f"⏱️ Aggregation Window: {config.AGGREGATION_WINDOW_SEC} seconds")
    print(f"🔗 Enabled Modules: {config.ENABLED_MODULES}")
    print("=====================================================")
    
    state = GoldStateManager()
    aggregator = HealthAggregator(state)
    
    while True:
        raw_frames = []
        new_checkpoints = {}

        # 1. Sweep strictly the ENABLED Silver Tables
        for mod in config.ENABLED_MODULES:
            path = os.path.join(config.SILVER_DIR, mod)
            if not os.path.exists(path): 
                continue
            
            try:
                dt = DeltaTable(path)
                last_ts = state.checkpoints.get(mod, "1970-01-01")
                dataset = dt.to_pyarrow_dataset()
                
                if 'inference_ts' in dataset.schema.names:
                    filtered = dataset.scanner(filter=pc.field("inference_ts") > last_ts).to_table()
                    df = filtered.to_pandas()
                    
                    if not df.empty:
                        df = df.sort_values('inference_ts', ascending=True).head(config.BATCH_SIZE)
                        df['module_name'] = mod
                        raw_frames.append(df)
                        new_checkpoints[mod] = str(df['inference_ts'].max())
            except Exception:
                pass 
                
        if not raw_frames:
            time.sleep(config.POLL_INTERVAL)
            continue

        # 2. Chronological Timeline Reconstruction
        combined_df = pd.concat(raw_frames, ignore_index=True)
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        combined_df = combined_df.sort_values('timestamp', ascending=True)

        freq_str = f"{config.AGGREGATION_WINDOW_SEC}s"
        combined_df['window_ts'] = combined_df['timestamp'].dt.floor(freq_str)

        gold_records = []
        
        for (sim_id, window_ts), group in combined_df.groupby(['source_id', 'window_ts']):
            best_module_rows = group.sort_values('health_score', ascending=True).drop_duplicates(subset=['module_name'], keep='last')
            for _, row in group.iterrows():
                state.update_module_state(
                    sim_id=sim_id, 
                    module=row['module_name'], 
                    health=row['health_score'], 
                    features_json=row['top_features']
                )
            
            gold_row = aggregator.compute_gold_record(sim_id, str(window_ts))
            gold_records.append(gold_row)

        # 3. Write to Gold Delta Table
        if gold_records:
            gold_df = pd.DataFrame(gold_records)
            try:
                write_deltalake(config.GOLD_TABLE_DIR, gold_df, mode="append", schema_mode="merge")
                
                for mod, ts in new_checkpoints.items():
                    state.checkpoints[mod] = ts
                state.save_state()
                
                print(f"✅ Fused and wrote {len(gold_df)} Gold records.")
            except Exception as e:
                print(f"❌ Failed to write Gold table: {e}")

        time.sleep(config.POLL_INTERVAL)

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()