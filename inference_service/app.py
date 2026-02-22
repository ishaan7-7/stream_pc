import time
import os
import sys
from src.state_manager import StateManager
from src.ml_engine import MLEngine
from src.reader import BronzeReader
from src.writer import SilverWriter
from src import config

def main():
    if len(sys.argv) < 2:
        print(f"Usage: python app.py <module_name>")
        sys.exit(1)
        
    module = sys.argv[1].lower()
    if module not in config.MODULES:
        print(f"Invalid module. Choose from {config.MODULES}")
        sys.exit(1)

    print("=====================================================")
    print(f"🚀 STARTING REAL-TIME INFERENCE: [{module.upper()}]")
    print("=====================================================")
    
    state = StateManager(module)
    ml = MLEngine(state, module)
    reader = BronzeReader(state)
    writer = SilverWriter()
    
    print(f"📡 Polling Bronze Delta every {config.POLL_INTERVAL} seconds...")
    
    while True:
        processed_any = False
        
        df_new = reader.get_new_data(module)
        if df_new.empty:
            time.sleep(config.POLL_INTERVAL)
            continue
        
        if 'source_id' not in df_new.columns or 'ingest_ts' not in df_new.columns:
            print(f"⚠️ [SCHEMA WARN] {module.upper()} missing columns. Found: {df_new.columns.tolist()}")
            time.sleep(config.POLL_INTERVAL)
            continue

        active_sims = df_new['source_id'].unique()
        
        for sim_id in active_sims:
            sim_df = df_new[df_new['source_id'] == sim_id]
            last_ts = state.get_last_timestamp(sim_id)
            sim_df = sim_df[sim_df['ingest_ts'].astype(str) > str(last_ts)]
            
            if sim_df.empty: continue
            sim_df = sim_df.head(config.BATCH_SIZE)
            
            try:
                out_df = ml.process_batch(sim_df.copy(), sim_id)
                if not out_df.empty:
                    writer.write(out_df, module)
                    max_ingest_ts = str(sim_df['ingest_ts'].max())
                    state.update_checkpoint(sim_id, max_ingest_ts)
                    
                    processed_any = True
                    print(f"✅ [{module.upper():<12}] {sim_id}: Inferred {len(out_df)} rows")
            except Exception as e:
                print(f"⚠️ Warning: Skipped batch for {sim_id} {module} due to Error: {e}")

        if not processed_any:
            time.sleep(config.POLL_INTERVAL)

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()