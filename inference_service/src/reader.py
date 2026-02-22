
import os
import pandas as pd
from deltalake import DeltaTable
from src import config

class BronzeReader:
    def __init__(self, state_manager):
        self.state = state_manager

    def get_new_data(self, module):
        path = os.path.join(config.BRONZE_DIR, module)
        if not os.path.exists(path):
            return pd.DataFrame()
        
        try:
            # 1. Load the Delta Table
            dt = DeltaTable(path)
            
            # 2. Convert to Pandas FIRST to avoid PyArrow strict type-casting crashes
            df = dt.to_pandas()
            
            if df.empty:
                return df
                
            # 3. Find the lowest watermark for this module
            checkpoints = [v for k, v in self.state.checkpoints.items() if k.endswith(f"_{module}")]
            
            if checkpoints:
                min_watermark = min(checkpoints)
                # Safely filter using Pandas which handles type coercion (str vs timestamp) much better
                # We convert both sides to string just for the comparison to be absolutely safe
                df = df[df['ingest_ts'].astype(str) > str(min_watermark)]
            
            if df.empty:
                return df
                
            # 4. Sort strictly chronologically so ML sequences build correctly
            if 'ingest_ts' in df.columns:
                df = df.sort_values('ingest_ts', ascending=True)
                
            return df
            
        except Exception as e:
            # THIS WILL NOW SHOW US EXACTLY WHY IT IS FAILING
            print(f"❌ [READER ERROR] Failed to read Bronze table for {module}: {repr(e)}")
            return pd.DataFrame()
