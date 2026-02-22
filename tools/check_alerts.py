import os
import pandas as pd
from deltalake import DeltaTable
from pathlib import Path

SCORE_DELTAS = {"CRITICAL": 20, "WARNING": 5, "NORMAL": -10}
MAX_FAULT_SCORE = 100
MIN_FAULT_SCORE = 0
MODULES = ["engine", "battery", "transmission", "body", "tyre"]

def analyze_alerts():
    print("==========================================================")
    print("🔍 STATIC ALERT VERIFICATION TOOL (LEAKY BUCKET ANALYSIS)")
    print("==========================================================")
    
    root_dir = Path(__file__).resolve().parent.parent
    silver_dir = root_dir / "data" / "delta" / "silver"
    
    matrix_data = {}
    found_any_data = False

    for mod in MODULES:
        mod_path = silver_dir / mod
        if not mod_path.exists():
            print(f"⚠️ Silver table '{mod}' not found. Skipping.")
            continue
            
        print(f"⏳ Analyzing '{mod}'...")
        try:
            dt = DeltaTable(str(mod_path))
            
            # FIXED: Added .to_table() before .to_pandas()
            df = dt.to_pyarrow_dataset().scanner(columns=['source_id', 'timestamp', 'severity']).to_table().to_pandas()
            
            if df.empty:
                continue
                
            found_any_data = True
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by=['source_id', 'timestamp'])
            
            for sim_id, group in df.groupby('source_id'):
                if sim_id not in matrix_data:
                    matrix_data[sim_id] = {m: 0 for m in MODULES}
                    
                fault_score = 0
                is_active = False
                alert_count = 0
                
                for severity in group['severity']:
                    delta = SCORE_DELTAS.get(severity, 0)
                    fault_score += delta
                    fault_score = max(MIN_FAULT_SCORE, min(MAX_FAULT_SCORE, fault_score))
                    
                    if not is_active and fault_score >= MAX_FAULT_SCORE:
                        alert_count += 1
                        is_active = True
                    elif is_active and fault_score <= MIN_FAULT_SCORE:
                        is_active = False
                        
                matrix_data[sim_id][mod] = alert_count
                
        except Exception as e:
            print(f"❌ Error processing '{mod}': {e}")

    if not found_any_data or not matrix_data:
        print("\n❌ No data found in Silver tables to analyze.")
        return

    print("\n📊 TRUE ALERTS FOUND IN SILVER DATA (Per Sim / Per Module):")
    
    matrix_df = pd.DataFrame.from_dict(matrix_data, orient='index')
    
    existing_cols = [m for m in MODULES if m in matrix_df.columns]
    matrix_df = matrix_df[existing_cols]
    
    matrix_df.loc['TOTAL_BY_MODULE'] = matrix_df.sum()
    
    print("-" * 75)
    print(matrix_df.to_string())
    print("-" * 75)
    print("✅ Analysis complete.")

if __name__ == "__main__":
    analyze_alerts()