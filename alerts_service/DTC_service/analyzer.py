import os
import sys
import json
import pandas as pd
from pathlib import Path
from deltalake import DeltaTable
import pyarrow.compute as pc

# Ensure we can import the existing DTC pipeline from the root project
current_file = Path(__file__).resolve()
root_dir = current_file.parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

from DTC.src.inference import DTCInferenceService
from alerts_service.src import config as alert_config

class DTCAdapter:
    def __init__(self, module_name):
        self.module_name = module_name
        self.dtc_service = DTCInferenceService(module_name)
        with open(alert_config.DTC_MASTER_JSON, 'r') as f:
            master = json.load(f)
            self.msg_map = {d['dtc_code']: d['dashboard_message'] for d in master['modules'].get(module_name, [])}

    def fetch_traceback(self, source_id, peak_ts):
        """Fetches BRONZE data strictly ending at peak_ts, looking back N rows."""
        dt_path = os.path.join(alert_config.BRONZE_DIR, self.module_name)
        if not os.path.exists(dt_path): return pd.DataFrame()
        
        try:
            dataset = DeltaTable(dt_path).to_pyarrow_dataset()
            df = dataset.scanner(filter=(pc.field("source_id") == source_id)).to_table().to_pandas()
            if df.empty: return df
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            peak_ts_pd = pd.to_datetime(peak_ts)
            
            df = df[df['timestamp'] <= peak_ts_pd]
            df = df.sort_values('timestamp', ascending=True).tail(alert_config.DTC_LOOKBACK_ROWS).reset_index(drop=True)
            
            # Clean the raw Bronze data so PyTorch doesn't crash on NaNs
            df = df.ffill().fillna(0.0)
            
            return df
        except Exception as e:
            print(f"Error fetching traceback from Bronze: {e}")
            return pd.DataFrame()

    def _smart_attribution(self, raw_crit, raw_noncrit):
        """
        Applies Exponential Magnification to separate the root cause from the noise,
        then strictly scales the top anomaly to exactly 1.0.
        """
        cols_crit = [c for c in raw_crit.columns if c != 'timestamp']
        cols_noncrit = [c for c in raw_noncrit.columns if c != 'timestamp']
        
        raw_buildups = {}
        
        # 1. Dynamic Noise Floor & Exponential Magnification (Squaring)
        for col in cols_crit:
            floor = raw_crit[col].median()
            evidence = (raw_crit[col] - floor).clip(lower=0.0)
            evidence = evidence ** 2  # Squaring separates the true root cause from noise
            raw_buildups[col] = evidence.cumsum()
            
        for col in cols_noncrit:
            floor = raw_noncrit[col].median()
            evidence = (raw_noncrit[col] - floor).clip(lower=0.0)
            evidence = evidence ** 2 
            raw_buildups[col] = evidence.cumsum()
            
        # 2. Find the absolute global maximum (The Winner of the Race)
        if raw_buildups:
            global_max = max(series.max() for series in raw_buildups.values())
        else:
            global_max = 0.0
            
        # 3. Strict Scaling (Force the absolute worst anomaly to exactly 1.0)
        if global_max > 1e-6:
            scale_factor = 1.0 / global_max
        else:
            scale_factor = 0.0
        
        # 4. Rebuild the final DataFrames
        df_crit = pd.DataFrame(index=raw_crit.index)
        if 'timestamp' in raw_crit.columns: df_crit['timestamp'] = raw_crit['timestamp']
        for col in cols_crit:
            df_crit[col] = (raw_buildups[col] * scale_factor).clip(upper=1.0)
            
        df_noncrit = pd.DataFrame(index=raw_noncrit.index)
        if 'timestamp' in raw_noncrit.columns: df_noncrit['timestamp'] = raw_noncrit['timestamp']
        for col in cols_noncrit:
            df_noncrit[col] = (raw_buildups[col] * scale_factor).clip(upper=1.0)
            
        return df_crit, df_noncrit

    def run_diagnosis(self, bronze_df):
        """Runs raw inference, applies Smart Attribution, and extracts triggers."""
        diagnostics = {"models_loaded": len(self.dtc_service.models), "skipped_dtcs": {}}
        
        # Schema Diagnostics
        for dtc_code, model in self.dtc_service.models.items():
            features = self.dtc_service.configs[dtc_code]['features']
            missing_cols = [f for f in features if f not in bronze_df.columns]
            if missing_cols: diagnostics["skipped_dtcs"][dtc_code] = missing_cols

        # 1. Raw PyTorch Inference
        raw_results = self.dtc_service.analyze_window(bronze_df)
        
        # 2. Smart Attribution Integration (Fixed Function Call)
        df_crit_buildup, df_noncrit_buildup = self._smart_attribution(
            raw_results['critical'], 
            raw_results['non_critical']
        )

        # 3. Message Extraction
        triggered_alerts = []
        for col in df_crit_buildup.columns:
            if col != 'timestamp' and df_crit_buildup[col].max() >= 0.99:
                triggered_alerts.append({"code": col, "severity": "CRITICAL", "message": self.msg_map.get(col, "Unknown Critical Error")})
                
        for col in df_noncrit_buildup.columns:
            if col != 'timestamp' and df_noncrit_buildup[col].max() >= 0.99:
                triggered_alerts.append({"code": col, "severity": "WARNING", "message": self.msg_map.get(col, "Unknown Warning")})

        return df_crit_buildup, df_noncrit_buildup, triggered_alerts, diagnostics