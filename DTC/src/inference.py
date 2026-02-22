import torch
import joblib
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Path Handling
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from DTC.src import config
from DTC.src.model_factory import DTC_Network

class DTCInferenceService:
    """
    Loads trained artifacts and performs inference on new data.
    """
    def __init__(self, module_name):
        self.module_name = module_name
        self.models = {}  # {dtc_code: model_obj}
        self.scalers = {} # {dtc_code: scaler_obj}
        self.configs = {} # {dtc_code: {features: [], severity: ''}}
        
        self._load_artifacts()

    def _load_artifacts(self):
        """
        Walks through DTC/artifacts/{module}/ and loads every model found.
        """
        try:
            master = config.load_dtc_master()
            dtc_list = master['modules'][self.module_name]
        except Exception as e:
            print(f"Error loading master contract: {e}")
            return

        print(f"[{self.module_name.upper()}] Loading Artifacts...")
        
        loaded_count = 0
        for dtc_entry in dtc_list:
            dtc_code = dtc_entry['dtc_code']
            
            _, artifact_dir = config.ensure_dirs(self.module_name, dtc_code)
            model_path = artifact_dir / "model.pt"
            scaler_path = artifact_dir / "scaler.pkl"
            
            if model_path.exists() and scaler_path.exists():
                self.configs[dtc_code] = {
                    'features': dtc_entry['features'],
                    'severity': dtc_entry['severity'],
                    'description': dtc_entry['description']
                }
                self.scalers[dtc_code] = joblib.load(scaler_path)
                
                input_dim = len(dtc_entry['features'])
                model = DTC_Network(input_dim)
                model.load_state_dict(torch.load(model_path))
                model.eval()
                self.models[dtc_code] = model
                loaded_count += 1
            else:
                pass 

        print(f"   -> Loaded {loaded_count} models successfully.")

    def analyze_window(self, df):
        """
        Runs all loaded models on the provided Dataframe.
        Returns: A dictionary of results (Critical vs Non-Critical DataFrames)
        """
        results = {
            'critical': pd.DataFrame(index=df.index),
            'non_critical': pd.DataFrame(index=df.index)
        }
        
        if 'timestamp' in df.columns:
            results['critical']['timestamp'] = df['timestamp']
            results['non_critical']['timestamp'] = df['timestamp']

        for dtc_code, model in self.models.items():
            dtc_config = self.configs[dtc_code]
            features = dtc_config['features']
            severity = dtc_config['severity']
            
            missing_cols = [f for f in features if f not in df.columns]
            if missing_cols:
                continue
                
            X_raw = df[features].values
            scaler = self.scalers[dtc_code]
            X_scaled = scaler.transform(X_raw)
            
            with torch.no_grad():
                X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
                probs = model(X_tensor).numpy().flatten()
            
            bucket = 'critical' if severity == 'critical' else 'non_critical'
            results[bucket][dtc_code] = probs
            
        return results

# --- NEW ACCUMULATION LOGIC ---
def accumulate_risk(df_results, noise_floor=0.15, sensitivity=0.001):
    """
    Integrates probability over time to simulate 'buildup'.
    
    Args:
        df_results: Raw probability DataFrame.
        noise_floor: Probabilities below this are IGNORED (subtracted).
        sensitivity: How fast the bucket fills (Multiplier).
                     Since your data is high-freq (~100Hz), a small value like 0.001 
                     means it takes ~20-30 seconds of solid faults to reach 100%.
    """
    df_processed = df_results.copy()
    
    for col in df_processed.columns:
        if col == 'timestamp': continue
        
        # 1. Calculate Effective Evidence (Input - Noise Floor)
        # If prob is 0.10 and floor is 0.15, result is -0.05 -> clipped to 0.0.
        # This completely silences the low-level noise.
        evidence = (df_processed[col] - noise_floor).clip(lower=0.0)
        
        # 2. Accumulate (Integration)
        # cumulative_sum() adds up the evidence row by row.
        # We multiply by sensitivity to control the slope.
        buildup = evidence.cumsum() * sensitivity
        
        # 3. Clamp at 1.0 (100% Triggered)
        # Once it hits the ceiling, it stays there.
        buildup = buildup.clip(upper=1.0)
        
        df_processed[col] = buildup
        
    return df_processed