import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path

# Add project root to path to ensure relative imports work if run directly
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from DTC.src import config

class FaultInjector:
    """
    The 'Virtual Mechanic' class.
    Responsibility: Take healthy data and inject mathematical faults 
    to create a labelled training dataset (0.0 = Healthy, 1.0 = Critical).
    """

    def __init__(self, module_name, sim_id="sim001"):
        """
        Initialize with the specific module (e.g., 'engine') and source simulation.
        """
        self.module_name = module_name
        self.sim_id = sim_id
        
        # Load the healthy reference data immediately upon initialization
        self.raw_data = self._load_raw_data()

    def _load_raw_data(self):
        """
        Locates and loads the specific CSV for this module from the data directory.
        Returns: DataFrame of healthy data.
        """
        # Construct path: data/vehicles/sim001/synthetic_{module}_*.csv
        # We use a wildcard * because the scenario name might vary (scenarioA, etc.)
        search_pattern = config.DATA_DIR / self.sim_id / f"synthetic_{self.module_name}_*.csv"
        files = glob.glob(str(search_pattern))
        
        if not files:
            error_msg = f"CRITICAL: No data found for module '{self.module_name}' in {search_pattern}"
            print(error_msg)
            raise FileNotFoundError(error_msg)
            
        # We take the first matching file found
        target_file = files[0]
        print(f"[{self.module_name.upper()}] Loading reference data source: {os.path.basename(target_file)}")
        
        try:
            df = pd.read_csv(target_file)
            return df
        except Exception as e:
            raise IOError(f"Failed to read CSV file: {e}")

    def create_dataset(self, dtc_code, n_samples=5000):
        """
        Generates a balanced dataset for Training.
        Structure:
          - 50% Healthy Rows (Label = 0.0)
          - 50% Faulty Rows (Label = 0.1 to 1.0, representing buildup)
        """
        # 1. Retrieve the 'Menu' of features for this specific DTC
        try:
            dtc_config = config.get_dtc_config(self.module_name, dtc_code)
            features = dtc_config['features']
        except Exception as e:
            print(f"Error fetching config for {dtc_code}: {e}")
            raise

        print(f"   -> Generating {n_samples} samples for {dtc_code} using features: {features}")

        # 2. Extract only the relevant columns from raw data
        # Check if all features exist in the csv
        missing_cols = [f for f in features if f not in self.raw_data.columns]
        if missing_cols:
            raise KeyError(f"The following features defined in DTC_master.json are missing from the CSV: {missing_cols}")
            
        subset_df = self.raw_data[features].copy()
        
        # 3. Create Healthy Partition (Label = 0.0)
        # Randomly sample existing rows
        n_healthy = n_samples // 2
        healthy_df = subset_df.sample(n=n_healthy, replace=True).copy()
        healthy_df['target'] = 0.0
        
        # 4. Create Faulty Partition (Label = Buildup)
        n_faulty = n_samples - n_healthy
        fault_base_df = subset_df.sample(n=n_faulty, replace=True).copy()
        
        # Apply the specific "Physics" logic
        faulty_df = self._apply_physics_logic(fault_base_df, dtc_code, features)
        
        # 5. Combine and Shuffle
        final_df = pd.concat([healthy_df, faulty_df], axis=0).sample(frac=1).reset_index(drop=True)
        
        return final_df

    def _apply_physics_logic(self, df, dtc_code, features):
        """
        The Core Logic Layer. 
        Applies mathematical transformations to simulate specific failures.
        """
        df = df.copy()
        n = len(df)
        
        # Create the Buildup Curve (Linearly increasing severity from 0.1 to 1.0)
        # This teaches the model to recognize "worsening" conditions.
        severity = np.linspace(0.1, 1.0, n)
        df['target'] = severity
        
        # --- STRATEGY 1: THERMAL OVERHEAT (Gain) ---
        # Used for: Coolant Temp (P0217), Trans Temp (P0218), Battery Temp (P0A1F), Tire Temp (C0514)
        if dtc_code in ['P0217', 'P0128', 'P0218', 'P0A1F', 'C0514', 'P0113', 'B1081']:
            # Primary feature (index 0) gets multiplied
            # Result: Temp rises 20% to 50% above normal
            col = features[0]
            gain = 1.0 + (0.5 * severity) 
            df[col] = df[col] * gain

        # --- STRATEGY 2: PRESSURE DROP / LEAN / UNDERBOOST (Attenuation) ---
        # Used for: Low Oil Pressure, System Lean (P0171), Low Tire Pressure (C0077)
        elif dtc_code in ['P0171', 'P0524', 'P0299', 'P0562', 'C0077', 'P0868', 'P0A09', 'P0534']:
            # Primary feature drops significantly
            col = features[0]
            # Result: Value drops to 40% of normal at max severity
            attenuation = 1.0 - (0.6 * severity)
            df[col] = df[col] * attenuation

        # --- STRATEGY 3: DRIFT / EFFICIENCY LOSS (Offset) ---
        # Used for: Catalyst (P0420), Gear Ratio (P0730), Battery SOH (P0A80)
        elif dtc_code in ['P0420', 'P0730', 'P0A80', 'C0078', 'P0461']:
            # Introduce a divergence between two related metrics
            # E.g. for P0420, we drift the sensor value so it no longer tracks the expected baseline
            col = features[0]
            # Add additive noise/drift
            drift = (df[col].mean() * 0.25) * severity 
            # We add random sign to simulate drift up OR down
            df[col] = df[col] + drift

        # --- STRATEGY 4: MISFIRE / STABILITY (Noise Injection) ---
        # Used for: Misfire (P0300), Wheel Speed (C0031), Yaw Rate (C0063)
        elif dtc_code in ['P0300', 'P0101', 'C0031', 'C0063', 'C1234', 'B1000']:
            # Inject chaotic noise into the first two features
            for col in features[:2]:
                # Noise magnitude is 50% of the mean value at max severity
                noise_mag = df[col].mean() * 0.5
                noise = np.random.normal(0, 1, n) * noise_mag * severity
                df[col] = df[col] + noise
                
        # --- STRATEGY 5: DEFAULT FALLBACK ---
        else:
            # If code not explicitly mapped, use generic noise injection on first feature
            col = features[0]
            df[col] = df[col] * (1.0 + (0.3 * severity))

        return df

    def save_synthetic_data(self, df, dtc_code):
        """
        Saves the generated dataset to the temporary synth_data folder.
        """
        # Ensure directory exists using config utility
        save_dir, _ = config.ensure_dirs(self.module_name)
        
        file_path = save_dir / f"synthetic_{dtc_code}.csv"
        df.to_csv(file_path, index=False)
        print(f"   -> Saved synthetic dataset: {file_path} (Rows: {len(df)})")
        return file_path