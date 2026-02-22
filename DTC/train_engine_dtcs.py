import sys
import argparse
from pathlib import Path
from tqdm import tqdm # Progress bar

# --- Setup Python Path ---
# Logic: Ensure we can import from the root project directory
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from DTC.src import config
from DTC.src.fault_injector import FaultInjector
from DTC.src.model_factory import ModelFactory

def train_module(module_name="engine", sim_id="sim001", n_samples=5000, epochs=100):
    """
    Batch trains all DTC models for a specific module.
    """
    print(f"==================================================")
    print(f"   DTC TRAINING ORCHESTRATOR: {module_name.upper()}")
    print(f"   Source Sim: {sim_id} | Samples: {n_samples} | Epochs: {epochs}")
    print(f"==================================================\n")

    # 1. Load Master Contract
    try:
        master_data = config.load_dtc_master()
        if module_name not in master_data['modules']:
            print(f"ERROR: Module '{module_name}' not found in DTC_master.json")
            return
        dtc_list = master_data['modules'][module_name]
    except Exception as e:
        print(f"CRITICAL ERROR loading master contract: {e}")
        return

    # 2. Initialize Components
    try:
        injector = FaultInjector(module_name, sim_id)
        factory = ModelFactory()
    except Exception as e:
        print(f"CRITICAL ERROR initializing components: {e}")
        return

    # 3. Batch Loop
    success_count = 0
    fail_count = 0
    
    print(f"-> Found {len(dtc_list)} DTCs to process.\n")

    # Progress bar for visual feedback
    pbar = tqdm(dtc_list, desc="Training DTCs", unit="model")
    
    for dtc_entry in pbar:
        dtc_code = dtc_entry['dtc_code']
        features = dtc_entry['features']
        
        pbar.set_postfix_str(f"Processing {dtc_code}")
        
        try:
            # A. Generate Data (The Physics)
            # Create synthetic dataset (50% healthy, 50% faulty)
            df_train = injector.create_dataset(dtc_code, n_samples=n_samples)
            
            # Save synthetic data for inspection (optional but good for debugging)
            injector.save_synthetic_data(df_train, dtc_code)
            
            # B. Train Model (The Brain)
            # Train the neural network
            # Note: We silence the per-epoch verbose output to keep the console clean
            model, scaler = factory.train(df_train, features, epochs=epochs, verbose=False)
            
            # C. Save Artifacts
            save_path = factory.save_artifacts(model, scaler, module_name, dtc_code)
            
            success_count += 1
            
        except Exception as e:
            # If a specific DTC fails (e.g. missing column), log it and move on
            tqdm.write(f"\n[ERROR] Failed {dtc_code}: {e}")
            fail_count += 1
            continue

    # 4. Final Summary
    print(f"\n==================================================")
    print(f"   TRAINING COMPLETE")
    print(f"   Success: {success_count}")
    print(f"   Failed:  {fail_count}")
    print(f"   Artifacts Location: {config.ARTIFACTS_DIR / module_name}")
    print(f"==================================================")

if __name__ == "__main__":
    # Simple CLI argument parsing
    parser = argparse.ArgumentParser(description='Train DTC Models')
    parser.add_argument('--module', type=str, default='engine', help='Module to train (default: engine)')
    parser.add_argument('--sim', type=str, default='sim001', help='Source Simulation ID (default: sim001)')
    parser.add_argument('--epochs', type=int, default=50, help='Training epochs (default: 50)')
    
    args = parser.parse_args()
    
    train_module(module_name=args.module, sim_id=args.sim, epochs=args.epochs)