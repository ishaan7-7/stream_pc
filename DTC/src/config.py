import os
import json
from pathlib import Path

# Resolve the project root dynamically
# Logic: This file is in .../DTC/src/, so we go up 3 levels to get to project root
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Define System Paths
DTC_ROOT = BASE_DIR / "DTC"
CONTRACTS_DIR = BASE_DIR / "contracts"
DATA_DIR = BASE_DIR / "data" / "vehicles"

# DTC Sub-directories
ARTIFACTS_DIR = DTC_ROOT / "artifacts"
SYNTH_DATA_DIR = DTC_ROOT / "synth_data"

# Source of Truth
DTC_MASTER_PATH = CONTRACTS_DIR / "DTC_master.json"

def load_dtc_master():
    if not DTC_MASTER_PATH.exists():
        raise FileNotFoundError(f"DTC Master contract missing at: {DTC_MASTER_PATH}")
    
    with open(DTC_MASTER_PATH, 'r') as f:
        return json.load(f)

def get_dtc_config(module_name, dtc_code):
    master = load_dtc_master()
    
    if "modules" not in master:
        raise ValueError("Invalid Master JSON: 'modules' key missing")
        
    if module_name not in master["modules"]:
        raise ValueError(f"Module '{module_name}' not defined in Master Contract")
    
    dtc_list = master["modules"][module_name]
    for dtc in dtc_list:
        if dtc['dtc_code'] == dtc_code:
            return dtc
            
    raise ValueError(f"DTC '{dtc_code}' not found in module '{module_name}'")

def ensure_dirs(module_name, dtc_code=None):
    # Ensure Synth Data Directory exists
    synth_path = SYNTH_DATA_DIR / module_name
    synth_path.mkdir(parents=True, exist_ok=True)
    
    artifact_path = None
    if dtc_code:
        # Ensure Artifact Directory exists (e.g., artifacts/engine/P0217)
        artifact_path = ARTIFACTS_DIR / module_name / dtc_code
        artifact_path.mkdir(parents=True, exist_ok=True)
        
    return synth_path, artifact_path