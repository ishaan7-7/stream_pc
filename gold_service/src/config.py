import os

# --- BASE PATHS ---
GOLD_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(GOLD_DIR)

SILVER_DIR = os.path.join(ROOT_DIR, "data", "delta", "silver")
GOLD_TABLE_DIR = os.path.join(ROOT_DIR, "data", "delta", "gold", "vehicle_health")
STATE_DIR = os.path.join(GOLD_DIR, "state")

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(os.path.join(ROOT_DIR, "data", "delta", "gold"), exist_ok=True)

CHECKPOINT_FILE = os.path.join(STATE_DIR, "checkpoints.json")
CACHE_FILE = os.path.join(STATE_DIR, "vehicle_cache.pkl")

# --- PROCESSING PARAMETERS ---
POLL_INTERVAL = 2.0               
BATCH_SIZE = 50               
AGGREGATION_WINDOW_SEC = 300      

# --- DYNAMIC MODULE CONFIGURATION ---
# 1. Define exactly which modules are active for this pipeline
ENABLED_MODULES = ["engine", "transmission"] # "engine", "battery", "transmission", "body", "tyre"

# 2. Define the raw hierarchical weights (They do not need to sum to 1.0)
RAW_WEIGHTS = {
    "engine": 0.80,
#   "battery": 0.20,
   "transmission": 0.20,
#   "body": 0.10,
#   "tyre": 0.10
}

# 3. Define Tier 1 Penalties (Module Name -> Critical Threshold Health Score)
TIER_1_PENALTIES = {
    "engine": 20.0,
}

# --- SMART AUTO-NORMALIZATION ENGINE ---
# Filter weights to only include enabled modules
_active_weights = {m: RAW_WEIGHTS.get(m, 0.0) for m in ENABLED_MODULES}
_total_weight = sum(_active_weights.values())

if _total_weight <= 0:
    raise ValueError("Sum of enabled module weights must be greater than 0.")

# Normalize weights so they perfectly equal 1.0 while preserving preference ratios
NORMALIZED_WEIGHTS = {m: (w / _total_weight) for m, w in _active_weights.items()}

# Print startup calibration for logging
print(f"⚙️ Configured Weights Normalized to 1.0: { {k: round(v, 4) for k, v in NORMALIZED_WEIGHTS.items()} }")