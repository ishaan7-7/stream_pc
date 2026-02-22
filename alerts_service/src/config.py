import os

ALERTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(ALERTS_DIR)

BRONZE_DIR = os.path.join(ROOT_DIR, "data", "delta", "bronze") # <-- ADDED THIS
SILVER_DIR = os.path.join(ROOT_DIR, "data", "delta", "silver")
GOLD_ALERTS_DIR = os.path.join(ROOT_DIR, "data", "delta", "gold", "alerts")
STATE_DIR = os.path.join(ALERTS_DIR, "state")

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(os.path.join(ROOT_DIR, "data", "delta", "gold"), exist_ok=True)

CHECKPOINT_FILE = os.path.join(STATE_DIR, "checkpoints.json")
CACHE_FILE = os.path.join(STATE_DIR, "alert_state_cache.pkl")

POLL_INTERVAL = 2.0               
BATCH_SIZE = 500              
ENABLED_MODULES = ["engine", "battery", "transmission", "body", "tyre"]

# --- LEAKY BUCKET STATE MACHINE RULES ---
MAX_FAULT_SCORE = 100
MIN_FAULT_SCORE = 0

SCORE_DELTAS = {
    "CRITICAL": 20,
    "WARNING": 5,
    "NORMAL": -10
}

# ==========================================================
# --- 🔬 DTC DEEP DIVE CONFIGURATION ---
# ==========================================================
DTC_MASTER_JSON = os.path.join(ROOT_DIR, "contracts", "DTC_master.json")
DTC_ARTIFACTS_DIR = os.path.join(ROOT_DIR, "DTC", "artifacts")
DTC_LOOKBACK_ROWS = 600       
DTC_NOISE_FLOOR = 0.3       
DTC_SENSITIVITY = 0.0002
