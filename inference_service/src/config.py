
import os

# Base paths
INFERENCE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(INFERENCE_DIR)

# Data paths (Delta Tables)
BRONZE_DIR = os.path.join(ROOT_DIR, "data", "delta", "bronze")
SILVER_DIR = os.path.join(ROOT_DIR, "data", "delta", "silver")

# Inference Paths
ARTIFACTS_DIR = os.path.join(INFERENCE_DIR, "artifacts")
STATE_DIR = os.path.join(INFERENCE_DIR, "state")

os.makedirs(STATE_DIR, exist_ok=True)

# State Files
CHECKPOINT_FILE = os.path.join(STATE_DIR, "checkpoints.json")
ML_STATE_FILE = os.path.join(STATE_DIR, "ml_state.pkl")
ALERTS_FILE = os.path.join(STATE_DIR, "system_alerts.json")

# ML & Streaming Config
MODULES = ["engine", "battery", "body", "transmission", "tyre"]
BATCH_SIZE = 60       # Maximum rows to read per module per sim at once
POLL_INTERVAL = 2.0    # Seconds to wait if no new data in bronze
EMA_ALPHA = 0.2
PERSISTENCE_LIMIT = 5
