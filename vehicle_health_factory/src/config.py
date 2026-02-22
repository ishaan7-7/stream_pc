import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
ARTIFACTS_DIR = os.path.join(BASE_DIR, "artifacts")

# Hard requirement: Must resolve to the streaming emulator contract
MASTER_JSON_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "contracts", "master.json"))

MODULES = ["engine", "battery", "body", "transmission", "tyre"]

# Model Hyperparameters (Optimized for deep CPU training with high regularization)
SEQ_LENGTH = 30
BATCH_SIZE = 128
EPOCHS = 100
PATIENCE = 10
LEARNING_RATE = 1e-3
HIDDEN_DIM = 64
NUM_LAYERS = 2
DROPOUT = 0.2