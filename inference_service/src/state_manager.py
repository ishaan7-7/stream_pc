import os
import json
import pickle
from datetime import datetime
from src import config

class StateManager:
    def __init__(self, module_name):
        self.module = module_name
        self.checkpoint_file = os.path.join(config.STATE_DIR, f"checkpoints_{self.module}.json")
        self.ml_state_file = os.path.join(config.STATE_DIR, f"ml_state_{self.module}.pkl")
        self.alerts_file = os.path.join(config.STATE_DIR, f"system_alerts_{self.module}.json")
        
        self.checkpoints = self._load_json(self.checkpoint_file, default={})
        self.ml_state = self._load_pkl(self.ml_state_file, default={})
        self.alerts = self._load_json(self.alerts_file, default=[])

    def _load_json(self, path, default):
        if os.path.exists(path):
            with open(path, 'r') as f:
                try: return json.load(f)
                except: return default
        return default

    def _load_pkl(self, path, default):
        if os.path.exists(path):
            with open(path, 'rb') as f:
                try: return pickle.load(f)
                except: return default
        return default

    def _save_json(self, data, path):
        with open(path, 'w') as f: json.dump(data, f, indent=4)

    def _save_pkl(self, data, path):
        with open(path, 'wb') as f: pickle.dump(data, f)

    def get_last_timestamp(self, sim_id):
        key = f"{sim_id}_{self.module}"
        return self.checkpoints.get(key, "1970-01-01T00:00:00.000Z")

    def update_checkpoint(self, sim_id, timestamp_str):
        key = f"{sim_id}_{self.module}"
        self.checkpoints[key] = timestamp_str
        self._save_json(self.checkpoints, self.checkpoint_file)

    def get_ml_state(self, sim_id):
        key = f"{sim_id}_{self.module}"
        if key not in self.ml_state:
            self.ml_state[key] = {"ema_error": 0.0, "persistence_counter": 0, "last_window_data": None}
        return self.ml_state[key]

    def update_ml_state(self, sim_id, ema_error, persistence_counter, last_window_data):
        key = f"{sim_id}_{self.module}"
        self.ml_state[key] = {
            "ema_error": float(ema_error), 
            "persistence_counter": int(persistence_counter), 
            "last_window_data": last_window_data
        }
        self._save_pkl(self.ml_state, self.ml_state_file)

    def log_alert(self, sim_id, level, message):
        alert = {
            "timestamp": datetime.utcnow().isoformat(),
            "sim_id": sim_id,
            "module": self.module,
            "level": level,
            "message": message
        }
        self.alerts.append(alert)
        self.alerts = self.alerts[-50:]
        self._save_json(self.alerts, self.alerts_file)