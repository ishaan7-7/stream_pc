import os
import json
import pickle
from src import config

class AlertStateManager:
    def __init__(self):
        self.checkpoints = self._load_json(config.CHECKPOINT_FILE, default={m: "1970-01-01T00:00:00" for m in config.ENABLED_MODULES})
        self.alert_cache = self._load_pkl(config.CACHE_FILE, default={})

    def _load_json(self, path, default):
        if os.path.exists(path) and os.path.getsize(path) > 0:
            try:
                with open(path, 'r') as f: 
                    return json.load(f)
            except json.JSONDecodeError:
                pass
        return default

    def _load_pkl(self, path, default):
        if os.path.exists(path):
            with open(path, 'rb') as f: 
                return pickle.load(f)
        return default

    def save_state(self):
        with open(config.CHECKPOINT_FILE, 'w') as f: json.dump(self.checkpoints, f, indent=4)
        with open(config.CACHE_FILE, 'wb') as f: pickle.dump(self.alert_cache, f)

    def get_state(self, sim_id, module):
        key = f"{sim_id}_{module}"
        if key not in self.alert_cache:
            self.alert_cache[key] = {
                "phase": "IDLE", 
                "fault_score": 0, 
                "alert_id": None, 
                "start_ts": None, 
                "accumulated_features": {},
                "max_score": 0.0, 
                "peak_ts": None
            }
        return self.alert_cache[key], key

    def update_state(self, key, state_dict):
        self.alert_cache[key] = state_dict