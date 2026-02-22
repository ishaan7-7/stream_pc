import os
import json
import pickle
from src import config

class GoldStateManager:
    def __init__(self):
        self.checkpoints = self._load_json(config.CHECKPOINT_FILE, default={m: "1970-01-01T00:00:00" for m in config.ENABLED_MODULES})
        self.vehicle_cache = self._load_pkl(config.CACHE_FILE, default={})

    def _load_json(self, path, default):
        if os.path.exists(path):
            with open(path, 'r') as f: return json.load(f)
        return default

    def _load_pkl(self, path, default):
        if os.path.exists(path):
            with open(path, 'rb') as f: return pickle.load(f)
        return default

    def save_state(self):
        with open(config.CHECKPOINT_FILE, 'w') as f: json.dump(self.checkpoints, f, indent=4)
        with open(config.CACHE_FILE, 'wb') as f: pickle.dump(self.vehicle_cache, f)

    def get_vehicle_state(self, sim_id):
        if sim_id not in self.vehicle_cache:
            # Cold start initialization strictly for enabled modules
            self.vehicle_cache[sim_id] = {
                mod: {"health": 100.0, "feats": "{}"} for mod in config.ENABLED_MODULES
            }
        return self.vehicle_cache[sim_id]

    def update_module_state(self, sim_id, module, health, features_json):
        # Ignore data from disabled modules if they are still sitting in Silver tables
        if module not in config.ENABLED_MODULES:
            return
            
        state = self.get_vehicle_state(sim_id)
        state[module] = {"health": float(health), "feats": features_json}