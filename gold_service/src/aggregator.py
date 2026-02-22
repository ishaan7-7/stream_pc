
import json
from datetime import datetime
from src import config

class HealthAggregator:
    def __init__(self, state_manager):
        self.state = state_manager

    def compute_gold_record(self, sim_id, timestamp):
        cache = self.state.get_vehicle_state(sim_id)
        
        total_health = 0.0
        feature_pool = []
        contribs = {}

        # 1. Base Normalized Weighted Sum
        for mod, weight in config.NORMALIZED_WEIGHTS.items():
            # If a module hasn't reported yet, default to 100.0 health
            mod_data = cache.get(mod, {"health": 100.0, "feats": "{}"})
            h_score = mod_data["health"]
            
            total_health += (h_score * weight)
            contribs[f"{mod}_contrib"] = round(h_score, 2)
            
            try:
                feats = json.loads(mod_data["feats"])
                for f_name, f_val in feats.items():
                    # Normalize feature impact by the smart module weight
                    feature_pool.append({"feature": f_name, "impact": f_val * weight})
            except:
                pass

        # 2. Dynamic Tier 1 Penalties
        for penalty_mod, threshold in config.TIER_1_PENALTIES.items():
            contrib_key = f"{penalty_mod}_contrib"
            if contrib_key in contribs:
                mod_health = contribs[contrib_key]
                if mod_health < threshold:
                    # Clamp the vehicle health to the worst failing critical module
                    total_health = min(total_health, mod_health)

        # 3. Top 5 Features Extraction
        feature_pool.sort(key=lambda x: x["impact"], reverse=True)
        top_5 = {f["feature"]: round(f["impact"], 4) for f in feature_pool[:5]}

        return {
            "source_id": sim_id,
            "gold_window_ts": timestamp,
            "vehicle_health_score": round(total_health, 2),
            **contribs,
            "top_5_features": json.dumps(top_5),
            "gold_write_ts": datetime.utcnow().isoformat()
        }
