import json
import uuid
from datetime import datetime
from src import config

class AlertEngine:
    def __init__(self, state_manager):
        self.state_mgr = state_manager

    def process_row(self, row):
        sim_id, module = row['source_id'], row['module_name']
        state, key = self.state_mgr.get_state(sim_id, module)
        
        severity = row['severity']
        comp_score = row['composite_score']
        timestamp = str(row['timestamp'])
        
        try:
            feats = json.loads(row['top_features'])
        except:
            feats = {}

        action_payload = None

        # 1. Math Update: Add/Subtract the score based on severity, clamp between 0 and 100
        delta = config.SCORE_DELTAS.get(severity, 0)
        state["fault_score"] += delta
        state["fault_score"] = max(config.MIN_FAULT_SCORE, min(config.MAX_FAULT_SCORE, state["fault_score"]))

        # 2. Accumulate features and track peak while the bucket has any anomalies in it
        if state["fault_score"] > 0:
            self._accumulate_features(state, feats)
            
            if state["phase"] == "IDLE" and state["start_ts"] is None:
                state["start_ts"] = timestamp
            
            self._check_peak(state, comp_score, timestamp)

        # 3. State Machine Transitions
        if state["phase"] == "IDLE":
            if state["fault_score"] >= config.MAX_FAULT_SCORE:
                state["phase"] = "ACTIVE"
                state["alert_id"] = str(uuid.uuid4())
                action_payload = self._build_payload(sim_id, module, state, "OPEN")
            elif state["fault_score"] == 0:
                # Anomaly leaked out without triggering. Wipe tracking.
                self._reset_tracking(state)

        elif state["phase"] == "ACTIVE":
            if state["fault_score"] <= config.MIN_FAULT_SCORE:
                action_payload = self._build_payload(sim_id, module, state, "CLOSED", end_ts=timestamp)
                state["phase"] = "IDLE"
                self._reset_tracking(state)
            else:
                action_payload = self._build_payload(sim_id, module, state, "OPEN")

        self.state_mgr.update_state(key, state)
        return action_payload

    def _accumulate_features(self, state, new_feats):
        for f, val in new_feats.items():
            state["accumulated_features"][f] = state["accumulated_features"].get(f, 0.0) + val

    def _check_peak(self, state, comp_score, timestamp):
        if comp_score > state["max_score"]:
            state["max_score"] = comp_score
            state["peak_ts"] = timestamp

    def _reset_tracking(self, state):
        state["start_ts"] = None
        state["accumulated_features"] = {}
        state["max_score"] = 0.0
        state["peak_ts"] = None
        state["alert_id"] = None

    def _build_payload(self, sim_id, module, state, status, end_ts=None):
        sorted_feats = sorted(state["accumulated_features"].items(), key=lambda x: x[1], reverse=True)[:10]
        top_10 = {k: round(v, 4) for k, v in sorted_feats}
        
        return {
            "alert_id": state["alert_id"],
            "source_id": sim_id,
            "module": module,
            "status": status,
            "alert_start_ts": state["start_ts"],
            "alert_end_ts": end_ts,
            "peak_anomaly_ts": state["peak_ts"],
            "max_composite_score": round(state["max_score"], 4),
            "top_10_features": json.dumps(top_10),
            "last_updated_ts": datetime.utcnow().isoformat()
        }