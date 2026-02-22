
import os
import json
import joblib
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from datetime import datetime

from src import config

class LSTMAutoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim=64, num_layers=2, dropout=0.2):
        super(LSTMAutoencoder, self).__init__()
        self.encoder = nn.LSTM(input_size=input_dim, hidden_size=hidden_dim, 
                               num_layers=num_layers, batch_first=True, 
                               dropout=dropout if num_layers > 1 else 0)
        self.decoder = nn.LSTM(input_size=hidden_dim, hidden_size=input_dim, 
                               num_layers=num_layers, batch_first=True, 
                               dropout=dropout if num_layers > 1 else 0)
    def forward(self, x):
        encoded, (hidden, cell) = self.encoder(x)
        repeated_hidden = hidden[-1].unsqueeze(1).repeat(1, x.size(1), 1)
        decoded, _ = self.decoder(repeated_hidden)
        return decoded

class MLEngine:
    def __init__(self, state_manager, module):
        self.state = state_manager
        self.module = module  
        self.device = torch.device("cpu") 
        self.models = {}
        self._load_artifact()

    def _load_artifact(self):
        print(f"🧠 Loading ML Artifacts for {self.module.upper()}...")
        path = os.path.join(config.ARTIFACTS_DIR, self.module)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing artifacts for {self.module} at {path}")
            
        with open(os.path.join(path, "model_meta.json")) as f: meta = json.load(f)
        with open(os.path.join(path, "features.json")) as f: feats = json.load(f)["features"]
        
        scaler = joblib.load(os.path.join(path, "scaler.pkl"))
        gmm = joblib.load(os.path.join(path, "gmm.pkl"))
        
        lstm = LSTMAutoencoder(input_dim=len(feats)).to(self.device)
        lstm.load_state_dict(torch.load(os.path.join(path, "lstm_model.pt"), map_location=self.device, weights_only=True))
        lstm.eval()
        
        self.models[self.module] = {
            "lstm": lstm, "gmm": gmm, "scaler": scaler, 
            "meta": meta, "expected_cols": feats
        }

    def process_batch(self, df_batch, sim_id):
        # FIXED: Define artifacts
        artifacts = self.models[self.module]
        expected_cols = artifacts["expected_cols"]
        meta = artifacts["meta"]
        
        # 1. Pre-Flight Check
        available_cols = set(df_batch.columns)
        missing_cols = [c for c in expected_cols if c not in available_cols]
        missing_count = len(missing_cols)
        
        if missing_count > 3:
            msg = f"CRITICAL: {missing_count} sensors missing. {missing_cols}"
            # FIXED: Removed 'module' arg from state calls
            self.state.log_alert(sim_id, "CRITICAL", msg)
            raise ValueError(f"Inference aborted for {sim_id} {self.module}: {msg}")
        elif missing_count > 0:
            self.state.log_alert(sim_id, "WARNING", f"Missing {missing_count} sensors. Masking active.")

        # 2. Extract Data & Impute
        df_numeric = df_batch.reindex(columns=expected_cols).ffill()
        for col in expected_cols:
            if col in meta.get('feature_means', {}):
                df_numeric[col] = df_numeric[col].fillna(meta['feature_means'][col])
            else:
                df_numeric[col] = df_numeric[col].fillna(0.0)

        X_scaled = artifacts["scaler"].transform(df_numeric)
        
        # 3. Retrieve Historical State
        # FIXED: Removed 'module' arg from state calls
        ml_state = self.state.get_ml_state(sim_id)
        window_size = meta["sequence_length"]
        
        if ml_state["last_window_data"] is not None:
            history = np.array(ml_state["last_window_data"])
            X_combined = np.vstack([history, X_scaled])
        else:
            padding = np.tile(X_scaled[0], (window_size - 1, 1))
            X_combined = np.vstack([padding, X_scaled])

        new_last_window = X_combined[-(window_size - 1):].tolist()

        # 4. Sequence Building
        sequences = [X_combined[i : i + window_size] for i in range(len(X_combined) - window_size + 1)]
        X_tensor = torch.tensor(np.array(sequences), dtype=torch.float32).to(self.device)
        
        mask_indices = [expected_cols.index(c) for c in missing_cols]
        
        # 5. ML Inference Execution
        lstm_thresh = meta["lstm_thresholds"]
        gmm_thresh = meta["gmm_thresholds"]
        
        ema_err = ml_state["ema_error"]
        pers_count = ml_state["persistence_counter"]
        
        out_rows = []
        
        with torch.no_grad():
            for i in range(0, len(X_tensor), config.BATCH_SIZE):
                batch = X_tensor[i : i + config.BATCH_SIZE]
                rec = artifacts["lstm"](batch)
                
                diff = (batch - rec)**2
                if mask_indices:
                    mask = torch.ones_like(diff)
                    mask[:, :, mask_indices] = 0.0
                    diff = diff * mask
                
                rec_error = torch.mean(diff, dim=(1,2)).cpu().numpy()
                feat_diff = diff[:, -1, :].cpu().numpy()
                
                last_step_data = batch[:, -1, :].cpu().numpy()
                gmm_log_probs = artifacts["gmm"].score_samples(last_step_data)
                
                for j in range(len(batch)):
                    raw_err = float(rec_error[j])
                    
                    ema_err = (config.EMA_ALPHA * raw_err) + ((1 - config.EMA_ALPHA) * ema_err)
                    
                    log_prob = float(gmm_log_probs[j])
                    limit = gmm_thresh['p05']
                    if log_prob >= limit: familiarity = 1.0
                    else: familiarity = max(1.0 - (abs(limit - log_prob) / (abs(limit - gmm_thresh['min']) + 1e-6)), 0.0)

                    if ema_err <= lstm_thresh["p95"]: comp = 0.5 * (ema_err / max(lstm_thresh["p95"], 1e-6))
                    elif ema_err <= lstm_thresh["p99_5"]: comp = 0.5 + 0.3 * ((ema_err - lstm_thresh["p95"]) / max((lstm_thresh["p99_5"] - lstm_thresh["p95"]), 1e-6))
                    else: comp = 0.8 + 0.2 * min((ema_err - lstm_thresh["p99_5"]) / max((lstm_thresh["max"] - lstm_thresh["p99_5"]), 1e-6), 1.0)
                    
                    if familiarity > 0.8 and comp < 0.9: comp *= 0.6
                    elif familiarity < 0.2: comp = min(comp + 0.2, 1.0)
                    
                    sev, code = "NORMAL", 0
                    if comp >= 0.8: pers_count += 1
                    elif comp >= 0.5:
                        pers_count = max(0, pers_count - 1)
                        sev, code = "WARNING", 1
                    else: pers_count = 0
                    
                    if pers_count >= config.PERSISTENCE_LIMIT:
                        sev, code = "CRITICAL", 2
                        comp = max(comp, 0.85)
                    elif comp >= 0.8: sev, code = "WARNING", 1
                    
                    row_feats = feat_diff[j]
                    top_k = {expected_cols[k]: float(row_feats[k]) for k in np.argsort(row_feats)[-8:][::-1]}
                    
                    original_idx = df_batch.index[i+j]
                    
                    out_rows.append({
                        "row_hash": df_batch.at[original_idx, "row_hash"],
                        "source_id": df_batch.at[original_idx, "source_id"] if "source_id" in df_batch.columns else sim_id,
                        "module": self.module,
                        "timestamp": df_batch.at[original_idx, "timestamp"],
                        "ingest_ts": df_batch.at[original_idx, "ingest_ts"] if "ingest_ts" in df_batch.columns else None,
                        "writer_ts": df_batch.at[original_idx, "writer_ts"] if "writer_ts" in df_batch.columns else None,
                        "inference_ts": datetime.utcnow().isoformat(),
                        "lstm_raw_error": raw_err,
                        "lstm_smoothed": ema_err,
                        "composite_score": comp,
                        "health_score": (1.0 - comp) * 100,
                        "severity": sev,
                        "severity_code": code,
                        "top_features": json.dumps(top_k)
                    })

        # 6. Update State
        # FIXED: Removed 'module' arg from state calls
        self.state.update_ml_state(sim_id, ema_err, pers_count, new_last_window)
        
        return pd.DataFrame(out_rows)
