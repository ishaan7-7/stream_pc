import os
import json
import pickle
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from sklearn.mixture import GaussianMixture
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = "gold_standard_engine.csv"
ARTIFACTS_DIR = os.path.join("artifacts", "engine")
SEQ_LENGTH = 30
BATCH_SIZE = 128
EPOCHS = 50
PATIENCE = 5
LEARNING_RATE = 1e-3
HIDDEN_DIM = 64
NUM_LAYERS = 2

# --- MODELS ---
class LSTMAutoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_layers, dropout=0.2):
        super(LSTMAutoencoder, self).__init__()
        self.encoder = nn.LSTM(input_size=input_dim, hidden_size=hidden_dim, 
                               num_layers=num_layers, batch_first=True, 
                               dropout=dropout if num_layers > 1 else 0)
        self.decoder = nn.LSTM(input_size=hidden_dim, hidden_size=input_dim, 
                               num_layers=num_layers, batch_first=True, 
                               dropout=dropout if num_layers > 1 else 0)
    def forward(self, x):
        _, (hidden, _) = self.encoder(x)
        repeated_hidden = hidden[-1].unsqueeze(1).repeat(1, x.size(1), 1)
        decoded, _ = self.decoder(repeated_hidden)
        return decoded

class TimeSeriesDataset(Dataset):
    def __init__(self, sequences):
        self.sequences = torch.tensor(sequences, dtype=torch.float32)
    def __len__(self):
        return len(self.sequences)
    def __getitem__(self, idx):
        return self.sequences[idx]

# --- PIPELINE ---
def main():
    print(f"\n{'='*50}\n🚀 Training Models on Gold Standard (GPU Enabled)\n{'='*50}")
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

    # 1. Device Setup for GTX 1650
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[*] Compute Device: {device.type.upper()}")
    if device.type != "cuda":
        print("[!] WARNING: CUDA not detected. Training will be slow. Check PyTorch installation.")

    # 2. Data Loading & Preprocessing
    print(f"[*] Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    cols_to_drop = ['timestamp', 'date', 'source_id', 'ingest_ts', 'writer_ts', 'row_hash']
    features = [c for c in df.columns if c not in cols_to_drop]
    df_physics = df[features]
    
    feature_means = df_physics.mean().to_dict()
    
    print("[*] Scaling data...")
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df_physics)
    
    # 3. Sequence Generation
    print(f"[*] Building sequences (Window: {SEQ_LENGTH})...")
    sequences = []
    # We don't cross trip boundaries (missing data gaps). 
    # Since our gold standard is stitched blocks, we just slide the window.
    for i in range(len(scaled_data) - SEQ_LENGTH + 1):
        sequences.append(scaled_data[i : i + SEQ_LENGTH])
    
    sequences = np.array(sequences)
    
    # Split
    train_seqs, val_seqs = train_test_split(sequences, test_size=0.2, random_state=42, shuffle=True)
    train_loader = DataLoader(TimeSeriesDataset(train_seqs), batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(TimeSeriesDataset(val_seqs), batch_size=BATCH_SIZE, shuffle=False)

    # 4. LSTM Training
    print("\n[*] Initializing LSTM Autoencoder...")
    model = LSTMAutoencoder(input_dim=len(features), hidden_dim=HIDDEN_DIM, num_layers=NUM_LAYERS).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=1e-4)
    criterion = nn.MSELoss()

    train_losses, val_losses = [], []
    best_val_loss = float('inf')
    patience_counter = 0

    for epoch in range(EPOCHS):
        model.train()
        epoch_train_loss = 0
        for batch in tqdm(train_loader, desc=f"Epoch {epoch+1}/{EPOCHS} [Train]", leave=False):
            batch = batch.to(device)
            optimizer.zero_grad()
            output = model(batch)
            loss = criterion(output, batch)
            loss.backward()
            optimizer.step()
            epoch_train_loss += loss.item()
            
        avg_train_loss = epoch_train_loss / len(train_loader)
        train_losses.append(avg_train_loss)

        model.eval()
        epoch_val_loss = 0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                loss = criterion(model(batch), batch)
                epoch_val_loss += loss.item()
                
        avg_val_loss = epoch_val_loss / len(val_loader)
        val_losses.append(avg_val_loss)

        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            patience_counter = 0
            torch.save(model.state_dict(), os.path.join(ARTIFACTS_DIR, "lstm_model.pt"))
        else:
            patience_counter += 1
            
        if patience_counter >= PATIENCE:
            print(f"[*] Early stopping triggered at epoch {epoch+1}")
            break

    # 5. GMM Training (Regime Classifier)
    print("\n[*] Training Gaussian Mixture Model (GMM)...")
    # GMM maps the physics space. We train it on the flat, scaled data.
    gmm = GaussianMixture(n_components=3, covariance_type='full', random_state=42)
    gmm.fit(scaled_data)

    # 6. Calibration Pass (Finding the pure thresholds)
    print("[*] Executing Calibration Pass...")
    model.load_state_dict(torch.load(os.path.join(ARTIFACTS_DIR, "lstm_model.pt"), weights_only=True))
    model.eval()
    
    calib_loader = DataLoader(TimeSeriesDataset(sequences), batch_size=BATCH_SIZE, shuffle=False)
    lstm_errors = []
    
    with torch.no_grad():
        for batch in calib_loader:
            batch = batch.to(device)
            output = model(batch)
            # Match exactly how inference calculates error (Mean over features and sequence)
            mse = torch.mean(torch.pow(batch - output, 2), dim=(1, 2)).cpu().numpy()
            lstm_errors.extend(mse)
            
    lstm_errors = np.array(lstm_errors)
    gmm_scores = gmm.score_samples(scaled_data)
    
    model_meta = {
        "lstm_thresholds": {
            "p50": float(np.percentile(lstm_errors, 50)),
            "p90": float(np.percentile(lstm_errors, 90)),
            "p95": float(np.percentile(lstm_errors, 95)),
            "p99": float(np.percentile(lstm_errors, 99)),
            "p99_5": float(np.percentile(lstm_errors, 99.5)),
            "max": float(np.max(lstm_errors))
        },
        "gmm_thresholds": {
            "p05": float(np.percentile(gmm_scores, 5)),
            "min": float(np.min(gmm_scores))
        },
        "feature_means": feature_means,
        "sequence_length": SEQ_LENGTH
    }

    # 7. Save Artifacts
    print("[*] Saving Artifacts...")
    with open(os.path.join(ARTIFACTS_DIR, "features.json"), "w") as f:
        json.dump({"features": features}, f, indent=4)
    with open(os.path.join(ARTIFACTS_DIR, "model_meta.json"), "w") as f:
        json.dump(model_meta, f, indent=4)
    with open(os.path.join(ARTIFACTS_DIR, "scaler.pkl"), "wb") as f:
        pickle.dump(scaler, f)
    with open(os.path.join(ARTIFACTS_DIR, "gmm.pkl"), "wb") as f:
        pickle.dump(gmm, f)

    # 8. Training Plot
    plt.figure(figsize=(10, 5))
    plt.plot(train_losses, label='Training Loss', color='blue')
    plt.plot(val_losses, label='Validation Loss', color='orange')
    plt.axhline(y=best_val_loss, color='r', linestyle='--', label=f'Best Val Loss: {best_val_loss:.4f}')
    plt.title('Gold Standard LSTM Training')
    plt.xlabel('Epoch')
    plt.ylabel('MSE Loss')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(ARTIFACTS_DIR, "training_report.png"))
    plt.close()
    
    print(f"\n✅ Phase 2 Complete. All gold standard artifacts saved to {ARTIFACTS_DIR}/")

if __name__ == "__main__":
    main()