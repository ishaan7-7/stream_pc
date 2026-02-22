import os
import json
import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt
import pickle
from torch.utils.data import DataLoader
from tqdm import tqdm

from src import config
from src.data_loader import load_and_preprocess_data, TimeSeriesDataset
from src.models import LSTMAutoencoder, build_iforest

def train_module(module_name: str):
    print(f"\n{'='*50}\nTraining Module: {module_name.upper()}\n{'='*50}")
    
    module_artifact_dir = os.path.join(config.ARTIFACTS_DIR, module_name)
    os.makedirs(module_artifact_dir, exist_ok=True)

    # 1. Load Data
    sequences, scaled_flat_data, scaler, features, feature_means = load_and_preprocess_data(module_name)
    input_dim = len(features)
    
    dataset = TimeSeriesDataset(sequences)
    dataloader = DataLoader(dataset, batch_size=config.BATCH_SIZE, shuffle=True)

    # 2. Initialize Model (CPU bounded)
    device = torch.device("cpu")
    model = LSTMAutoencoder(
        input_dim=input_dim,
        hidden_dim=config.HIDDEN_DIM,
        num_layers=config.NUM_LAYERS,
        dropout=config.DROPOUT
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=config.LEARNING_RATE, weight_decay=1e-4)
    criterion = nn.MSELoss()

    # 3. Train Model
    model.train()
    loss_history = []
    best_loss = float('inf')
    patience_counter = 0

    print("Training LSTM Autoencoder...")
    for epoch in range(config.EPOCHS):
        epoch_loss = 0
        for batch in tqdm(dataloader, desc=f"Epoch {epoch+1}/{config.EPOCHS}", leave=False):
            batch = batch.to(device)
            optimizer.zero_grad()
            output = model(batch)
            loss = criterion(output, batch)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        
        avg_loss = epoch_loss / len(dataloader)
        loss_history.append(avg_loss)
        
        if avg_loss < best_loss:
            best_loss = avg_loss
            patience_counter = 0
            torch.save(model.state_dict(), os.path.join(module_artifact_dir, "lstm_model.pt"))
        else:
            patience_counter += 1
            
        if patience_counter >= config.PATIENCE:
            print(f"Early stopping triggered at epoch {epoch+1}")
            break

    # 4. Train Isolation Forest
    print("Training Isolation Forest...")
    iforest = build_iforest()
    iforest.fit(scaled_flat_data)

    # 5. Calibration Pass
    print("Executing Calibration Pass...")
    model.load_state_dict(torch.load(os.path.join(module_artifact_dir, "lstm_model.pt"), weights_only=True))
    model.eval()
    
    calibration_loader = DataLoader(dataset, batch_size=config.BATCH_SIZE, shuffle=False)
    lstm_errors = []
    
    with torch.no_grad():
        for batch in calibration_loader:
            batch = batch.to(device)
            output = model(batch)
            mse = torch.mean(torch.pow(batch - output, 2), dim=(1, 2)).cpu().numpy()
            lstm_errors.extend(mse)
            
    lstm_errors = np.array(lstm_errors)
    if_scores = -iforest.score_samples(scaled_flat_data)
    
    model_meta = {
        "lstm_thresholds": {
            "p50": float(np.percentile(lstm_errors, 50)),
            "p90": float(np.percentile(lstm_errors, 90)),
            "p95": float(np.percentile(lstm_errors, 95)),
            "p99": float(np.percentile(lstm_errors, 99)),
            "p99_5": float(np.percentile(lstm_errors, 99.5)),
            "max": float(np.max(lstm_errors))
        },
        "iforest_thresholds": {
            "p50": float(np.percentile(if_scores, 50)),
            "p90": float(np.percentile(if_scores, 90)),
            "p95": float(np.percentile(if_scores, 95)),
            "p99": float(np.percentile(if_scores, 99)),
            "max": float(np.max(if_scores))
        },
        "feature_means": feature_means,
        "sequence_length": config.SEQ_LENGTH
    }

    # 6. Save Artifacts
    with open(os.path.join(module_artifact_dir, "features.json"), "w") as f:
        json.dump({"features": features}, f, indent=4)
        
    with open(os.path.join(module_artifact_dir, "model_meta.json"), "w") as f:
        json.dump(model_meta, f, indent=4)
        
    with open(os.path.join(module_artifact_dir, "scaler.pkl"), "wb") as f:
        pickle.dump(scaler, f)
        
    with open(os.path.join(module_artifact_dir, "iforest.pkl"), "wb") as f:
        pickle.dump(iforest, f)

    plt.figure(figsize=(10, 4))
    plt.plot(loss_history, label='Training Loss')
    plt.axhline(y=best_loss, color='r', linestyle='--', label=f'Best Loss: {best_loss:.4f}')
    plt.title(f'{module_name.capitalize()} LSTM Training')
    plt.xlabel('Epoch')
    plt.ylabel('MSE Loss')
    plt.legend()
    plt.savefig(os.path.join(module_artifact_dir, "training_report.png"))
    plt.close()
    
    print(f"Artifacts saved to {module_artifact_dir}")

if __name__ == "__main__":
    if not os.path.exists(config.MASTER_JSON_PATH):
        raise FileNotFoundError(f"CRITICAL: Cannot find master.json at {config.MASTER_JSON_PATH}.")

    for module in config.MODULES:
        try:
            train_module(module)
        except Exception as e:
            print(f"Failed to train module {module}: {str(e)}")