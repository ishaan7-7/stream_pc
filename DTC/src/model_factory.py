import torch
import torch.nn as nn
import torch.optim as optim
import pandas as pd
import numpy as np
import joblib
import sys
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler

# Ensure project root is in path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from DTC.src import config

# --- 1. The Neural Network Architecture ---
class DTC_Network(nn.Module):
    """
    A lightweight Feed-Forward Network designed for fast inference.
    Architecture: Input -> 32 (ReLU) -> 16 (ReLU) -> 1 (Sigmoid)
    """
    def __init__(self, input_dim):
        super(DTC_Network, self).__init__()
        self.layer1 = nn.Linear(input_dim, 32)
        self.layer2 = nn.Linear(32, 16)
        self.output = nn.Linear(16, 1)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = self.relu(self.layer1(x))
        x = self.relu(self.layer2(x))
        x = self.sigmoid(self.output(x))
        return x

# --- 2. The Factory Class ---
class ModelFactory:
    """
    Manages the training and saving of DTC models.
    """
    def __init__(self):
        self.loss_history = []

    def train(self, df, features, epochs=100, learning_rate=0.01, verbose=False):
        """
        Trains a PyTorch model on the provided synthetic dataframe.
        Returns: (Trained Model, Fitted Scaler)
        """
        # 1. Prepare Data
        # Extract features (X) and target (y)
        try:
            X = df[features].values
            y = df['target'].values.reshape(-1, 1)
        except KeyError as e:
            raise KeyError(f"Missing columns in training data: {e}")

        # 2. Scale Features (Crucial for Neural Networks)
        # We use MinMaxScaler to bind inputs between 0 and 1
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(X)

        # 3. Convert to PyTorch Tensors
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
        y_tensor = torch.tensor(y, dtype=torch.float32)

        # 4. Initialize Network
        input_dim = len(features)
        model = DTC_Network(input_dim)
        
        # 5. Define Training Strategy
        criterion = nn.BCELoss() # Binary Cross Entropy (Standard for 0-1 probability)
        optimizer = optim.Adam(model.parameters(), lr=learning_rate)

        # 6. Training Loop
        model.train()
        self.loss_history = []
        
        print(f"   -> Training started (Input Dim: {input_dim}, Rows: {len(df)})")
        for epoch in range(epochs):
            # Forward pass
            optimizer.zero_grad()
            outputs = model(X_tensor)
            loss = criterion(outputs, y_tensor)
            
            # Backward pass
            loss.backward()
            optimizer.step()
            
            self.loss_history.append(loss.item())
            
            if verbose and (epoch + 1) % 20 == 0:
                print(f"      Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

        print(f"   -> Training complete. Final Loss: {self.loss_history[-1]:.4f}")
        return model, scaler

    def save_artifacts(self, model, scaler, module_name, dtc_code):
        """
        Saves the trained model (.pt) and scaler (.pkl) to the artifacts folder.
        """
        # Get path from config (e.g., DTC/artifacts/engine/P0217)
        _, save_dir = config.ensure_dirs(module_name, dtc_code)
        
        # Save Model Weights
        model_path = save_dir / "model.pt"
        torch.save(model.state_dict(), model_path)
        
        # Save Scaler Object
        scaler_path = save_dir / "scaler.pkl"
        joblib.dump(scaler, scaler_path)
        
        print(f"   -> Artifacts saved to: {save_dir}")
        return save_dir