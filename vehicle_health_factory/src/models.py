import torch
import torch.nn as nn
from sklearn.ensemble import IsolationForest

class LSTMAutoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_layers, dropout):
        super(LSTMAutoencoder, self).__init__()
        
        self.encoder = nn.LSTM(
            input_size=input_dim, 
            hidden_size=hidden_dim, 
            num_layers=num_layers, 
            batch_first=True, 
            dropout=dropout if num_layers > 1 else 0
        )
        
        self.decoder = nn.LSTM(
            input_size=hidden_dim, 
            hidden_size=input_dim, 
            num_layers=num_layers, 
            batch_first=True, 
            dropout=dropout if num_layers > 1 else 0
        )

    def forward(self, x):
        encoded, (hidden, cell) = self.encoder(x)
        # Repeat the last hidden state for decoding
        repeated_hidden = hidden[-1].unsqueeze(1).repeat(1, x.size(1), 1)
        decoded, _ = self.decoder(repeated_hidden)
        return decoded

def build_iforest():
    # n_estimators=100 is standard, contamination is an estimate of real-world anomaly rate
    return IsolationForest(n_estimators=100, contamination=0.01, random_state=42)