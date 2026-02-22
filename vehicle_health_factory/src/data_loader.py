
import pandas as pd
import numpy as np
import json
import torch
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from src import config

class TimeSeriesDataset(Dataset):
    def __init__(self, sequences):
        self.sequences = torch.FloatTensor(sequences)

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, idx):
        return self.sequences[idx]

def get_module_features(module_name: str) -> list:
    with open(config.MASTER_JSON_PATH, 'r') as f:
        master_data = json.load(f)
    
    # 1. Look inside the "modules" key
    modules_dict = master_data.get("modules", {})
    
    if module_name not in modules_dict:
        raise ValueError(f"Module {module_name} not found in master.json under 'modules'")
    
    # 2. Extract features from "columns" (not "properties")
    columns_dict = modules_dict[module_name].get("columns", {})
    
    # 3. Exclude non-numeric metadata columns so they don't crash the scaler
    exclude_cols = ["timestamp", "date", "source_id", "vehicle_id"]
    
    features = [key for key in columns_dict.keys() if key not in exclude_cols]
    
    return sorted(features)
def load_and_preprocess_data(module_name: str):
    csv_path = f"{config.DATA_DIR}/{module_name}.csv"
    df = pd.read_csv(csv_path)

    # 1. Parse Contract & Filter
    expected_features = get_module_features(module_name)
    
    available_cols = set(df.columns)
    missing_cols = [f for f in expected_features if f not in available_cols]
    if missing_cols:
        raise ValueError(f"Training data for {module_name} is missing contract columns: {missing_cols}")

    # 2. Sort temporally, then strip timestamp
    if 'timestamp' in df.columns:
        df = df.sort_values('timestamp')
    
    df = df[expected_features]

    # 3. Handle missing values (Mean imputation for training baseline)
    feature_means = df.mean().to_dict()
    df = df.fillna(df.mean())

    # 4. Scale
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df)

    # 5. Create Sequences
    sequences = []
    for i in range(len(scaled_data) - config.SEQ_LENGTH + 1):
        sequences.append(scaled_data[i : i + config.SEQ_LENGTH])
    
    sequences = np.array(sequences)

    return sequences, scaled_data, scaler, expected_features, feature_means
