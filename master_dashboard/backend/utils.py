import os
import time
import json
import pickle
import pandas as pd
import numpy as np

def sanitize_dataframe(df: pd.DataFrame) -> list:
    """Cleans NaN, Inf, and serializes Datetimes for JSON."""
    if df.empty:
        return []
        
    # Convert datetimes to strings
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]', '<M8[ns]']).columns:
        df[col] = df[col].astype(str)
        
    # Replace Infinity and NaN with None (which becomes null in JSON)
    cleaned_df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    return cleaned_df.to_dict(orient="records")

def sanitize_dict(data: dict) -> dict:
    cleaned = {}
    for k, v in data.items():
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            cleaned[k] = None
        elif isinstance(v, dict):
            cleaned[k] = sanitize_dict(v)
        else:
            cleaned[k] = v
    return cleaned

def safe_read_pickle(file_path: str, retries: int = 5, delay: float = 0.05):
    if not os.path.exists(file_path):
        return None

    for attempt in range(retries):
        try:
            with open(file_path, "rb") as f:
                data = pickle.load(f)
                
            if isinstance(data, pd.DataFrame):
                return sanitize_dataframe(data)
            elif isinstance(data, dict):
                return sanitize_dict(data)
            return data
            
        except (EOFError, pickle.UnpicklingError, PermissionError):
            time.sleep(delay)
        except Exception as e:
            print(f"Critical error reading pickle {file_path}: {e}")
            return None
            
    print(f"Timeout: Could not read locked pickle {file_path} after {retries} attempts.")
    return None

def safe_read_json(file_path: str, retries: int = 5, delay: float = 0.05):
    if not os.path.exists(file_path):
        return None

    for attempt in range(retries):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError):
            time.sleep(delay)
        except Exception as e:
            print(f"Critical error reading json {file_path}: {e}")
            return None
            
    print(f"Timeout: Could not read locked json {file_path} after {retries} attempts.")
    return None