import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# --- CONFIGURATION ---
INPUT_REAL_DATA = "raw_31800_engine.csv"  
OUTPUT_FILE = "sim001_engine_40d.csv"  # <-- Explicitly naming the new output file
SIM_ID = "sim001"
DAYS_TO_GENERATE = 40                  # <-- Explicitly set to 40 days
TARGET_FREQ_SECONDS = 4
BLOCK_MINUTES = 5
ROWS_PER_BLOCK = int((BLOCK_MINUTES * 60) / TARGET_FREQ_SECONDS)

# Fault injection timeline
FAULT_START_DAY = 10
FAULT_END_DAY = 38

def load_and_prep_blocks(filepath):
    print(f"[*] Loading raw data from {filepath}...")
    df = pd.read_csv(filepath)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', utc=True)
    df.set_index('timestamp', inplace=True)
    
    cols_to_drop = ['date', 'source_id', 'ingest_ts', 'writer_ts', 'row_hash']
    physics_cols = [c for c in df.columns if c not in cols_to_drop]
    
    for col in physics_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    df_resampled = df[physics_cols].resample(f'{TARGET_FREQ_SECONDS}s').mean()
    df_resampled = df_resampled.interpolate(method='linear', limit=2)
    
    blocks = []
    for i in range(0, len(df_resampled) - ROWS_PER_BLOCK, ROWS_PER_BLOCK):
        chunk = df_resampled.iloc[i : i + ROWS_PER_BLOCK]
        if not chunk.isna().any().any():
            blocks.append(chunk.reset_index(drop=True))
            
    print(f"[*] Extracted {len(blocks)} continuous physics blocks.")
    return blocks, physics_cols

def smooth_seams(df, window=5):
    seam_indices = list(range(ROWS_PER_BLOCK, len(df), ROWS_PER_BLOCK))
    for idx in seam_indices:
        start = max(0, idx - window)
        end = min(len(df), idx + window)
        df.iloc[start:end] = df.iloc[start:end].rolling(window=3, center=True, min_periods=1).mean()
    return df

def apply_vacuum_leak_physics(df, day_index):
    # Days 0-9 and Days 39-40 (Post-Service) get NO faults
    if day_index < FAULT_START_DAY or day_index > FAULT_END_DAY:
        return df

    # Scales from 0.0 (Day 10) to 1.0 (Day 38)
    degradation_factor = (day_index - FAULT_START_DAY + 1) / (FAULT_END_DAY - FAULT_START_DAY + 1)
    
    # Apply physical drift
    df['mass_air_flow_rate_g_s'] *= (1 - (0.35 * degradation_factor))
    df['fuel_trim_bank_1_long_term'] += (25.0 * degradation_factor)
    df['fuel_trim_bank_1_short_term'] += (8.0 * degradation_factor)
    
    if degradation_factor > 0.75:
        lean_severity = (degradation_factor - 0.75) / 0.25
        df['air_fuel_ratio_measured_1'] += (1.8 * lean_severity)
        
    if degradation_factor > 0.85:
        misfire_severity = (degradation_factor - 0.85) / 0.15
        t = np.arange(len(df))
        
        rpm_volatility = (np.sin(t * 0.8) * 120 * misfire_severity) + (np.random.normal(0, 60, len(t)) * misfire_severity)
        df['engine_rpm_rpm'] += rpm_volatility
        
        load_volatility = np.random.normal(0, 8, len(t)) * misfire_severity
        df['engine_load_absolute'] += load_volatility
        df['engine_load_absolute'] = df['engine_load_absolute'].clip(upper=100.0)

    return df

def generate_trip(blocks, duration_minutes, start_time, day_index):
    num_blocks_needed = int(duration_minutes / BLOCK_MINUTES)
    trip_blocks = [random.choice(blocks) for _ in range(num_blocks_needed)]
    
    trip_df = pd.concat(trip_blocks, ignore_index=True)
    trip_df = smooth_seams(trip_df)
    
    # Inject the faults based on what day it is
    trip_df = apply_vacuum_leak_physics(trip_df, day_index)
    
    timestamps = [start_time + timedelta(seconds=i * TARGET_FREQ_SECONDS) for i in range(len(trip_df))]
    
    trip_df['timestamp'] = [ts.strftime('%Y-%m-%d %H:%M:%S+00:00') for ts in timestamps]
    trip_df['date'] = [ts.strftime('%Y-%m-%d') for ts in timestamps]
    trip_df['source_id'] = SIM_ID
    
    return trip_df

def main():
    blocks, physics_cols = load_and_prep_blocks(INPUT_REAL_DATA)
    start_date = datetime(2024, 7, 5, 0, 0, 0)
    all_trips = []
    
    print(f"[*] Synthesizing {DAYS_TO_GENERATE} days of heavy-duty storyline telemetry for {SIM_ID}...")
    for day in range(DAYS_TO_GENERATE):
        current_day = start_date + timedelta(days=day)
        
        # Shift 1: Morning Route (3.5 to 4.5 hours)
        shift1_start = current_day.replace(hour=random.randint(6, 7), minute=random.randint(0, 30))
        shift1_duration = random.randint(210, 270) 
        all_trips.append(generate_trip(blocks, shift1_duration, shift1_start, day))
        
        # Shift 2: Mid-day Route (3 to 4 hours)
        shift2_start = current_day.replace(hour=random.randint(12, 13), minute=random.randint(0, 30))
        shift2_duration = random.randint(180, 240)
        all_trips.append(generate_trip(blocks, shift2_duration, shift2_start, day))
        
        # Shift 3: Evening Route (2 to 2.5 hours)
        shift3_start = current_day.replace(hour=random.randint(18, 19), minute=random.randint(0, 30))
        shift3_duration = random.randint(120, 150)
        all_trips.append(generate_trip(blocks, shift3_duration, shift3_start, day))
        
    final_df = pd.concat(all_trips, ignore_index=True)
    
    # Enforce exact column order required by the pipeline
    cols = ['timestamp', 'date', 'source_id'] + physics_cols
    final_df = final_df[cols]
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Generated {len(final_df)} rows. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()