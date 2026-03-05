import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Configuration
INPUT_REAL_DATA = "real_8h_engine.csv"  # Replace with your actual 8-hour dataset filename
OUTPUT_FILE = "gold_standard_engine.csv"
SIM_ID = "sim001"
DAYS_TO_GENERATE = 10
FREQ_SECONDS = 4
BLOCK_MINUTES = 5
ROWS_PER_BLOCK = int((BLOCK_MINUTES * 60) / FREQ_SECONDS)

def load_and_prep_blocks(filepath):
    """Loads the real data and chunks it into usable 5-minute blocks."""
    print(f"Loading real data from {filepath}...")
    df = pd.read_csv(filepath)
    
    # Drop original structural columns to isolate pure physics
    cols_to_drop = ['timestamp', 'date', 'source_id', 'ingest_ts', 'writer_ts', 'row_hash']
    physics_cols = [c for c in df.columns if c not in cols_to_drop]
    df_physics = df[physics_cols]
    
    blocks = []
    for i in range(0, len(df_physics) - ROWS_PER_BLOCK, ROWS_PER_BLOCK):
        blocks.append(df_physics.iloc[i : i + ROWS_PER_BLOCK].reset_index(drop=True))
    
    print(f"Created {len(blocks)} physical blocks of {BLOCK_MINUTES} minutes each.")
    return blocks, physics_cols

def smooth_seams(df, window=5):
    """Applies a rolling average strictly at the block seams to maintain physics."""
    seam_indices = list(range(ROWS_PER_BLOCK, len(df), ROWS_PER_BLOCK))
    for idx in seam_indices:
        start = max(0, idx - window)
        end = min(len(df), idx + window)
        df.iloc[start:end] = df.iloc[start:end].rolling(window=3, center=True, min_periods=1).mean()
    return df

def generate_trip(blocks, duration_minutes, start_time):
    """Stitches random blocks to form a continuous trip."""
    num_blocks_needed = int(duration_minutes / BLOCK_MINUTES)
    trip_blocks = [random.choice(blocks) for _ in range(num_blocks_needed)]
    
    trip_df = pd.concat(trip_blocks, ignore_index=True)
    trip_df = smooth_seams(trip_df)
    
    # Generate strict timestamps
    timestamps = [start_time + timedelta(seconds=i * FREQ_SECONDS) for i in range(len(trip_df))]
    
    trip_df['timestamp'] = [ts.strftime('%Y-%m-%d %H:%M:%S+00:00') for ts in timestamps]
    trip_df['date'] = [ts.strftime('%Y-%m-%d') for ts in timestamps]
    trip_df['source_id'] = SIM_ID
    
    return trip_df

def main():
    blocks, physics_cols = load_and_prep_blocks(INPUT_REAL_DATA)
    
    start_date = datetime(2024, 7, 5, 0, 0, 0) # Base start date
    all_trips = []
    
    print(f"Synthesizing {DAYS_TO_GENERATE} days of Gold Standard data...")
    for day in range(DAYS_TO_GENERATE):
        current_day = start_date + timedelta(days=day)
        
        # Trip 1: Morning Commute (07:00 to 09:00 window)
        trip1_start = current_day.replace(hour=random.randint(7, 8), minute=random.randint(0, 59))
        trip1_duration = random.randint(20, 60)
        all_trips.append(generate_trip(blocks, trip1_duration, trip1_start))
        
        # Trip 2: Evening Commute (17:00 to 19:00 window)
        trip2_start = current_day.replace(hour=random.randint(17, 18), minute=random.randint(0, 59))
        trip2_duration = random.randint(20, 60)
        all_trips.append(generate_trip(blocks, trip2_duration, trip2_start))
        
    final_df = pd.concat(all_trips, ignore_index=True)
    
    # Reorder columns to ensure timestamp, date, source_id are first
    cols = ['timestamp', 'date', 'source_id'] + physics_cols
    final_df = final_df[cols]
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Generated {len(final_df)} rows of Gold Standard data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()