# File: C:\streaming_emulator\tools\reset_writer.py
import shutil
import os
import glob
from pathlib import Path

# Config
# Go up one level from tools/ to project root
PROJECT_ROOT = Path(__file__).parent.parent
PATHS_TO_CLEAN = [
    PROJECT_ROOT / "data" / "delta" / "bronze",
    PROJECT_ROOT / "data" / "checkpoints" / "writer",
    PROJECT_ROOT / "writer_service" / "state",  # <--- Now cleans the entire state folder
]

def reset_writer():
    print("⚠️  WARNING: This will factory reset the Writer Service.")
    print("   The following data will be PERMANENTLY DELETED:")
    for p in PATHS_TO_CLEAN:
        print(f"    - {p}")
        
    confirm = input("\nAre you sure? (Type 'yes' to confirm): ").strip().lower()
    
    if confirm == 'yes':
        print("\n🧹 Cleaning up...")
        for p in PATHS_TO_CLEAN:
            if p.exists():
                try:
                    # If it's a directory, remove tree
                    if p.is_dir():
                        shutil.rmtree(p)
                    # If it's a file, remove file
                    else:
                        os.remove(p)
                    print(f"   ✅ Deleted: {p}")
                except Exception as e:
                    print(f"   ❌ Failed to delete {p}: {e}")
            else:
                print(f"   Using cached clean state (Not found): {p}")
                
        # Re-create the state directory so the listener doesn't crash
        state_dir = PROJECT_ROOT / "writer_service" / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        print(f"   ✅ Re-created empty state directory: {state_dir}")
        
        print("\n✨ Writer Service Reset Complete. You can now start fresh.")
    else:
        print("❌ Reset cancelled.")

if __name__ == "__main__":
    reset_writer()