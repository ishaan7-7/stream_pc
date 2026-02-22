import os
import shutil
from pathlib import Path

def reset_gold_layer():
    print("=========================================")
    print("🧹 RESETTING GOLD LAYER (VEHICLE HEALTH)")
    print("=========================================")
    
    # Define paths relative to the tools directory
    tools_dir = Path(__file__).resolve().parent
    root_dir = tools_dir.parent
    
    gold_table_dir = root_dir / "data" / "delta" / "gold" / "vehicle_health"
    state_dir = root_dir / "gold_service" / "state"

    # 1. Clear the Gold Delta Table
    if gold_table_dir.exists():
        print(f"🗑️ Deleting Gold Delta Table: {gold_table_dir}")
        try:
            shutil.rmtree(gold_table_dir)
            print("   ✅ Gold table deleted.")
        except Exception as e:
            print(f"   ⚠️ Could not delete Gold table: {e}")
    else:
        print("   ✅ Gold table already empty/missing.")

    # 2. Clear State Files (Checkpoints & Zero-Order Hold Cache)
    if state_dir.exists():
        print(f"🗑️ Clearing Gold State Files: {state_dir}")
        for item in state_dir.iterdir():
            if item.name == ".gitignore":
                continue
            try:
                if item.is_file():
                    item.unlink()
                    print(f"   ✅ Deleted {item.name}")
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"   ⚠️ Could not delete {item.name}: {e}")
        print("   ✅ Gold state cleared.")
    else:
        print("   ✅ Gold state directory already empty/missing.")

    print("=========================================")
    print("✅ GOLD LAYER FULLY RESET. Ready for fresh fusion.")
    print("=========================================")

if __name__ == "__main__":
    reset_gold_layer()