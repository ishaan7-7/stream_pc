import os
import shutil
from pathlib import Path

def reset_alerts_layer():
    print("=========================================")
    print("🧹 RESETTING GOLD LAYER (ALERTS)")
    print("=========================================")
    
    # Define paths relative to the tools directory
    tools_dir = Path(__file__).resolve().parent
    root_dir = tools_dir.parent
    
    alerts_table_dir = root_dir / "data" / "delta" / "gold" / "alerts"
    state_dir = root_dir / "alerts_service" / "state"

    # 1. Clear the Alerts Delta Table
    if alerts_table_dir.exists():
        print(f"🗑️ Deleting Alerts Delta Table: {alerts_table_dir}")
        try:
            shutil.rmtree(alerts_table_dir)
            print("   ✅ Alerts table deleted.")
        except Exception as e:
            print(f"   ⚠️ Could not delete Alerts table: {e}")
    else:
        print("   ✅ Alerts table already empty/missing.")

    # 2. Clear State Files (Checkpoints & State Machine Cache)
    if state_dir.exists():
        print(f"🗑️ Clearing Alerts State Files: {state_dir}")
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
        print("   ✅ Alerts state cleared.")
    else:
        print("   ✅ Alerts state directory already empty/missing.")

    print("=========================================")
    print("✅ ALERTS LAYER FULLY RESET. Ready for fresh evaluation.")
    print("=========================================")

if __name__ == "__main__":
    reset_alerts_layer()