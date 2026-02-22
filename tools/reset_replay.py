
import os
import shutil
from pathlib import Path

def reset_replay():
    print("=========================================")
    print("🧹 RESETTING REPLAY SERVICE")
    print("=========================================")
    
    # Define paths relative to the tools directory
    tools_dir = Path(__file__).resolve().parent
    root_dir = tools_dir.parent
    
    checkpoints_dir = root_dir / "replay" / "checkpoints"
    dlq_dir = root_dir / "replay" / "dlq"

    # 1. Clear Checkpoints and Archives
    if checkpoints_dir.exists():
        print(f"🗑️ Clearing Replay Checkpoints: {checkpoints_dir}")
        for item in checkpoints_dir.iterdir():
            if item.name == ".gitignore": 
                continue # Keep the gitignore
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"   ⚠️ Could not delete {item.name}: {e}")
        print("   ✅ Replay checkpoints cleared.")
    else:
        print("   ✅ Replay checkpoints already empty.")

    # 2. Clear DLQ (Dead Letter Queue)
    if dlq_dir.exists():
        print(f"🗑️ Clearing Replay DLQ: {dlq_dir}")
        for item in dlq_dir.iterdir():
            if item.name == ".gitignore":
                continue
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"   ⚠️ Could not delete {item.name}: {e}")
        print("   ✅ Replay DLQ cleared.")
    else:
        print("   ✅ Replay DLQ already empty.")

    print("=========================================")
    print("✅ REPLAY FULLY RESET. Ready for fresh stream.")
    print("=========================================")

if __name__ == "__main__":
    reset_replay()
