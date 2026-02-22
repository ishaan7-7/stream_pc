
import os
import shutil

def reset_inference():
    print("=========================================")
    print("🧹 RESETTING INFERENCE SERVICE")
    print("=========================================")

    # Define paths relative to the tools directory
    tools_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(tools_dir)
    
    silver_dir = os.path.join(root_dir, "data", "delta", "silver")
    state_dir = os.path.join(root_dir, "inference_service", "state")

    # 1. Delete Silver Delta Tables
    if os.path.exists(silver_dir):
        print(f"🗑️ Deleting Silver Delta Tables: {silver_dir}")
        shutil.rmtree(silver_dir)
        print("   ✅ Silver tables removed.")
    else:
        print("   ✅ Silver tables already empty.")

    # 2. Delete State Files
    # 2. Delete All Module State Files
    import glob
    if os.path.exists(state_dir):
        files = glob.glob(os.path.join(state_dir, "*"))
        for f in files:
            try:
                os.remove(f)
                print(f"🗑️ Deleted state file: {os.path.basename(f)}")
            except Exception as e:
                print(f"⚠️ Could not delete {f}: {e}")

    print("=========================================")
    print("✅ INFERENCE FULLY RESET. Ready for fresh stream.")
    print("=========================================")

if __name__ == "__main__":
    reset_inference()
