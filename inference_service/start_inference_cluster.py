
import subprocess
import sys
import time
from src import config
import os

def start_cluster():
    print("=========================================")
    print("🚀 LAUNCHING INFERENCE CLUSTER")
    print("=========================================")
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    app_script = os.path.join(base_dir, "app.py")
    
    processes = []
    
    for mod in config.MODULES:
        print(f"Starting background process for: {mod.upper()}")
        # FIXED: Run subprocess directly without 'start cmd /k'
        # This keeps all logging in the current terminal window
        p = subprocess.Popen([sys.executable, app_script, mod])
        processes.append(p)
        time.sleep(1.5) # Stagger startup heavily to prevent disk lock overhead
        
    print("\n✅ All inference processes launched.")
    print("They are running in parallel in this window. Press Ctrl+C to stop all.\n")
    
    try:
        # Keep the main process alive to capture output
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\n🛑 Stopping Inference Cluster...")
        for p in processes:
            p.terminate()
        print("✅ All processes stopped safely.")

if __name__ == "__main__":
    start_cluster()
