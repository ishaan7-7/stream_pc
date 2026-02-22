# File: C:\streaming_emulator\writer_service\start_writer_cluster.py
import subprocess
import time
import sys
import os

MODULES = ["engine", "body", "battery", "transmission", "tyre"]
PROCESSES = []

def start_writers():
    print(f"🚀 Launching 5 Parallel Writers on {sys.platform}...")
    print(f"   Config: 5 Processes x local[2] threads = 10 threads total.")
    
    python_exe = sys.executable
    script_path = "writer_service/src/stream_processor.py"
    
    if not os.path.exists(script_path):
        print(f"❌ Error: Not found: {script_path}")
        return

    for module in MODULES:
        print(f"   [+] Spawning worker for: {module}")
        # Opens a new standalone process for this module
        p = subprocess.Popen([python_exe, script_path, module])
        PROCESSES.append(p)
        # Wait 3s between starts to let JVMs initialize smoothly without spiking CPU
        time.sleep(3) 

    print("\n✅ Cluster Active. Monitor throughput in separate terminal.")
    print("   Press Ctrl+C here to stop all writers.")
    
    try:
        while True:
            time.sleep(1)
            for i, p in enumerate(PROCESSES):
                if p.poll() is not None:
                    print(f"⚠️ Worker {MODULES[i]} crashed!")
    except KeyboardInterrupt:
        print("\n🛑 Stopping Cluster...")
        for p in PROCESSES:
            p.terminate()

if __name__ == "__main__":
    start_writers()