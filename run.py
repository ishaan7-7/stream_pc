import os
import sys
import time
import signal
import psutil
import subprocess
import webbrowser
from typing import List, Dict

# Configuration
VENV_PATH = r".venv\Scripts\activate.bat"
STREAMLIT_APPS = [
    {"file": r"alerts_service\dashboard_alerts.py", "port": 8501, "name": "Alerts Dashboard"},
    {"file": r"gold_service\dashboard_gold.py", "port": 8502, "name": "Gold Dashboard"},
    {"file": r"inference_service\dashboard_inference.py", "port": 8503, "name": "Inference Dashboard"},
    {"file": r"writer_service\dashboard_ops.py", "port": 8504, "name": "Ops Dashboard"},
]
OBSERVER_BAT = "start_observer.bat"

SERVICES = [
    r"alerts_service\app.py",
    r"gold_service\app.py",
    r"inference_service\start_inference_cluster.py",
    r"writer_service\src\start_writer_cluster.py"
]
INGEST_BAT = r"ingest\start_ingest.bat"

RESET_SCRIPTS = [
    r"tools\reset_alerts_gold.py",
    r"tools\reset_vehicle_health_gold.py",
    r"tools\reset_inference.py",
    r"tools\reset_writer.py",
    r"tools\reset_replay.py"
]

running_processes = []

def kill_port_owner(port):
    for conn in psutil.net_connections():
        if conn.laddr.port == port and conn.status == 'LISTEN':
            try:
                proc = psutil.Process(conn.pid)
                print(f"Cleaning port {port} (PID: {conn.pid})...")
                proc.terminate()
                proc.wait(timeout=5)
            except:
                pass

def run_command(cmd, wait_time=0, name="Process"):
    print(f"--- Starting {name} ---")
    # Using CREATE_NEW_PROCESS_GROUP for graceful Windows shutdown later
    proc = subprocess.Popen(
        cmd, 
        shell=True, 
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
    )
    running_processes.append(proc)
    if wait_time > 0:
        time.sleep(wait_time)
    return proc

def cleanup(sig=None, frame=None):
    print("\n\n--- Shutdown Initiated ---")
    ans = input("Have you stopped the replay worker from the notebook? (y/n): ")
    
    # Reverse order teardown
    for proc in reversed(running_processes):
        try:
            # Send CTRL_BREAK_EVENT to the process group (Windows specific)
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
            print(f"Terminated process {proc.pid}")
        except:
            proc.kill()
            
    print("Stream offline. Infrastructure safely closed.")
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)

def main():
    # 1. Venv Check
    activate_venv = input("Activate .venv? (y/n): ").lower()
    if activate_venv == 'y':
        if not os.path.exists(VENV_PATH):
            print("Error: .venv not found!")
            return
        print("Venv Ready.")

    # 2. Infrastructure Check
    for port in [2181, 9092]:
        if any(c.laddr.port == port for c in psutil.net_connections() if c.status == 'LISTEN'):
            kill = input(f"Port {port} (Zookeeper/Kafka) is busy. Kill sessions? (y/n): ")
            if kill.lower() == 'y':
                kill_port_owner(port)

    # 3. Reset Stream
    reset = input("Reset stream? (y/n): ").lower()
    if reset == 'y':
        # Start Infra
        run_command(r"tools\kafka\start_zookeeper.bat", 20, "Zookeeper")
        run_command(r"tools\kafka\start_kafka.bat", 30, "Kafka")
        
        # Reset Files in specified order (Alerts -> Replay)
        for script in RESET_SCRIPTS:
            print(f"Resetting {script}...")
            subprocess.run(f"python {script}", shell=True)
        
        # Topic check logic would go here via kafka-topics.bat
        print("Stream reset success.")

    # 4. Streamlit Apps
    start_ui = input("Start Streamlit Dashboards? (y/n): ").lower()
    if start_ui == 'y':
        for app in STREAMLIT_APPS:
            # Headless run + automated browser open
            cmd = f"streamlit run {app['file']} --server.port {app['port']} --server.headless true"
            run_command(cmd, 2, app['name'])
            webbrowser.open(f"http://localhost:{app['port']}")
            time.sleep(18) # Net 20s cooldown
        
        run_command(OBSERVER_BAT, 20, "Observer")

    # 5. Services
    start_serv = input("Start Services? (y/n): ").lower()
    if start_serv == 'y':
        for service in SERVICES:
            run_command(f"python {service}", 20, service)
        run_command(INGEST_BAT, 5, "Ingest Service")
        
        print("\n" + "="*40)
        print("ALL SERVICES ACTIVE")
        print("Action: Start replay using the Notebook.")
        print("Press Ctrl+C to stop all services safely.")
        print("="*40)

    # Keep main alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()