import os
import sys
import time
import psutil
import subprocess
import webbrowser

# 1. Hardcode the VENV Python executable to avoid environment path issues
VENV_PYTHON = r".venv\Scripts\python.exe"

# Fallback in case the user didn't create the folder exactly as `.venv`
if not os.path.exists(VENV_PYTHON):
    print(f"Warning: {VENV_PYTHON} not found. Using system Python.")
    VENV_PYTHON = sys.executable 

STREAMLIT_APPS = [
    {"file": r"alerts_service\dashboard_alerts.py", "port": 8501, "name": "Alerts Dashboard"},
    {"file": r"gold_service\dashboard_gold.py", "port": 8502, "name": "Gold Dashboard"},
    {"file": r"inference_service\dashboard_inference.py", "port": 8503, "name": "Inference Dashboard"},
    {"file": r"writer_service\dashboard_ops.py", "port": 8504, "name": "Ops Dashboard"},
    {"file": r"telemetry_observer\ui.py", "port": 8505, "name": "Observer UI"} # Replaced the .bat file
]

SERVICES = [
    r"telemetry_observer\observer_backend.py", # Extracted from the .bat file
    r"alerts_service\app.py",
    r"gold_service\app.py",
    r"inference_service\start_inference_cluster.py",
    r"writer_service\src\start_writer_cluster.py"
]

RESET_SCRIPTS = [
    r"tools\reset_alerts_gold.py",
    r"tools\reset_vehicle_health_gold.py",
    r"tools\reset_inference.py",
    r"tools\reset_writer.py",
    r"tools\reset_replay.py"
]

running_processes = []

def run_command(cmd, wait_time=0, name="Process"):
    print(f"--- Starting {name} ---")
    # No CREATE_NEW_PROCESS_GROUP here. We want them bound to this console.
    proc = subprocess.Popen(cmd, shell=True)
    running_processes.append({"proc": proc, "name": name})
    if wait_time > 0:
        time.sleep(wait_time)
    return proc

def kill_process_tree(pid, name):
    """Safely kills a process and all its children (e.g. cmd.exe -> java.exe)"""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        
        # Friendly terminate
        for child in children:
            try: child.terminate()
            except: pass
            
        _, alive = psutil.wait_procs(children, timeout=3)
        
        # Force kill if still alive
        for child in alive:
            try: child.kill()
            except: pass
            
        parent.terminate()
        parent.wait(timeout=3)
        print(f"Successfully closed {name}.")
    except psutil.NoSuchProcess:
        pass
    except Exception as e:
        print(f"Force closed {name} with minor errors.")

def cleanup():
    print("\n" + "="*40)
    print("SHUTDOWN SEQUENCE INITIATED")
    print("="*40)
    
    ans = input("Have you stopped the replay worker from the notebook? (y/n): ")
    
    # Reverse order teardown: Ingest -> Services -> UI -> Kafka -> ZK
    for p_info in reversed(running_processes):
        print(f"Terminating {p_info['name']}...")
        kill_process_tree(p_info['proc'].pid, p_info['name'])
            
    print("\nStream offline. Infrastructure safely closed.")
    sys.exit(0)

def main():
    # 1. Infrastructure Check
    for port in [2181, 9092]:
        if any(c.laddr.port == port for c in psutil.net_connections() if c.status == 'LISTEN'):
            kill = input(f"Port {port} (Zookeeper/Kafka) is busy. We must kill it to proceed. Kill? (y/n): ")
            if kill.lower() == 'y':
                for conn in psutil.net_connections():
                    if conn.laddr.port == port and conn.status == 'LISTEN':
                        kill_process_tree(conn.pid, f"Port {port}")

    # 2. Reset Stream
    reset = input("\nReset stream? (y/n): ").lower()
    if reset == 'y':
        run_command(r"tools\kafka\start_zookeeper.bat", 20, "Zookeeper")
        run_command(r"tools\kafka\start_kafka.bat", 30, "Kafka")
        
        for script in RESET_SCRIPTS:
            print(f"Resetting {script}...")
            # Using absolute python path to ensure venv is respected
            subprocess.run(f'"{VENV_PYTHON}" {script}', shell=True)
            
        print("Stream reset success.")

    # 3. Streamlit Apps
    start_ui = input("\nStart Streamlit Dashboards? (y/n): ").lower()
    if start_ui == 'y':
        for app in STREAMLIT_APPS:
            # -m streamlit run ensures it uses the exact streamlit installed in the venv
            cmd = f'"{VENV_PYTHON}" -m streamlit run {app["file"]} --server.port {app["port"]} --server.headless true'
            run_command(cmd, 0, app['name'])
            
            # Wait 5 seconds for the server to bind to the port before opening the browser
            print(f"Waiting for {app['name']} server to boot...")
            time.sleep(5) 
            webbrowser.open(f"http://localhost:{app['port']}")
            time.sleep(15) # Remaining cool-down

    # 4. Services
    start_serv = input("\nStart Services? (y/n): ").lower()
    if start_serv == 'y':
        for service in SERVICES:
            run_command(f'"{VENV_PYTHON}" {service}', 20, service)
            
        run_command(r"ingest\start_ingest.bat", 5, "Ingest Service")
        
        print("\n" + "="*40)
        print("ALL SERVICES ACTIVE")
        print("Action: Start replay using the Notebook.")
        print("Press Ctrl+C inside this terminal to safely shut everything down.")
        print("="*40)

if __name__ == "__main__":
    try:
        main()
        # Keep the main thread alive to catch the Ctrl+C
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # This catches the Ctrl+C cleanly
        cleanup()