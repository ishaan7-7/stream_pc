import os
import sys
import time
import psutil
import subprocess
import webbrowser

# Force UTF-8 encoding for all child processes to prevent Windows charmap crashes
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUTF8"] = "1"

VENV_PYTHON = r".venv\Scripts\python.exe"

if not os.path.exists(VENV_PYTHON):
    print(f"Warning: {VENV_PYTHON} not found. Using system Python.")
    VENV_PYTHON = sys.executable 

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

STREAMLIT_APPS = [
    {"file": r"alerts_service\dashboard_alerts.py", "port": 8501, "name": "Alerts_Dashboard"},
    {"file": r"gold_service\dashboard_gold.py", "port": 8502, "name": "Gold_Dashboard"},
    {"file": r"inference_service\dashboard_inference.py", "port": 8503, "name": "Inference_Dashboard"},
    {"file": r"writer_service\dashboard_ops.py", "port": 8504, "name": "Ops_Dashboard"},
    {"file": r"telemetry_observer\ui.py", "port": 8505, "name": "Observer_UI"}
]

SERVICES = [
    r"telemetry_observer\observer_backend.py",
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
open_log_files = []

def run_background_task(cmd, name, wait_time=0):
    """Runs a task silently in the background, piping output to a UTF-8 log file."""
    print(f"--- Starting {name} (Logs: logs/{name}.log) ---")
    log_file = open(f"logs/{name}.log", "w", encoding="utf-8")
    open_log_files.append(log_file)
    
    proc = subprocess.Popen(cmd, shell=True, stdout=log_file, stderr=subprocess.STDOUT)
    running_processes.append({"proc": proc, "name": name})
    
    if wait_time > 0:
        time.sleep(wait_time)

def run_detached_console(cmd, name, wait_time=0):
    """Opens a separate CMD window for visual monitoring (Kafka/Zookeeper)."""
    print(f"--- Starting {name} in a new window ---")
    proc = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    running_processes.append({"proc": proc, "name": name})
    
    if wait_time > 0:
        time.sleep(wait_time)

def kill_process_tree(pid, name):
    """Safely kills a process and all its children to prevent memory leaks."""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        
        for child in children:
            try: child.terminate()
            except: pass
            
        _, alive = psutil.wait_procs(children, timeout=3)
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
    """Handles the Ctrl+C graceful shutdown sequence."""
    print("\n" + "="*40)
    print("SHUTDOWN SEQUENCE INITIATED")
    print("="*40)
    
    input("Have you stopped the replay worker from the notebook? (Press Enter to confirm): ")
    
    for p_info in reversed(running_processes):
        print(f"Terminating {p_info['name']}...")
        kill_process_tree(p_info['proc'].pid, p_info['name'])
        
    for f in open_log_files:
        try: f.close()
        except: pass
            
    print("\nStream offline. All detached windows and background services closed.")
    sys.exit(0)

def main():
    infra_needs_start = True

    # 1. Infrastructure Check
    for port in [2181, 9092]:
        if any(c.laddr.port == port for c in psutil.net_connections() if c.status == 'LISTEN'):
            kill = input(f"Port {port} (Zookeeper/Kafka) is busy. Kill previous session? (y/n): ")
            if kill.lower() == 'y':
                for conn in psutil.net_connections():
                    if conn.laddr.port == port and conn.status == 'LISTEN':
                        kill_process_tree(conn.pid, f"Port {port}")
            else:
                print(f"Leaving process on port {port} running.")
                infra_needs_start = False

    # 2. Start Infrastructure (Independent of Reset logic)
    if infra_needs_start:
        print("\nStarting core infrastructure...")
        run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", 20)
        run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", 30)
    else:
        print("\nSkipping Zookeeper/Kafka boot (already running).")

    # 3. Reset Stream
    reset = input("\nReset stream files and topics? (y/n): ").lower()
    if reset == 'y':
        for script in RESET_SCRIPTS:
            print(f"Resetting {script}...")
            # Automatically feed "yes" + Enter to bypass any prompts
            subprocess.run(f'"{VENV_PYTHON}" {script}', shell=True, input="yes\n", text=True)
            
        print("Stream reset success.")

    # 4. Streamlit Apps
    start_ui = input("\nStart Streamlit Dashboards? (y/n): ").lower()
    if start_ui == 'y':
        for app in STREAMLIT_APPS:
            cmd = f'"{VENV_PYTHON}" -m streamlit run {app["file"]} --server.port {app["port"]} --server.headless true'
            run_background_task(cmd, app['name'])
            
            print(f"Waiting 5s for {app['name']} server to bind...")
            time.sleep(5) 
            webbrowser.open(f"http://localhost:{app['port']}")
            time.sleep(15) # Cooldown before starting the next heavy UI process

    # 5. Services
    start_serv = input("\nStart Services? (y/n): ").lower()
    if start_serv == 'y':
        for service in SERVICES:
            name = service.split('\\')[-1].replace('.py', '')
            run_background_task(f'"{VENV_PYTHON}" {service}', f"Service_{name}", 20)
            
        # Use Uvicorn directly from the venv to host the FastAPI ingest gateway
        run_background_task(f'"{VENV_PYTHON}" -m uvicorn ingest.app.main:app --host 0.0.0.0 --port 8000', "Service_Ingest", 5)
        
        print("\n" + "="*40)
        print("ALL SERVICES ACTIVE IN BACKGROUND")
        print("Action: Start replay using the Notebook.")
        print("Press Ctrl+C in THIS terminal to safely shut everything down.")
        print("="*40)

if __name__ == "__main__":
    try:
        main()
        # Keep main thread alive to catch KeyboardInterrupt gracefully
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()