import os
import sys
import time
import psutil
import shutil
import subprocess
import webbrowser

# Force UTF-8 and Unbuffered output so logs write instantly and cleanly
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1" 

VENV_PYTHON = r".venv\Scripts\python.exe"

if not os.path.exists(VENV_PYTHON):
    print(f"Warning: {VENV_PYTHON} not found. Using system Python.")
    VENV_PYTHON = sys.executable 

# --- KAFKA RESET CONFIGURATION ---
# Default Windows paths for Kafka/Zookeeper data. 
# Change these if your server.properties points somewhere else!
KAFKA_LOG_DIR = r"C:\tmp\kafka-logs"
ZK_LOG_DIR = r"C:\tmp\zookeeper"
KAFKA_TOPICS = ["battery", "body", "engine", "transmission", "tyre"]

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
    """Runs a task silently, forcing immediate unbuffered logs and mapping the PYTHONPATH."""
    print(f"--- Starting {name} (Logs: logs/{name}.log) ---")
    log_file = open(f"logs/{name}.log", "w", encoding="utf-8")
    open_log_files.append(log_file)
    
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd() # Forces Uvicorn to recognize your project structure
    
    proc = subprocess.Popen(cmd, shell=True, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    running_processes.append({"proc": proc, "name": name})
    
    if wait_time > 0:
        time.sleep(wait_time)

def run_detached_console(cmd, name, wait_time=0):
    print(f"--- Starting {name} in a new window ---")
    proc = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    running_processes.append({"proc": proc, "name": name})
    if wait_time > 0:
        time.sleep(wait_time)

def kill_process_tree(pid, name):
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
        print(f"Successfully killed {name}.")
    except Exception:
        pass

def cleanup():
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
    # 1. Unconditional Pre-emptive Cleanup
    print("\nEnsuring a clean state: Checking for ghost Kafka/Zookeeper processes...")
    for port in [2181, 9092]:
        for conn in psutil.net_connections():
            if conn.laddr.port == port and conn.status == 'LISTEN':
                print(f"Force stopping active process on port {port}...")
                kill_process_tree(conn.pid, f"Port {port}")
    time.sleep(2) # Brief cooldown to let Windows release the ports

    # 2. Reset Logic Branching
    reset = input("\nReset stream files and topics? (y/n): ").lower()
    
    if reset == 'y':
        print("\n--- Hard Resetting Infrastructure ---")
        print(f"Deleting physical Kafka logs from {KAFKA_LOG_DIR}...")
        shutil.rmtree(KAFKA_LOG_DIR, ignore_errors=True)
        shutil.rmtree(ZK_LOG_DIR, ignore_errors=True)
        
        run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", 20)
        run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", 30)
        
        print("\n--- Recreating Kafka Topics ---")
        for topic in KAFKA_TOPICS:
            print(f"Creating topic: {topic}")
            cmd = f"kafka-topics.bat --create --topic {topic} --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
            subprocess.run(cmd, shell=True)
            
        print("\n--- Resetting Spark Delta Files ---")
        for script in RESET_SCRIPTS:
            print(f"Resetting {script}...")
            subprocess.run(f'"{VENV_PYTHON}" {script}', shell=True, input="yes\n", text=True)
        print("Stream reset success.")
        
    else:
        print("\n--- Resuming Existing Infrastructure ---")
        run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", 20)
        run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", 30)

    # 3. Streamlit Apps
    start_ui = input("\nStart Streamlit Dashboards? (y/n): ").lower()
    if start_ui == 'y':
        for app in STREAMLIT_APPS:
            cmd = f'"{VENV_PYTHON}" -m streamlit run {app["file"]} --server.port {app["port"]} --server.headless true'
            run_background_task(cmd, app['name'])
            
            print(f"Waiting 5s for {app['name']} server to bind...")
            time.sleep(5) 
            webbrowser.open(f"http://localhost:{app['port']}")
            time.sleep(15) 

    # 4. Services
    start_serv = input("\nStart Services? (y/n): ").lower()
    if start_serv == 'y':
        for service in SERVICES:
            name = service.split('\\')[-1].replace('.py', '')
            run_background_task(f'"{VENV_PYTHON}" {service}', f"Service_{name}", 20)
            
        # 5. The Ingest Fix
        run_background_task(f'"{VENV_PYTHON}" -m uvicorn ingest.app.main:app --host 0.0.0.0 --port 8000', "Service_Ingest", 5)
        
        print("\n" + "="*40)
        print("ALL SERVICES ACTIVE IN BACKGROUND")
        print("Action: Start replay using the Notebook.")
        print("Press Ctrl+C in THIS terminal to safely shut everything down.")
        print("="*40)

if __name__ == "__main__":
    try:
        main()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()