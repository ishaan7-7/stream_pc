import os
import sys
import time
import psutil
import shutil
import subprocess
import webbrowser

os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

VENV_SCRIPTS = os.path.abspath(os.path.join(".venv", "Scripts"))
VENV_PYTHON = os.path.join(VENV_SCRIPTS, "python.exe")

if not os.path.exists(VENV_PYTHON):
    print(f"Warning: {VENV_PYTHON} not found. Using system Python.")
    VENV_PYTHON = sys.executable 
else:
    os.environ["PATH"] = VENV_SCRIPTS + os.pathsep + os.environ.get("PATH", "")

KAFKA_BIN_DIR = r"C:\kafka\bin\windows"
KAFKA_LOG_DIR = r"C:\tmp\kafka-logs"
ZK_LOG_DIR = r"C:\tmp\zookeeper"

KAFKA_TOPICS = [
    "telemetry.battery", 
    "telemetry.body", 
    "telemetry.engine", 
    "telemetry.transmission", 
    "telemetry.tyre"
]

os.makedirs("logs", exist_ok=True)

STREAMLIT_APPS = [
    {"file": r"alerts_service\dashboard_alerts.py", "port": 8501, "name": "Alerts_Dashboard"},
    {"file": r"gold_service\dashboard_gold.py", "port": 8502, "name": "Gold_Dashboard"},
    {"file": r"inference_service\dashboard_inference.py", "port": 8503, "name": "Inference_Dashboard"},
    {"file": r"writer_service\dashboard_ops.py", "port": 8504, "name": "Ops_Dashboard"},
    {"file": r"telemetry_observer\ui.py", "port": 8505, "name": "Observer_UI"}
]

SERVICE_MAP = {
    "telemetry_observer": (r"python telemetry_observer\observer_backend.py", False),
    "alerts": (r"python alerts_service\app.py", False),
    "gold": (r"python gold_service\app.py", False),
    "inference": (r"python inference_service\start_inference_cluster.py", False),
    "writer": (r"python writer_service\src\start_writer_cluster.py", False),
    "ingest": (r"python -m uvicorn ingest.app.main:app --port 8000 --reload", True) 
}

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
    print(f"--- Starting {name} (Logs: logs/{name}.log) ---")
    log_file = open(f"logs/{name}.log", "a", encoding="utf-8") 
    open_log_files.append(log_file)
    
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    
    proc = subprocess.Popen(cmd, shell=True, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    running_processes.append({"proc": proc, "name": name})
    
    if wait_time > 0:
        time.sleep(wait_time)

def run_detached_console(cmd, name, wait_time=0):
    print(f"--- Starting {name} in a new window ---")
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    
    full_cmd = f'title {name} && {cmd}'
    proc = subprocess.Popen(["cmd.exe", "/k", full_cmd], creationflags=subprocess.CREATE_NEW_CONSOLE, env=env)
    
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
        print(f"Successfully closed {name}.")
    except Exception:
        pass

def restart_service(service_key):
    service_name = f"Service_{service_key}"
    
    target_idx = -1
    for i, p_info in enumerate(running_processes):
        if p_info['name'].lower() == service_name.lower():
            target_idx = i
            break
            
    if target_idx != -1:
        p_info = running_processes[target_idx]
        print(f"\n[RESTART] Terminating existing {p_info['name']} (PID: {p_info['proc'].pid})...")
        kill_process_tree(p_info['proc'].pid, p_info['name'])
        running_processes.pop(target_idx)
        time.sleep(2) 
    else:
        print(f"\n[RESTART] {service_name} was not running. Starting fresh.")

    cmd, is_detached = SERVICE_MAP[service_key]
    if is_detached:
        run_detached_console(cmd, service_name, 2)
    else:
        run_background_task(cmd, service_name, 2)
    print(f"[RESTART] {service_name} is back online.\n")

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
    infra_is_running = False

    print("\nChecking Infrastructure State...")
    for port in [2181, 9092]:
        for conn in psutil.net_connections():
            if conn.laddr.port == port and conn.status == 'LISTEN':
                infra_is_running = True
                
    if infra_is_running:
        kill = input("Zookeeper/Kafka are already running. Kill them? (y/n): ")
        if kill.lower() == 'y':
            for port in [2181, 9092]:
                for conn in psutil.net_connections():
                    if conn.laddr.port == port and conn.status == 'LISTEN':
                        kill_process_tree(conn.pid, f"Port {port}")
            infra_is_running = False
            time.sleep(2)

    reset = input("\nReset stream files and topics (Hard Reset)? (y/n): ").lower()
    if reset == 'y':
        if infra_is_running:
            print("Force closing Kafka/ZK to allow log deletion...")
            for port in [2181, 9092]:
                for conn in psutil.net_connections():
                    if conn.laddr.port == port and conn.status == 'LISTEN':
                        kill_process_tree(conn.pid, f"Port {port}")
            time.sleep(2)
            
        print("\n--- Hard Resetting Infrastructure ---")
        shutil.rmtree(KAFKA_LOG_DIR, ignore_errors=True)
        shutil.rmtree(ZK_LOG_DIR, ignore_errors=True)
        
        run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", 20)
        run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", 30)
        
        print("\n--- Recreating Kafka Topics ---")
        for topic in KAFKA_TOPICS:
            cmd = fr"{KAFKA_BIN_DIR}\kafka-topics.bat --create --topic {topic} --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1"
            subprocess.run(cmd, shell=True)

        print("\n--- Resetting Spark/Stream Files ---")
        for script in RESET_SCRIPTS:
            subprocess.run(f'python {script}', shell=True, input="yes\n", text=True)
            
    else:
        if not infra_is_running:
            print("\n--- Booting Infrastructure ---")
            run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", 20)
            run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", 30)
        else:
            print("\n--- Resuming Existing Infrastructure ---")

    start_ui = input("\nStart Streamlit Dashboards? (y/n): ").lower()
    if start_ui == 'y':
        for app in STREAMLIT_APPS:
            cmd = f'streamlit run {app["file"]} --server.port {app["port"]} --server.headless true'
            run_background_task(cmd, app['name'])
            time.sleep(5) 
            webbrowser.open(f"http://localhost:{app['port']}")
            time.sleep(15) 

    start_serv = input("\nStart Services? (y/n): ").lower()
    if start_serv == 'y':
        for service_key, (cmd, is_detached) in SERVICE_MAP.items():
            service_name = f"Service_{service_key}"
            if is_detached:
                run_detached_console(cmd, service_name, 5)
            else:
                run_background_task(cmd, service_name, 10)

    print("\n" + "="*50)
    print("ALL SERVICES ACTIVE.")
    print("Action: Start replay using the Notebook.")
    print("="*50)
    
    print("\nINTERACTIVE SERVICE MANAGER")
    print(f"Available services to restart: {list(SERVICE_MAP.keys())}")
    print("Type a service name and press Enter to restart it.")
    print("Press Ctrl+C at any time to safely shut down the entire emulator.")
    
    while True:
        try:
            target = input("\nemulator> ").strip().lower()
            if target in SERVICE_MAP:
                restart_service(target)
            elif target:
                print(f"Unknown service '{target}'. Valid options: {list(SERVICE_MAP.keys())}")
        except KeyboardInterrupt:
            # Catch the Ctrl+C here to break the input loop and trigger cleanup naturally
            break

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass # Caught by the inner loop or general execution, proceed to cleanup
    finally:
        cleanup()