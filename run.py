import os
import sys
import time
import psutil
import shutil
import subprocess
import webbrowser
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

# --- Define Absolute Paths & Environments ---
ROOT_DIR = os.path.abspath(os.getcwd())
VENV_PYTHON = os.path.join(ROOT_DIR, ".venv", "Scripts", "python.exe")
DASH_VENV_PYTHON = os.path.join(ROOT_DIR, "master_dashboard", ".venv_dash", "Scripts", "python.exe")

NODE_DIR = os.path.join(ROOT_DIR, "tools", "node")
NPM_CACHE = os.path.join(ROOT_DIR, "tools", "npm_cache")

if not os.path.exists(VENV_PYTHON):
    print(f"Warning: {VENV_PYTHON} not found. Using system Python.")
    VENV_PYTHON = sys.executable 

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

# --- Service Maps ---
STREAMLIT_APPS = [
    {"file": r"alerts_service\dashboard_alerts.py", "port": 8501, "name": "Alerts_Dashboard"},
    {"file": r"gold_service\dashboard_gold.py", "port": 8502, "name": "Gold_Dashboard"},
    {"file": r"inference_service\dashboard_inference.py", "port": 8503, "name": "Inference_Dashboard"},
    {"file": r"writer_service\dashboard_ops.py", "port": 8504, "name": "Ops_Dashboard"},
    {"file": r"telemetry_observer\ui.py", "port": 8505, "name": "Observer_UI"}
]

# Format: "Service_Name": ([cmd_array_or_string], is_detached, cwd_path)
SERVICE_MAP = {
    # Dedicated APIs for Master Dashboard
    "api_observer": ([VENV_PYTHON, r"telemetry_observer\api.py"], False, ROOT_DIR),
    "api_alerts": ([VENV_PYTHON, r"alerts_service\api.py"], False, ROOT_DIR),
    "api_gold": ([VENV_PYTHON, r"gold_service\api.py"], False, ROOT_DIR),
    "api_inference": ([VENV_PYTHON, r"inference_service\api.py"], False, ROOT_DIR),
    "api_writer": ([VENV_PYTHON, r"writer_service\api.py"], False, ROOT_DIR),
    
    # Core Processing Engines
    "engine_alerts": ([VENV_PYTHON, r"alerts_service\app.py"], False, ROOT_DIR),
    "engine_gold": ([VENV_PYTHON, r"gold_service\app.py"], False, ROOT_DIR),
    "engine_inference": ([VENV_PYTHON, r"inference_service\start_inference_cluster.py"], False, ROOT_DIR),
    "engine_writer": ([VENV_PYTHON, r"writer_service\src\start_writer_cluster.py"], False, ROOT_DIR),
    
    # Ingest remains detached so you can see the FastAPI logs in a cmd window
    "ingest": (f"{VENV_PYTHON} -m uvicorn ingest.app.main:app --port 8000 --reload", True, ROOT_DIR) 
}

RESET_SCRIPTS = [
    r"tools\reset_alerts_gold.py",
    r"tools\reset_vehicle_health_gold.py",
    r"tools\reset_inference.py",
    r"tools\reset_writer.py",
    r"tools\reset_replay.py",
    r"tools\reset_dashboard_cache.py" # Integrated RAM wipe
]

running_processes = []
open_log_files = []

# --- Execution Helpers ---

def run_background_task(cmd_list, name, cwd_path, wait_time=0):
    print(f"--- Starting {name} (Logs: logs/{name}.log) ---")
    log_file = open(f"logs/{name}.log", "a", encoding="utf-8") 
    open_log_files.append(log_file)
    
    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT_DIR
    
    proc = subprocess.Popen(cmd_list, cwd=cwd_path, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    running_processes.append({"proc": proc, "name": name, "detached": False})
    
    if wait_time > 0:
        time.sleep(wait_time)

def run_detached_console(cmd_str, name, cwd_path, wait_time=0):
    print(f"--- Starting {name} in a new window ---")
    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT_DIR
    
    full_cmd = f'title {name} && {cmd_str}'
    proc = subprocess.Popen(
        ["cmd.exe", "/k", full_cmd], 
        cwd=cwd_path, 
        creationflags=subprocess.CREATE_NEW_CONSOLE, 
        env=env
    )
    
    running_processes.append({"proc": proc, "name": name, "detached": True})
    
    if wait_time > 0:
        time.sleep(wait_time)

# --- Master Dashboard Launchers ---

def launch_master_backend():
    print("--- Starting Master Dashboard Gateway (Port 8005) ---")
    log_file = open(f"logs/Master_Dash_Backend.log", "a", encoding="utf-8")
    open_log_files.append(log_file)
    
    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT_DIR
    # FIXED: Uses main_v2 to act as the Gateway to the 5 isolated APIs
    cmd = [DASH_VENV_PYTHON, "-m", "uvicorn", "backend.main_v2:app", "--port", "8005"]
    
    dash_dir = os.path.join(ROOT_DIR, "master_dashboard")
    proc = subprocess.Popen(cmd, cwd=dash_dir, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    running_processes.append({"proc": proc, "name": "Master_Dash_Backend", "detached": False})

def launch_master_frontend():
    print("--- Starting Master Dashboard Frontend (React/Vite) ---")
    log_file = open(f"logs/Master_Dash_Frontend.log", "a", encoding="utf-8")
    open_log_files.append(log_file)
    
    env = os.environ.copy()
    env["PATH"] = NODE_DIR + os.pathsep + env.get("PATH", "")
    env["npm_config_cache"] = NPM_CACHE
    
    frontend_dir = os.path.join(ROOT_DIR, "master_dashboard", "frontend")
    
    cmd = "npm run dev -- --port 5173 --strictPort"
    proc = subprocess.Popen(cmd, shell=True, cwd=frontend_dir, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    running_processes.append({"proc": proc, "name": "Master_Dash_Frontend", "detached": False})
    
    print("   ⏳ Waiting for Vite to compile (6s)...")
    time.sleep(6)
    print("   🌐 Opening browser at http://localhost:5173")
    webbrowser.open("http://localhost:5173")

# --- Process Management ---

def kill_process(p_info):
    """Smart killer: Uses graceful terminate for APIs, and absolute force for CMD windows."""
    pid = p_info['proc'].pid
    name = p_info['name']
    is_detached = p_info.get('detached', False)

    if is_detached:
        # Windows native taskkill destroys the CMD window and all child JVM/Python processes instantly
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"   ✅ Closed window for {name}.")
    else:
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
            try:
                parent.wait(timeout=3) 
                print(f"   ✅ Closed {name}.")
            except psutil.TimeoutExpired:
                # Fallback to absolute force
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"   ✅ Force-closed {name}.")
                
        except Exception:
            pass

def hunt_and_kill_port(port, name):
    for conn in psutil.net_connections(kind='inet'):
        if conn.laddr.port == port and conn.status == 'LISTEN':
            try:
                p = psutil.Process(conn.pid)
                p.terminate()
                p.wait(timeout=2)
                print(f"   ✅ Force-closed orphaned {name} on port {port}.")
            except Exception:
                pass

def hunt_zombie_replay_workers():
    print("   🔍 Scanning OS for orphaned zombie replay workers...")
    zombies = []
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = p.info['cmdline']
            if cmdline and 'python' in p.info['name'].lower():
                cmd_str = " ".join(cmdline).lower()
                if 'replay' in cmd_str and 'run.py' not in cmd_str:
                    zombies.append(p)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    if zombies:
        print(f"\n⚠️ WARNING: Detected {len(zombies)} orphaned Replay Worker process(es) still running in the background.")
        try:
            ans = input("Do you want to force-kill these zombie workers? (y/n): ").lower()
            if ans == 'y':
                for z in zombies:
                    try:
                        z.terminate()
                        z.wait(timeout=2)
                    except psutil.TimeoutExpired:
                        z.kill()
                    except Exception:
                        pass
                print("   ✅ Zombie replay workers eliminated.")
            else:
                print("   ℹ️ Leaving zombie workers alive.")
        except KeyboardInterrupt:
            print("   ℹ️ Skipping zombie termination due to interrupt.")
    else:
        print("   ✅ No zombie replay workers found. System is clean.")

def restart_service(target):
    if target == "dash_backend":
        internal_name = "Master_Dash_Backend"
    elif target == "dash_frontend":
        internal_name = "Master_Dash_Frontend"
    else:
        internal_name = f"Service_{target}"
        
    target_idx = -1
    for i, p_info in enumerate(running_processes):
        if p_info['name'].lower() == internal_name.lower():
            target_idx = i
            break
            
    if target_idx != -1:
        p_info = running_processes[target_idx]
        print(f"\n[RESTART] Terminating existing {p_info['name']} (PID: {p_info['proc'].pid})...")
        kill_process(p_info)
        running_processes.pop(target_idx)
        time.sleep(2) 
    else:
        print(f"\n[RESTART] {internal_name} background task was not found. Starting fresh.")

    if target == "dash_backend":
        launch_master_backend()
    elif target == "dash_frontend":
        launch_master_frontend()
    else:
        cmd, is_detached, cwd = SERVICE_MAP[target]
        if is_detached:
            run_detached_console(cmd, internal_name, cwd, 2)
        else:
            run_background_task(cmd, internal_name, cwd, 2)
    print(f"[RESTART] {internal_name} is back online.\n")

def cleanup():
    print("\n" + "="*40)
    print("SHUTDOWN SEQUENCE INITIATED")
    print("="*40)
    
    for p_info in reversed(running_processes):
        print(f"Terminating {p_info['name']}...")
        kill_process(p_info)
        
    print("   ✅ All orchestrated processes terminated.")
        
    for f in open_log_files:
        try: f.close()
        except: pass
        
    hunt_and_kill_port(5173, "Node/Vite") 
    hunt_zombie_replay_workers()
            
    print("\nStream offline. All background services and detached windows closed safely.")
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
                hunt_and_kill_port(port, f"Kafka/ZK Port {port}")
            infra_is_running = False
            time.sleep(2)

    reset = input("\nReset stream files and topics (Hard Reset)? (y/n): ").lower()
    if reset == 'y':
        if infra_is_running:
            print("Force closing Kafka/ZK to allow log deletion...")
            for port in [2181, 9092]:
                hunt_and_kill_port(port, f"Kafka/ZK Port {port}")
            time.sleep(2)
            
        print("\n--- Hard Resetting Infrastructure ---")
        
        print("Clearing in-memory API Caches (Ports 8000-8006)...")
        for port in range(8000, 8007):
            hunt_and_kill_port(port, f"API Port {port}")
        time.sleep(2)
        
        shutil.rmtree(KAFKA_LOG_DIR, ignore_errors=True)
        shutil.rmtree(ZK_LOG_DIR, ignore_errors=True)
        
        run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", ROOT_DIR, 20)
        run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", ROOT_DIR, 30)
        
        print("\n--- Recreating Kafka Topics ---")
        for topic in KAFKA_TOPICS:
            cmd = fr"{KAFKA_BIN_DIR}\kafka-topics.bat --create --topic {topic} --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1"
            subprocess.run(cmd, shell=True)

        print("\n--- Resetting Spark/Stream Files ---")
        for script in RESET_SCRIPTS:
            script_path = os.path.join(ROOT_DIR, script)
            subprocess.run([VENV_PYTHON, script_path], input="yes\n", text=True)
            
    else:
        if not infra_is_running:
            print("\n--- Booting Infrastructure ---")
            run_detached_console(r"tools\kafka\start_zookeeper.bat", "Zookeeper", ROOT_DIR, 20)
            run_detached_console(r"tools\kafka\start_kafka.bat", "Kafka", ROOT_DIR, 30)
        else:
            print("\n--- Resuming Existing Infrastructure ---")

    start_master = False
    start_streamlit = False
    
    ans_both = input("\nStart Streamlit AND Master Dashboard? (y/n): ").lower()
    if ans_both == 'y':
        start_master = True
        start_streamlit = True
    else:
        ans_master = input("Start Master Dashboard ONLY? (y/n): ").lower()
        if ans_master == 'y':
            start_master = True
        else:
            ans_streamlit = input("Start Streamlit Dashboards ONLY? (y/n): ").lower()
            if ans_streamlit == 'y':
                start_streamlit = True

    start_serv = input("\nStart Backend Services? (y/n): ").lower()
    if start_serv == 'y':
        for service_key, (cmd, is_detached, cwd) in SERVICE_MAP.items():
            service_name = f"Service_{service_key}"
            if is_detached:
                run_detached_console(cmd, service_name, cwd, 5)
            else:
                run_background_task(cmd, service_name, cwd, 3) 

    if start_master:
        launch_master_backend()
        time.sleep(4)
        launch_master_frontend()
        
    if start_streamlit:
        for app in STREAMLIT_APPS:
            cmd = [VENV_PYTHON, "-m", "streamlit", "run", app["file"], "--server.port", str(app["port"]), "--server.headless", "true"]
            run_background_task(cmd, app['name'], ROOT_DIR)
            time.sleep(5) 
            webbrowser.open(f"http://localhost:{app['port']}")
            time.sleep(5)

    print("\n" + "="*50)
    print("SYSTEM READY.")
    print("Action: Start replay using the Notebook.")
    print("="*50)
    
    restartable_list = list(SERVICE_MAP.keys()) + ["dash_backend", "dash_frontend"]
    
    print("\nINTERACTIVE SERVICE MANAGER")
    print(f"Available services to restart:")
    for key in restartable_list:
        print(f"  - {key}")
    print("\nType a service name and press Enter to restart it.")
    print("Press Ctrl+C at any time to safely shut down the entire emulator.")
    
    while True:
        target = input("\nemulator> ").strip().lower()
        if target in restartable_list:
            restart_service(target)
        elif target:
            print(f"Unknown service '{target}'. Please choose from the list above.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[Ctrl+C Detected: Interrupting sequence safely...]")
    except Exception as e:
        print(f"\n[Unexpected Error: {e}]")
    finally:
        try:
            cleanup()
        except KeyboardInterrupt:
            pass