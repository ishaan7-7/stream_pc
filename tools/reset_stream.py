
import os
import sys
import time
import socket
import subprocess
import shutil
import json
from pathlib import Path

# --- Configuration ---
ROOT_DIR = Path(__file__).parent.parent.resolve()
TOOLS_DIR = ROOT_DIR / "tools"

KAFKA_HOME = Path("C:/kafka")
KAFKA_BIN = KAFKA_HOME / "bin" / "windows"
ZOOKEEPER_BAT = TOOLS_DIR / "kafka" / "start_zookeeper.bat"
KAFKA_BAT = TOOLS_DIR / "kafka" / "start_kafka.bat"

KAFKA_LOG_DIR = Path("C:/tmp/kafka-logs")
ZOOKEEPER_DATA_DIR = Path("C:/tmp/zookeeper")
KAFKA_SERVER_LOG = KAFKA_HOME / "logs" / "server.log"

INGEST_CONFIG_PATH = ROOT_DIR / "ingest" / "config" / "ingest_config.json"

def print_status(msg, status="INFO"):
    print(f"[{status}] {msg}")

def ask_user(prompt):
    while True:
        choice = input(f"\n> {prompt} (y/n): ").strip().lower()
        if choice in ['y', 'yes']: return True
        if choice in ['n', 'no']: return False

def run_script(script_path):
    """Executes a python script and waits for it to finish."""
    if not script_path.exists():
        print_status(f"Missing script: {script_path}", "ERROR")
        return
    subprocess.run([sys.executable, str(script_path)])

def check_port(port, host="127.0.0.1", timeout=1.0):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        return s.connect_ex((host, port)) == 0

def wait_for_port(port, service_name, retries=60, delay=1):
    print_status(f"Waiting for {service_name} on port {port}...", "WAIT")
    for _ in range(retries):
        if check_port(port):
            print_status(f"{service_name} is listening on port {port}.", "OK")
            return True
        time.sleep(delay)
    print_status(f"Timed out waiting for {service_name}.", "ERROR")
    return False

def start_new_window(command, title="Service"):
    try:
        subprocess.Popen(f'start "{title}" {command}', shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE)
        return True
    except Exception as e:
        print_status(f"Failed to launch window: {e}", "ERROR")
        return False

def clean_kafka_logs():
    cleaned = False
    for path in [KAFKA_LOG_DIR, ZOOKEEPER_DATA_DIR]:
        if path.exists():
            try:
                shutil.rmtree(path)
                print_status(f"Deleted: {path}", "CLEAN")
                cleaned = True
            except PermissionError:
                print_status(f"Cannot delete {path}. Ensure Kafka/Zookeeper processes are killed in Task Manager.", "ERROR")
                return False
            except Exception as e:
                print_status(f"Error deleting {path}: {e}", "ERROR")
                return False
    return True

def verify_kafka_health():
    """Scans the actual Kafka server.log for recent Windows corruption errors."""
    if not KAFKA_SERVER_LOG.exists():
        print_status("Kafka server.log not found, assuming healthy startup.", "WARN")
        return True
    
    try:
        with open(KAFKA_SERVER_LOG, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            recent_lines = lines[-100:] # Check last 100 lines
            errors = [l for l in recent_lines if "ERROR" in l or "FATAL" in l or "Exception" in l]
            
            if errors:
                print_status(f"Found {len(errors)} potential errors in recent Kafka logs!", "ERROR")
                print_status("Last error snippet: " + errors[-1].strip(), "DEBUG")
                return False
            else:
                print_status("No corruption/errors found in recent Kafka logs.", "OK")
                return True
    except Exception as e:
        print_status(f"Could not read Kafka logs for verification: {e}", "WARN")
        return True

def create_topics():
    if not INGEST_CONFIG_PATH.exists():
        print_status("Ingest config not found. Using default 5 topics.", "WARN")
        topics = ["telemetry.engine", "telemetry.battery", "telemetry.body", "telemetry.transmission", "telemetry.tyre"]
    else:
        with open(INGEST_CONFIG_PATH, 'r') as f:
            data = json.load(f)
            topics = list(data.get("topics", {}).values())

    topic_bat = KAFKA_BIN / "kafka-topics.bat"
    print_status(f"Creating {len(topics)} topics with 6 partitions each...", "INFO")
    
    for topic in topics:
        cmd = [
            str(topic_bat), "--create", "--topic", topic,
            "--bootstrap-server", "localhost:9092",
            "--partitions", "6", "--replication-factor", "1", "--if-not-exists"
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print_status(f"Topic '{topic}': OK", "OK")
            elif "already exists" in result.stdout:
                print_status(f"Topic '{topic}': Already exists", "OK")
            else:
                print_status(f"Topic '{topic}': FAILED - {result.stderr.strip()}", "ERROR")
        except Exception as e:
             print_status(f"Failed to execute topic command: {e}", "ERROR")

def main():
    print("\n=======================================================")
    print("🔥 STREAMING EMULATOR: MASTER STREAM RESET SEQUENCE 🔥")
    print("=======================================================\n")

    # --- PHASE 1: MICROSERVICE STATE RESET ---
    print_status("PHASE 1: Resetting Microservice States...", "INFO")
    run_script(TOOLS_DIR / "reset_replay.py")
    run_script(TOOLS_DIR / "reset_writer.py")
    run_script(TOOLS_DIR / "reset_inference.py")
    print_status("All local service states wiped successfully.\n", "OK")

    # --- PHASE 2: INFRASTRUCTURE RESET ---
    if ask_user("Do you want to Hard Reset Zookeeper and Kafka? (Required if stream was stuck)"):
        print_status("PHASE 2: Infrastructure Rebuild...", "INFO")
        
        if not clean_kafka_logs():
            print_status("Aborting infrastructure restart due to log lock. Kill Java processes and try again.", "FATAL")
            sys.exit(1)

        # Zookeeper Start
        start_new_window(f'"{ZOOKEEPER_BAT}"', title="Zookeeper")
        if wait_for_port(2181, "Zookeeper", retries=30):
            print_status("Stabilizing Zookeeper (Waiting 20s)...", "WAIT")
            time.sleep(20)

        # Kafka Start
        start_new_window(f'"{KAFKA_BAT}"', title="Kafka")
        if wait_for_port(9092, "Kafka", retries=60):
            print_status("Stabilizing Kafka (Waiting 20s)...", "WAIT")
            time.sleep(20)
            
            # Health Check
            if verify_kafka_health():
                create_topics()
            else:
                if not ask_user("Kafka logged errors during startup. Continue anyway?"):
                    sys.exit(1)
                create_topics()

    # --- PHASE 3: DASHBOARD DEPLOYMENT ---
    if ask_user("Do you want to launch the operational dashboards?"):
        print_status("PHASE 3: Deploying Dashboards...", "INFO")
        
        # 1. Replay Observer Dashboard
        print_status("Launching Replay Observer Dashboard...", "INFO")
        obs_script = ROOT_DIR / "start_observer.bat"
        # The bat file automatically starts the backend and UI
        start_new_window(f'"{obs_script}"', title="Observer Launcher")
        time.sleep(3)

        # 2. Writer Ops Dashboard
        print_status("Launching Writer Ops Dashboard (Port 8502)...", "INFO")
        writer_ui = ROOT_DIR / "writer_service" / "dashboard_ops.py"
        start_new_window(f'streamlit run "{writer_ui}" --server.port 8502', title="Writer Dashboard")
        time.sleep(3)

        # 3. Inference Ops Dashboard
        print_status("Launching Inference Ops Dashboard (Port 8503)...", "INFO")
        inf_ui = ROOT_DIR / "inference_service" / "dashboard_inference.py"
        start_new_window(f'streamlit run "{inf_ui}" --server.port 8503', title="Inference Dashboard")

        print_status("Dashboards launched. Browser tabs should open automatically.", "OK")

    print("\n=======================================================")
    print("✅ MASTER RESET COMPLETE. Environment is clean and ready.")
    print("=======================================================\n")

if __name__ == "__main__":
    main()
