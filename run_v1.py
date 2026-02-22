import os
import sys
import time
import socket
import subprocess
import shutil
import json
from pathlib import Path

# --- Configuration ---
ROOT_DIR = Path(__file__).parent.resolve()
VENV_SCRIPTS = ROOT_DIR / ".venv" / "Scripts"

# Kafka paths (Adjust if your Kafka is in a different location)
KAFKA_HOME = Path("C:/kafka")
KAFKA_BIN = KAFKA_HOME / "bin" / "windows"
ZOOKEEPER_BAT = ROOT_DIR / "tools" / "kafka" / "start_zookeeper.bat"
KAFKA_BAT = ROOT_DIR / "tools" / "kafka" / "start_kafka.bat"

# Default Kafka Log locations on Windows
KAFKA_LOG_DIR = Path("C:/tmp/kafka-logs")
ZOOKEEPER_DATA_DIR = Path("C:/tmp/zookeeper")

# Topics Config
INGEST_CONFIG_PATH = ROOT_DIR / "ingest" / "config" / "ingest_config.json"

# Service Definitions
SERVICES = {
    "ingest": {
        "port": 8000,
        "cmd": "uvicorn ingest.app.main:app --port 8000 --reload"
    },
    "kafka_metrics": {
        "port": 9201,
        "cmd": "uvicorn kafka_metrics.app.main:app --port 9201 --reload"
    },
    "dashboard": {
        "port": 9300,
        "cmd": "uvicorn dashboard.app.main:app --port 9300 --reload"
    }
}

def print_status(msg, status="INFO"):
    print(f"   [{status}] {msg}")

def check_port(port, host="127.0.0.1", timeout=1.0):
    """Checks if a port is open."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        result = s.connect_ex((host, port))
        return result == 0

def wait_for_port(port, service_name, retries=30, delay=1):
    """Waits until a port is open."""
    print_status(f"Waiting for {service_name} on port {port}...", "WAIT")
    for _ in range(retries):
        if check_port(port):
            print_status(f"{service_name} is listening on port {port}.", "OK")
            return True
        time.sleep(delay)
    print_status(f"Timed out waiting for {service_name}.", "ERROR")
    return False

def start_new_window(command, title="Service"):
    """Starts a command in a completely new CMD window."""
    try:
        subprocess.Popen(
            f'start "{title}" {command}', 
            shell=True, 
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        return True
    except Exception as e:
        print_status(f"Failed to launch window: {e}", "ERROR")
        return False

def activate_venv():
    """Injects venv scripts into PATH for this process and children."""
    if not VENV_SCRIPTS.exists():
        print_status("Virtual environment not found at .venv\\Scripts", "ERROR")
        return False
    
    # Prepend venv scripts to PATH
    os.environ["PATH"] = str(VENV_SCRIPTS) + os.pathsep + os.environ["PATH"]
    return True

def ask_user(prompt):
    """Simple y/n input."""
    while True:
        choice = input(f"\n> {prompt} (y/n): ").strip().lower()
        if choice in ['y', 'yes']:
            return True
        if choice in ['n', 'no']:
            return False

def clean_kafka_logs():
    """Deletes the C:/tmp logs to fix Windows corruption issues."""
    print_status("Checking for stale Kafka logs...", "INFO")
    
    cleaned = False
    for path in [KAFKA_LOG_DIR, ZOOKEEPER_DATA_DIR]:
        if path.exists():
            try:
                shutil.rmtree(path)
                print_status(f"Deleted: {path}", "CLEAN")
                cleaned = True
            except PermissionError:
                print_status(f"Cannot delete {path}. Is Kafka still running?", "ERROR")
                return False
            except Exception as e:
                print_status(f"Error deleting {path}: {e}", "ERROR")
                return False
    
    if not cleaned:
        print_status("No stale logs found.", "INFO")
    return True

def get_topics_from_config():
    """Reads topic names from ingest_config.json"""
    if not INGEST_CONFIG_PATH.exists():
        print_status("Ingest config not found, skipping topic list.", "WARN")
        return []
    
    try:
        with open(INGEST_CONFIG_PATH, 'r') as f:
            data = json.load(f)
            # Extact values from the "topics" dictionary
            # e.g. "engine": "telemetry.engine" -> we need "telemetry.engine"
            return list(data.get("topics", {}).values())
    except Exception as e:
        print_status(f"Failed to parse ingest config: {e}", "ERROR")
        return []

def create_topics():
    """Creates topics using kafka-topics.bat with 6 Partitions"""
    topics = get_topics_from_config()
    if not topics:
        print_status("No topics found in ingest_config.json to create.", "SKIP")
        return

    topic_bat = KAFKA_BIN / "kafka-topics.bat"
    if not topic_bat.exists():
        print_status(f"Cannot find kafka-topics.bat at {topic_bat}", "ERROR")
        print_status("Please check KAFKA_HOME path in run.py", "INFO")
        return

    print_status(f"Creating {len(topics)} topics with 6 partitions each...", "INFO")
    
    for topic in topics:
        # Command: kafka-topics.bat --create --topic <name> --partitions 6 --replication-factor 1 --if-not-exists
        cmd = [
            str(topic_bat),
            "--create",
            "--topic", topic,
            "--bootstrap-server", "localhost:9092",
            "--partitions", "6",         # <--- UPDATED TO 6
            "--replication-factor", "1",
            "--if-not-exists"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print_status(f"Topic '{topic}': OK (6 Partitions)", "OK")
            else:
                if "already exists" in result.stdout:
                    print_status(f"Topic '{topic}': Already exists", "OK")
                else:
                    print_status(f"Topic '{topic}': FAILED - {result.stderr.strip()}", "ERROR")
        except Exception as e:
             print_status(f"Failed to execute topic command: {e}", "ERROR")

def main():
    print("========================================")
    print("   Streaming Emulator - Master Runner   ")
    print("========================================")

    # 1. Activate .venv
    if ask_user("Activate .venv"):
        if activate_venv():
            print_status(".venv activated (injected into process PATH).", "OK")
        else:
            sys.exit(1)
    else:
        print_status("Activation required to proceed.", "WARN")
        sys.exit(0)

    # 2. Reset Logs
    if ask_user("Hard Reset Kafka Logs (Fixes corruption issues)"):
        if not clean_kafka_logs():
            print_status("Could not clean logs. Please stop existing Java processes manually.", "WARN")

    # 3. Start Zookeeper
    if ask_user("Start Zookeeper"):
        if not ZOOKEEPER_BAT.exists():
            print_status(f"Missing file: {ZOOKEEPER_BAT}", "ERROR")
        else:
            start_new_window(f'"{ZOOKEEPER_BAT}"', title="Zookeeper")
            if wait_for_port(2181, "Zookeeper", retries=30):
                print_status("Stabilizing Zookeeper (waiting 5s)...", "WAIT")
                time.sleep(5)
                print_status("Zookeeper Ready.", "OK")

    # 4. Start Kafka & Create Topics
    if check_port(2181): 
        if ask_user("Start Kafka"):
            if not KAFKA_BAT.exists():
                print_status(f"Missing file: {KAFKA_BAT}", "ERROR")
            else:
                start_new_window(f'"{KAFKA_BAT}"', title="Kafka")
                if wait_for_port(9092, "Kafka", retries=60):
                    print_status("Kafka is Running successfully.", "OK")
                    
                    # Ask to create topics
                    if ask_user("Create Topics (Required after Hard Reset)"):
                        create_topics()
    else:
        print_status("Skipping Kafka (Zookeeper port 2181 not open).", "SKIP")

    # 5. Start Python Services
    app_list = list(SERVICES.keys())
    print(f"\nAvailable Apps: {app_list}")
    
    start_all = ask_user("Start all apps")
    
    if start_all:
        for name, config in SERVICES.items():
            print_status(f"Launching {name}...", "INFO")
            start_new_window(config['cmd'], title=name)
            wait_for_port(config['port'], name)
    else:
        while True:
            selection = input(f"\nEnter app name from list (or 'exit'): ").strip().lower()
            if selection == 'exit':
                break
            
            if selection in SERVICES:
                config = SERVICES[selection]
                if check_port(config['port']):
                    print_status(f"{selection} is already running on port {config['port']}.", "WARN")
                else:
                    start_new_window(config['cmd'], title=selection)
                    wait_for_port(config['port'], selection)
            else:
                print_status("Invalid app name.", "WARN")

    print("\nAll tasks completed. You may close this launcher.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting...")