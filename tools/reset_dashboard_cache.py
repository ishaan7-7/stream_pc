import psutil
import time

def kill_process_on_port(port, service_name):
    """Hunts down and kills any process listening on the specified port."""
    killed = False
    for conn in psutil.net_connections(kind='inet'):
        if conn.laddr.port == port and conn.status == 'LISTEN':
            try:
                # Find the parent process
                parent = psutil.Process(conn.pid)
                
                # Terminate all children first (like uvicorn workers)
                children = parent.children(recursive=True)
                for child in children:
                    child.terminate()
                
                # Terminate the parent
                parent.terminate()
                
                # Wait for them to die, kill if stubborn
                gone, alive = psutil.wait_procs(children + [parent], timeout=3)
                for p in alive:
                    p.kill()
                    
                print(f"   ✅ Cleared RAM Cache for {service_name} (Port {port}, PID {conn.pid})")
                killed = True
            except psutil.NoSuchProcess:
                pass
            except Exception as e:
                print(f"   ⚠️ Could not clear {service_name} on port {port}: {e}")
                
    if not killed:
        print(f"   ℹ️ {service_name} API (Port {port}) was not running. Cache is already empty.")

def reset_dashboard_ram_caches():
    print("=========================================")
    print("🧠 CLEARING IN-MEMORY DASHBOARD CACHES")
    print("=========================================")
    
    # Port 8002 = Gold Service (Vehicle Health)
    # Port 8006 = Telemetry Observer (Replay / Fleet Status)
    
    kill_process_on_port(8002, "Vehicle Health (Gold API)")
    kill_process_on_port(8006, "Telemetry Observer API")

    print("=========================================")
    print("✅ RAM CACHES CLEARED. Dashboards will read zero.")
    print("=========================================")

if __name__ == "__main__":
    reset_dashboard_ram_caches()