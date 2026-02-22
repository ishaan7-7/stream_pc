# File: C:\streaming_emulator\writer_service\src\infrastructure.py
import os
import sys
from pathlib import Path
import logging

# Configure basic logging for infrastructure initialization
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger("InfraSetup")

def setup_environment():
    """
    Sets up the portable environment variables for Apache Spark on Windows.
    
    CRITICAL FIXES APPLIED:
    1. Forces JAVA_HOME and HADOOP_HOME to local portable directories.
    2. Overrides SPARK_HOME to use the library inside the .venv.
    3. Forces Spark Workers (PYSPARK_PYTHON) to use the .venv Python executable.
       (Fixes 'Connection Reset' errors with Python 3.13).
    4. Updates system PATH to include necessary DLLs (hadoop.dll).
    """
    logger.info("🔧 Initializing Portable Spark Infrastructure...")

    # 1. Locate Project Root (Auto-detection)
    current_dir = Path(__file__).parent.absolute()
    project_root = None
    # Traverse up to find the root directory
    for parent in [current_dir] + list(current_dir.parents):
        if parent.name == "streaming_emulator":
            project_root = parent
            break
    
    # Fallback if running standalone
    if not project_root:
        project_root = Path(r"C:\streaming_emulator")

    # 2. Define Critical Paths
    tools_dir = project_root / "tools"
    jdk_path = Path(r"C:\jdk-11.0.28+6")
    hadoop_home = tools_dir / "hadoop"
    hadoop_bin = hadoop_home / "bin"

    # 3. Validate Dependencies
    critical_missing = []
    if not jdk_path.exists():
        critical_missing.append(f"JDK not found at: {jdk_path}")
    
    if not (hadoop_bin / "winutils.exe").exists():
        critical_missing.append(f"winutils.exe not found at: {hadoop_bin}")

    if critical_missing:
        for err in critical_missing:
            logger.error(f"❌ {err}")
        logger.error("STOPPING: Please fix missing infrastructure dependencies.")
        sys.exit(1)

    # 4. OVERRIDE: Spark Home & Worker Configuration
    # This aligns the Spark Engine with the Python Interpreter
    try:
        import pyspark
        venv_spark_home = Path(pyspark.__path__[0])
        os.environ['SPARK_HOME'] = str(venv_spark_home)
        logger.info(f"✅ SPARK_HOME set to .venv: {venv_spark_home}")
    except ImportError:
        logger.error("❌ Could not import pyspark. Is it installed in the .venv?")
        sys.exit(1)

    # Fix for Python 3.13 Worker Crashes:
    # We explicitly tell Spark to use THIS running python executable for workers
    venv_python = sys.executable
    os.environ['PYSPARK_PYTHON'] = venv_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python
    
    # 5. Inject Environment Variables
    # Clean up conflicting global variables if they exist
    if 'PYTHONPATH' in os.environ:
        del os.environ['PYTHONPATH']
        
    os.environ['JAVA_HOME'] = str(jdk_path)
    os.environ['HADOOP_HOME'] = str(hadoop_home)
    
    # 6. Update System Path (Prepend to ensure priority)
    current_path = os.environ.get('PATH', '')
    new_paths = [
        str(jdk_path / "bin"),
        str(hadoop_bin)
    ]
    # Only add if not already present to avoid infinite growth on reload
    if str(hadoop_bin) not in current_path:
        os.environ['PATH'] = ";".join(new_paths) + ";" + current_path

    logger.info(f"✅ JAVA_HOME: {jdk_path}")
    logger.info(f"✅ HADOOP_HOME: {hadoop_home}")
    logger.info("✅ Infrastructure Ready (Production Mode).")

def get_spark_conf_defaults():
    """
    Returns a dictionary of Spark Configuration keys needed for stability
    on Windows + Python 3.13. Use this when building SparkSession.
    """
    return {
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.python.use.daemon": "false",      # CRITICAL for Py 3.13
        "spark.python.worker.reuse": "false",    # CRITICAL for Py 3.13
        "spark.sql.execution.arrow.pyspark.enabled": "true" # Boost performance
    }

if __name__ == "__main__":
    setup_environment()