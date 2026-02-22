# File: C:\streaming_emulator\writer_service\src\spark_factory.py
import sys
import glob
import os
from pyspark.sql import SparkSession
from pathlib import Path
import logging

logger = logging.getLogger("SparkFactory")

def get_spark_session(app_name="WriterService"):
    """
    Optimized Spark Session for Laptop/Demo Environments.
    Low Memory Footprint (400MB per instance).
    """
    # 1. Locate Project Root
    current_dir = Path(__file__).parent.absolute()
    root_dir = None
    for parent in [current_dir] + list(current_dir.parents):
        if parent.name == "streaming_emulator":
            root_dir = parent
            break
    if not root_dir:
        root_dir = Path(r"C:\streaming_emulator")

    warehouse_dir = root_dir / "data" / "spark-warehouse"
    checkpoint_root = root_dir / "data" / "checkpoints"
    jars_dir = root_dir / "tools" / "jars"

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_root.mkdir(parents=True, exist_ok=True)

    # 2. Build Classpath
    jar_files = glob.glob(str(jars_dir / "*.jar"))
    if not jar_files:
        logger.error(f"❌ No JARs found in {jars_dir}.")
        sys.exit(1)
    
    jar_path_str = ",".join(jar_files)
    
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    logger.info(f"⚡ Starting Spark Session (Low Mem): {app_name}")
    
    # 3. Build Session with AGGRESSIVE MEMORY LIMITS
    spark = (SparkSession.builder 
        .appName(app_name) 
        .master("local[2]") 
        
        # --- MEMORY DIET ---
        # Reduced from 1g to 400m. 
        # 5 processes * 400m = 2GB (vs 5GB previously)
        .config("spark.driver.memory", "512m") 
        .config("spark.executor.memory", "512m")
        
        # Limit UI retention to save RAM
        .config("spark.ui.enabled", "false") 
        .config("spark.ui.retainedJobs", "10")
        .config("spark.sql.ui.retainedExecutions", "10")
        
        # Garbage Collection Tuning for small heaps
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        
        # Standard Configs
        .config("spark.driver.host", "127.0.0.1") 
        .config("spark.driver.bindAddress", "127.0.0.1") 
        .config("spark.sql.warehouse.dir", str(warehouse_dir.as_uri())) 
        .config("spark.jars", jar_path_str)
        .config("spark.driver.extraClassPath", jar_path_str)
        .config("spark.executor.extraClassPath", jar_path_str)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") 
        .config("spark.sql.shuffle.partitions", "2") 
        .config("spark.default.parallelism", "2")
        .config("spark.sql.streaming.fileSink.log.cleanupDelay", "10m") 
        
        .getOrCreate()
    )
        
    return spark