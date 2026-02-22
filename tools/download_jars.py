# File: C:\streaming_emulator\tools\download_jars.py
import os
import urllib.request
from pathlib import Path

# Define the target directory relative to this script
TARGET_DIR = Path(__file__).parent / "jars"
TARGET_DIR.mkdir(parents=True, exist_ok=True)

# Exact JARs required for Spark 3.5.3 + Kafka + Delta 3.2.1
JARS = {
    "delta-spark": "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar",
    "delta-storage": "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar",
    "spark-sql-kafka": "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "spark-token-provider": "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "kafka-clients": "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "commons-pool2": "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
}

print(f"⬇️  Downloading {len(JARS)} JARs to {TARGET_DIR}...")

for name, url in JARS.items():
    filename = url.split("/")[-1]
    filepath = TARGET_DIR / filename
    
    if filepath.exists():
        print(f"   ✅ {filename} (Already exists)")
        continue
        
    print(f"   ⏳ Downloading {filename}...")
    try:
        urllib.request.urlretrieve(url, filepath)
        print(f"      ✅ Done.")
    except Exception as e:
        print(f"      ❌ FAILED: {e}")

print("\n🚀 Download Complete. You can now run Spark without Maven.")