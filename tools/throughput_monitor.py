# File: C:\streaming_emulator\tools\throughput_monitor.py
import time
import json
import glob
import os
import sys
from confluent_kafka import Consumer, TopicPartition
from pathlib import Path

# Configuration
KAFKA_BROKER = "localhost:9092"
DATA_ROOT = Path("C:/streaming_emulator/data/delta/bronze")
MODULES = ["engine", "battery", "body", "transmission", "tyre"]
INTERVAL_SEC = 20

def get_kafka_total_rows():
    """Queries Kafka for the High Watermark of all partitions."""
    conf = {'bootstrap.servers': KAFKA_BROKER, 'group.id': 'monitor_tool_v1', 'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)
    
    totals = {}
    
    try:
        for module in MODULES:
            topic = f"telemetry.{module}"
            # Fetch metadata to find partitions
            metadata = consumer.list_topics(topic, timeout=5)
            if topic not in metadata.topics:
                totals[module] = 0
                continue
            
            partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
            
            # Query High Watermark for each partition
            module_total = 0
            for p in partitions:
                low, high = consumer.get_watermark_offsets(p, timeout=2.0, cached=False)
                module_total += high
            
            totals[module] = module_total
    except Exception as e:
        print(f"⚠️ Kafka Error: {e}")
    finally:
        consumer.close()
        
    return totals

def get_delta_total_rows():
    """Parses Delta Lake Transaction Logs to count committed records."""
    totals = {}
    
    for module in MODULES:
        log_path = DATA_ROOT / module / "_delta_log"
        module_total = 0
        
        if log_path.exists():
            # Read all commit files (00000.json, etc.)
            json_files = glob.glob(str(log_path / "*.json"))
            for jf in json_files:
                try:
                    with open(jf, "r") as f:
                        for line in f:
                            action = json.loads(line)
                            if "add" in action:
                                # Delta stores stats as a stringified JSON inside the 'add' action
                                stats_str = action["add"].get("stats")
                                if stats_str:
                                    stats = json.loads(stats_str)
                                    module_total += int(stats.get("numRecords", 0))
                            elif "remove" in action:
                                # Handle compactions/deletes (simplified)
                                # In append-only streams, 'remove' usually means compaction. 
                                # For a raw count, we might ignore or subtract carefully.
                                # For this Bronze Append-Only streamer, we can largely trust 'add'.
                                pass
                except Exception:
                    pass # Skip corrupted logs
                    
        totals[module] = module_total
    
    return totals

def run_monitor():
    print(f"🔍 Starting Throughput Monitor (Interval: {INTERVAL_SEC}s)...")
    print(f"   [Kafka] Reading High Watermarks")
    print(f"   [Delta] Parsing _delta_log stats")
    print("-" * 100)
    print(f"{'TIMESTAMP':<10} | {'MODULE':<12} | {'KAFKA TOTAL':<12} | {'IN RATE':<10} | {'DELTA TOTAL':<12} | {'WRITE RATE':<10} | {'BACKLOG':<10}")
    print("-" * 100)

    # Initial State
    prev_kafka = get_kafka_total_rows()
    prev_delta = get_delta_total_rows()
    prev_time = time.time()

    while True:
        try:
            # Wait loop
            time_to_wait = INTERVAL_SEC
            while time_to_wait > 0:
                sys.stdout.write(f"\r⏳ Updating in {time_to_wait}s...   ")
                sys.stdout.flush()
                time.sleep(1)
                time_to_wait -= 1
            sys.stdout.write("\r" + " " * 30 + "\r") # Clear line

            # Capture Current State
            curr_time = time.time()
            curr_kafka = get_kafka_total_rows()
            curr_delta = get_delta_total_rows()
            
            dt = curr_time - prev_time
            
            # Aggregates for Summary
            sum_kafka_rate = 0
            sum_delta_rate = 0
            sum_kafka_total = 0
            sum_delta_total = 0
            sum_backlog = 0

            timestamp_str = time.strftime("%H:%M:%S")

            # Print Per-Module Stats
            for module in MODULES:
                # Kafka Calcs
                k_total = curr_kafka.get(module, 0)
                k_prev = prev_kafka.get(module, 0)
                k_rate = (k_total - k_prev) / dt
                
                # Delta Calcs
                d_total = curr_delta.get(module, 0)
                d_prev = prev_delta.get(module, 0)
                d_rate = (d_total - d_prev) / dt
                
                # Backlog
                backlog = k_total - d_total
                
                # Aggregates
                sum_kafka_total += k_total
                sum_delta_total += d_total
                sum_kafka_rate += k_rate
                sum_delta_rate += d_rate
                sum_backlog += backlog

                print(f"{timestamp_str:<10} | {module:<12} | {k_total:<12,} | {k_rate:>8.1f}/s | {d_total:<12,} | {d_rate:>8.1f}/s | {backlog:<10,}")

            # Print Summary Row
            print("-" * 100)
            print(f"{'SUMMARY':<10} | {'ALL MODULES':<12} | {sum_kafka_total:<12,} | {sum_kafka_rate:>8.1f}/s | {sum_delta_total:<12,} | {sum_delta_rate:>8.1f}/s | {sum_backlog:<10,}")
            print("=" * 100)
            print("")

            # Update State
            prev_kafka = curr_kafka
            prev_delta = curr_delta
            prev_time = curr_time

        except KeyboardInterrupt:
            print("\n🛑 Monitor Stopped.")
            break

if __name__ == "__main__":
    run_monitor()