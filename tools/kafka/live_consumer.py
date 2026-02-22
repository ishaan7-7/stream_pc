import json
import os
import time
from collections import deque
from kafka import KafkaConsumer

# ---------------- CONFIG ----------------
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telemetry.engine"      # change per module
GROUP_ID = "live-viewer"
MAX_MESSAGES = 5                # how many latest messages to show
REFRESH_SECONDS = 1
# ---------------------------------------


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    buffer = deque(maxlen=MAX_MESSAGES)

    print("Starting live Kafka JSON viewer (Ctrl+C to stop)...")
    time.sleep(1)

    try:
        for msg in consumer:
            buffer.append({
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key,
                "value": msg.value,
            })

            clear_screen()
            print(f"Live Kafka View — Topic: {TOPIC}")
            print(f"Showing last {len(buffer)} messages\n")

            for i, record in enumerate(buffer, start=1):
                print("=" * 80)
                print(f"[{i}] partition={record['partition']} offset={record['offset']}")
                print(f"key: {record['key']}\n")
                print(json.dumps(record["value"], indent=2, ensure_ascii=False))
                print()

            time.sleep(REFRESH_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping live viewer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
