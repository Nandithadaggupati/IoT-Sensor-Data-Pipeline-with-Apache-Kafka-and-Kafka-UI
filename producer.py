#!/usr/bin/env python3
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_READINGS_TOPIC = os.getenv("KAFKA_READINGS_TOPIC", "sensor-readings")
PRODUCER_INTERVAL_SEC = float(os.getenv("PRODUCER_INTERVAL_SEC", "2.0"))

DEVICE_IDS = ["sensor-001", "sensor-002", "sensor-003", "sensor-004", "sensor-005"]


def create_topic_if_not_exists(bootstrap_servers, topic_name):
    """Ensures topic exists before publishing."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        if topic_name not in metadata.topics:
            print(f"Creating topic '{topic_name}'...", flush=True)
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            fs = admin.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic '{topic}' created successfully.", flush=True)
                except Exception as e:
                    print(f"Topic '{topic}' creation note: {e}", flush=True)
        else:
            print(f"Topic '{topic_name}' already exists.", flush=True)
    except Exception as err:
        print(f"AdminClient check note: {err}", flush=True)


def delivery_report(err, msg):
    """Callback triggered by producer poll/flush when message delivery completes or fails."""
    if err is not None:
        print(f"Message delivery failed: {err}", flush=True)
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}", flush=True)


def main():
    print("Starting IoT Sensor Data Producer...", flush=True)
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
    print(f"Target Topic: {KAFKA_READINGS_TOPIC}", flush=True)

    create_topic_if_not_exists(KAFKA_BOOTSTRAP_SERVERS, KAFKA_READINGS_TOPIC)

    try:
        from confluent_kafka import Producer
        producer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "iot-sensor-producer"
        }
        producer = Producer(producer_config)
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}", flush=True)
        sys.exit(1)

    print("Producer successfully started. Emitting simulated sensor readings...", flush=True)

    try:
        while True:
            device_id = random.choice(DEVICE_IDS)
            temperature = round(random.uniform(10.0, 40.0), 1)
            humidity = round(random.uniform(30.0, 90.0), 1)
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            payload = {
                "device_id": str(device_id),
                "temperature": float(temperature),
                "humidity": float(humidity),
                "timestamp": str(timestamp)
            }

            json_payload = json.dumps(payload)

            producer.produce(
                KAFKA_READINGS_TOPIC,
                key=device_id,
                value=json_payload.encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            print(f"Published reading: {json_payload}", flush=True)

            time.sleep(PRODUCER_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Shutting down producer gracefully...", flush=True)
    finally:
        print("Flushing remaining messages...", flush=True)
        producer.flush(timeout=5)
        print("Producer shutdown complete.", flush=True)


if __name__ == "__main__":
    main()
