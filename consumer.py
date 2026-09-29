#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_READINGS_TOPIC = os.getenv("KAFKA_READINGS_TOPIC", "sensor-readings")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "sensor-alerts")
CONSUMER_GROUP_ID = "sensor-processor-group"

TEMP_THRESHOLD = 35.0
HUMIDITY_THRESHOLD = 80.0


def create_topics_if_not_exist(bootstrap_servers, topics):
    """Ensures required topics exist before consumer/producer operation."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        new_topics = []
        for topic_name in topics:
            if topic_name not in metadata.topics:
                print(f"Creating topic '{topic_name}'...", flush=True)
                new_topics.append(NewTopic(topic_name, num_partitions=1, replication_factor=1))

        if new_topics:
            fs = admin.create_topics(new_topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic '{topic}' created successfully.", flush=True)
                except Exception as e:
                    print(f"Topic '{topic}' creation note: {e}", flush=True)
    except Exception as err:
        print(f"AdminClient check note: {err}", flush=True)


def alert_delivery_report(err, msg):
    if err is not None:
        print(f"Alert message delivery failed: {err}", flush=True)
    else:
        print(f"Alert message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}", flush=True)


def main():
    print("Starting IoT Sensor Stream Processor (Consumer)...", flush=True)
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
    print(f"Subscribed Topic: {KAFKA_READINGS_TOPIC}", flush=True)
    print(f"Alerts Topic: {KAFKA_ALERTS_TOPIC}", flush=True)
    print(f"Consumer Group ID: {CONSUMER_GROUP_ID}", flush=True)

    create_topics_if_not_exist(KAFKA_BOOTSTRAP_SERVERS, [KAFKA_READINGS_TOPIC, KAFKA_ALERTS_TOPIC])

    try:
        from confluent_kafka import Consumer, KafkaError, Producer
        consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        consumer = Consumer(consumer_config)

        producer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "sensor-alert-producer"
        }
        alert_producer = Producer(producer_config)
    except Exception as e:
        print(f"Error initializing Kafka Consumer/Producer: {e}", flush=True)
        sys.exit(1)

    consumer.subscribe([KAFKA_READINGS_TOPIC])
    print(f"Consumer subscribed to '{KAFKA_READINGS_TOPIC}'. Polling for messages...", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}", flush=True)
                    continue

            try:
                raw_val = msg.value().decode("utf-8")
                reading = json.loads(raw_val)

                device_id = reading.get("device_id")
                temperature = float(reading.get("temperature", 0.0))
                humidity = float(reading.get("humidity", 0.0))

                print(f"Received reading from {device_id}: Temp={temperature}°C, Humidity={humidity}%", flush=True)

                alerts_to_send = []

                # High Temperature evaluation
                if temperature > TEMP_THRESHOLD:
                    alert_payload = {
                        "device_id": str(device_id),
                        "alert_type": "HIGH_TEMPERATURE",
                        "trigger_value": float(temperature),
                        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                    alerts_to_send.append(alert_payload)

                # High Humidity evaluation
                if humidity > HUMIDITY_THRESHOLD:
                    alert_payload = {
                        "device_id": str(device_id),
                        "alert_type": "HIGH_HUMIDITY",
                        "trigger_value": float(humidity),
                        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                    alerts_to_send.append(alert_payload)

                for alert in alerts_to_send:
                    alert_json = json.dumps(alert)
                    print(f"[ALERT] TRIGGERING ALERT: {alert_json}", flush=True)
                    alert_producer.produce(
                        KAFKA_ALERTS_TOPIC,
                        key=device_id,
                        value=alert_json.encode("utf-8"),
                        callback=alert_delivery_report
                    )
                    alert_producer.poll(0)

                if alerts_to_send:
                    alert_producer.flush(timeout=2.0)

            except json.JSONDecodeError as err:
                print(f"Failed to parse JSON message: {err}", flush=True)
            except Exception as err:
                print(f"Error processing message: {err}", flush=True)

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Shutting down consumer gracefully...", flush=True)
    finally:
        print("Closing Kafka consumer and flushing producer...", flush=True)
        try:
            consumer.close()
        except Exception as e:
            print(f"Error closing consumer: {e}", flush=True)
        try:
            alert_producer.flush(timeout=5)
        except Exception as e:
            print(f"Error flushing producer: {e}", flush=True)
        print("Consumer shutdown complete.", flush=True)


if __name__ == "__main__":
    main()
