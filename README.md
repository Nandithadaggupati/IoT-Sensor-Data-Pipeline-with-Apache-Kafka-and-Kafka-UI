# IoT Sensor Data Pipeline with Apache Kafka and Kafka UI

An end-to-end streaming data pipeline built with **Apache Kafka (KRaft Mode)**, **Kafka UI**, and **Python**. The pipeline simulates IoT sensors emitting real-time temperature and humidity readings, processes the stream in real-time, evaluates threshold breaches, and generates immediate alert events.

---

## 🏗️ Architecture Overview

```
+--------------------------------+       +------------------------------------+       +----------------------------------+
|      IoT Producer (Python)     | ----> |  Kafka Topic: sensor-readings      | ----> |   Stream Processor (Consumer)    |
| (Simulates device readings)    |       |  (Broker Port 9094)                |       | (Group: sensor-processor-group) |
+--------------------------------+       +------------------------------------+       +----------------------------------+
                                                                                                      |
                                                                                                      v Evaluates Thresholds
                                         +------------------------------------+                       | (Temp > 35.0 / Hum > 80.0)
                                         |    Kafka Topic: sensor-alerts      | <---------------------+
                                         |    (Broker Port 9094)              |
                                         +------------------------------------+
                                                           ^
                                                           | Visual Monitoring
                                         +------------------------------------+
                                         |    Kafka UI (Web Dashboard)        |
                                         |    http://localhost:8080           |
                                         +------------------------------------+
```

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- **Docker Desktop** (with Docker Compose)
- **Python 3.8+**

### 2. Environment Setup
1. Clone / navigate to the repository directory.
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy environment configuration:
   ```bash
   cp .env.example .env
   ```

### 3. Launch Infrastructure (Kafka & Kafka UI)
Start the Kafka broker in KRaft mode and Kafka UI using Docker Compose:
```bash
docker-compose up -d
```
Verify container status and health:
```bash
docker-compose ps
```
- **Kafka Broker**: exposed at `localhost:9094` (external) & `kafka:9092` (internal)
- **Kafka UI**: accessible at `http://localhost:8080`

### 4. Run Stream Processing Engine (Consumer)
In Terminal 1, start the consumer script:
```bash
python consumer.py
```
*The consumer joins the `sensor-processor-group` consumer group with `auto.offset.reset=earliest` and polls for incoming readings from `sensor-readings`.*

### 5. Run IoT Sensor Data Generator (Producer)
In Terminal 2, start the producer script:
```bash
python producer.py
```
*The producer continuously generates and streams simulated JSON sensor readings to the `sensor-readings` topic every 2 seconds.*

---

## 📊 Data Schema Contracts

### 1. Sensor Readings (`sensor-readings` topic)
```json
{
  "device_id": "sensor-001",
  "temperature": 36.8,
  "humidity": 45.2,
  "timestamp": "2026-09-29T11:57:00Z"
}
```

### 2. Sensor Alerts (`sensor-alerts` topic)
```json
{
  "device_id": "sensor-001",
  "alert_type": "HIGH_TEMPERATURE",
  "trigger_value": 36.8,
  "timestamp": "2026-09-29T11:57:01Z"
}
```

---

## 🚨 Anomaly Detection Thresholds

- **HIGH_TEMPERATURE Alert**: Triggered when `temperature > 35.0`
- **HIGH_HUMIDITY Alert**: Triggered when `humidity > 80.0`

---

## 🖥️ Monitoring via Kafka UI

1. Open your browser and navigate to `http://localhost:8080`.
2. Inspect `local-cluster` status (Online).
3. Select **Topics** to view `sensor-readings` and `sensor-alerts`.
4. Click into `sensor-readings` -> **Messages** to view live streaming telemetry data.
5. Select **Consumers** -> `sensor-processor-group` to monitor consumer group offset lag and active partitions.

---

## 🧪 Verification & Automated Testing

This repository includes a `submission.json` mapping required cluster endpoints, topic names, and execution commands for automated evaluation.
