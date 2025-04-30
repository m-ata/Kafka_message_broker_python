# Kafka Implementation using Python as the message broker.

## Overview

This project demonstrates a simple Kafka-based messaging system using Python. It includes a producer and consumer setup to simulate data flow.

## Files

- `main.py`: Kafka producer that sends messages.
- `consumer.py`: Kafka consumer that receives messages.
- `requirements.txt`: Project dependencies.
- `pyproject.toml`: Project configuration.
- `.python-version`: Python version used.
- `README.md`: Project documentation.

## Setup

### 1. Install and Run Dependencies

```bash
pip install -r requirements.txt
```

```bash
 docker run -p 2181:2181 zookeeper
```

```bash
docker run -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=192.168.2.103:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.2.103:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka
```

### 2. Run Programs

```bash
python main.py
```

```bash
python consumer.py
```
