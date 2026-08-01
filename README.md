# Olist E-Commerce Streaming Pipeline

![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10-blue.svg)
![Kafka](https://img.shields.io/badge/kafka-3.7.0-black.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-14-blue.svg)
![Dash](https://img.shields.io/badge/dash-plotly-teal.svg)

This project is a real-time data streaming pipeline built around the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It simulates a high-throughput production environment by replaying historical e-commerce orders as a live Kafka stream, processing them in real-time, performing statistical anomaly detection, and visualizing the results on a live dashboard.

## 🌟 Features

- **Historical Replay Engine**: Simulates live traffic by replaying static JSONL data into Kafka, perfectly preserving the exact time deltas between events (scaled for demo purposes).
- **Event-Time Windowing**: Accurately aggregates metrics based on the payload's timestamp rather than the system's wall-clock time, ensuring resilience against network lag and bursty traffic.
- **Real-Time Anomaly Detection**: Calculates rolling Z-scores for product prices grouped by category to instantly flag pricing anomalies.
- **High-Throughput Batching**: Buffers and commits database inserts in chunks to minimize Postgres transaction overhead.
- **Live Dashboards**: A Plotly/Dash frontend dynamically visualizes throughput, categorical revenue, and live anomalies via polling.

## 🏗️ Architecture

1. **Replay (`replay.py`)**: Reads `orders_grouped.jsonl`, calculates the time delta between sequential orders, scales it by a configurable factor (e.g., `86400x`), and publishes the JSON payload to Kafka.
2. **Message Broker (Apache Kafka)**: Runs in Zookeeper-less KRaft mode. Acts as the durable, append-only log connecting the replay engine to downstream consumers.
3. **Stream Processor (`processor.py`)**: A Python consumer that reads from Kafka, computes rolling category statistics, detects anomalies, and performs batch inserts into TimescaleDB.
4. **Database (TimescaleDB)**: A time-series optimized PostgreSQL database. Stores raw events in partitioned hypertables and aggregates 1-minute window metrics.
5. **Dashboard (`dashboard/app.py`)**: A frontend application that maintains a thread-safe PostgreSQL connection pool to poll and visualize the latest windowed data every few seconds.

## 🚀 Quick Start

The entire pipeline is containerized and comes bundled with the dataset. 

**Prerequisites:**
- Docker
- Docker Compose

**1. Clone the repository:**
```bash
git clone https://github.com/yourusername/olist-streaming-pipeline.git
cd olist-streaming-pipeline
```

**2. Start the pipeline:**
```bash
docker-compose up --build -d
```

**3. View the Dashboard:**
Navigate to [http://localhost:8050](http://localhost:8050) in your browser. The data stream begins immediately.

**4. Stop and Clean Up:**
To spin down the containers and wipe the database for a fresh run:
```bash
docker-compose down -v
```

## 🛠️ Technical Highlights

Building a reliable streaming architecture involves navigating several distributed systems challenges. This pipeline implements the following resilience patterns:

- **Connection Pool Management**: The dashboard uses `psycopg2.pool.ThreadedConnectionPool` with strict `try/finally` blocks, preventing connection leaks and database exhaustion during high-frequency polling.
- **Database Outage Recovery**: The stream processor implements an exponential backoff retry loop. If TimescaleDB experiences a transient outage, the processor gracefully pauses, reconnects, and resumes from its last Kafka offset without data loss.
- **Cold-Start Resilience**: Microservices often boot before their dependencies are fully ready. The dashboard lazily evaluates its database connection pool, ensuring it recovers smoothly even if it boots during TimescaleDB's schema initialization.
- **Idempotent Inserts**: `raw_orders` utilizes TimescaleDB hypertables with a composite primary key (`order_id, order_purchase_timestamp`) to support `ON CONFLICT DO NOTHING`, ensuring deduplication if the processor needs to retry a batch commit.

## ⚠️ Known Limitations

- **In-Memory State**: Rolling statistics (`category_stats`) currently live in the processor's memory. If the processor container restarts, the anomaly detection baselines reset to zero. In a production environment, this state would be checkpointed to an external store like Redis.
- **At-Least-Once Delivery**: While order ingestion is idempotent, `raw_order_items` and `anomalies` do not have unique constraints. A network failure exactly between a successful Postgres commit and the processor's acknowledgment could result in duplicate item rows on retry.
- **Ephemeral Storage**: The `timescaledb` service intentionally lacks a persistent Docker volume for `/var/lib/postgresql/data` to ensure a completely clean slate on every demo run.

## 📄 License

This project is licensed under the MIT License.
