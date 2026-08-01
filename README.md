# Olist Real-Time E-Commerce Streaming Pipeline

This project simulates a highly resilient, real-time data streaming pipeline using the historical [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It replays historical orders as if they were happening right now, processes them through a streaming architecture, performs real-time anomaly detection, and visualizes the results on a live dashboard.

It is designed as a **technical showcase** of streaming data concepts, handling real-world distributed systems challenges such as connection pooling, database resilience, event-time windowing, and statistical anomaly detection.

---

## 🏗️ Architecture overview

1. **Replay Engine (`replay.py`)**
   Reads a pre-processed historical dataset (`orders_grouped.jsonl`), calculates the exact time differences between events, scales them down by a massive factor (86,400x, turning days into seconds), and publishes them to Kafka. This creates a realistic, paced stream of data that mimics production load.
2. **Apache Kafka**
   Acts as the central nervous system. Running in Zookeeper-less KRaft mode, Kafka provides a distributed, append-only log that guarantees message persistence and allows multiple independent systems to consume the exact same stream of events at their own pace.
3. **Stream Processor (`processor.py`)**
   A custom Python consumer that reads from Kafka. It is responsible for:
   - Grouping events into **event-time windows**.
   - Maintaining rolling statistics (mean and variance) for product categories.
   - Flagging pricing anomalies in real-time using Z-Scores.
   - Buffering data and executing **batch inserts** into the database to maximize write throughput.
4. **TimescaleDB (PostgreSQL)**
   A time-series optimized relational database. It stores the live stream of `raw_orders` inside a heavily optimized hypertable, alongside parsed `raw_order_items`, `anomalies`, and aggregated `metrics_1m` tables.
5. **Live Dashboard (`dashboard/app.py`)**
   A Plotly/Dash frontend that utilizes a thread-safe connection pool to continually poll TimescaleDB. It visualizes the current pipeline throughput, a breakdown of revenue by category, and a live feed of flagged statistical anomalies.

---

## 🚀 How to Run

Because the project is entirely containerized and the data is bundled, spinning it up is a single command:

```bash
docker-compose up --build -d
```

Navigate to `http://localhost:8050` to view the live dashboard. The data stream will begin immediately, and anomalies will start populating once the processor gathers enough baseline samples.

To wipe the slate clean and restart the demo from scratch:
```bash
docker-compose down -v
docker-compose up --build -d
```

---

## 🧠 Design Decisions & Interview Defense

If asked why this system was built the way it was, here are the architectural justifications:

### Why Kafka over a simple queue (like RabbitMQ)?
Kafka is a distributed append-only log, not just an ephemeral queue. Messages are persistent and replayable. This allows multiple distinct systems (e.g., this real-time dashboard processor AND a separate batch data warehouse loader) to consume the exact same stream of events at their own pace without destroying the messages upon read.

### Why Event-Time vs Processing-Time windowing?
Using processing time (wall-clock time) means a "1-minute window" aggregates whatever messages happened to arrive during that real-world minute. During data bursts, network lag, or historical replays (like this scaled demo), processing time completely distorts the metrics. Event-time windowing uses the timestamp embedded in the payload (`order_purchase_timestamp`), meaning a "1-minute window" always accurately represents exactly one minute of historical time, regardless of how fast or erratically the data arrives.

### Why batch inserts into PostgreSQL rather than per-message inserts?
Relational databases incur significant overhead per transaction (network round trip, WAL logging, locking). Inserting one row at a time for high-throughput streaming events will quickly bottleneck the database and back up the Kafka consumer. The processor buffers events and batches inserts (e.g., executing `execute_values` every 2 seconds), which amortizes this transaction overhead and achieves orders of magnitude higher write throughput.

### How does the system handle consumer lag?
If the processor can't keep up with the incoming rate, consumer lag builds up. Because we use Kafka, we can seamlessly fix this by increasing the number of Kafka partitions for the topic and horizontally scaling by spinning up more processor instances. Kafka will automatically distribute the partitions exclusively among the running processor instances, allowing them to work in parallel on mutually exclusive subsets of the data with no race conditions.

---

## 🐛 Challenges Faced & Bugs Squashed

Building a robust distributed system is rarely straightforward. Here are the specific engineering challenges encountered and resolved during the construction of this pipeline:

1. **Database Connection Leaks (Dashboard)**
   - *The Problem*: The dashboard was initially opening a new `psycopg2.connect()` on every UI callback tick (every 2 seconds) and failing to close them. This rapidly exhausted PostgreSQL's max connection limit, causing the dashboard and the processor to crash.
   - *The Fix*: Implemented a `psycopg2.pool.ThreadedConnectionPool` initialized lazily on startup, and wrapped every database interaction in a strict `try/finally` block to guarantee connections are returned to the pool, holding the system at a rock-steady 10 active connections indefinitely.

2. **Statistical Noise & "Uncategorized" Anomalies**
   - *The Problem*: The anomaly detection system was constantly flagging items in the `uncategorized` bucket. Because this bucket acts as a catch-all for unrelated products, it has no coherent price distribution, making its variance enormous and any computed Z-score statistically meaningless.
   - *The Fix*: Explicitly excluded `uncategorized` items from anomaly calculations and increased the minimum sample threshold (`MIN_SAMPLES_FOR_ZSCORE`) to 30, ensuring anomalies are only flagged when there is enough historical data to form a highly stable mean.

3. **Processor Death on DB Outage**
   - *The Problem*: If TimescaleDB experienced a transient blip (e.g., a restart or network drop), the processor's active TCP connection would sever, throwing an `OperationalError` that crashed the entire consumer loop permanently.
   - *The Fix*: Built a resilient exponential backoff retry loop into the processor's `flush_batch` and `flush_window` functions. If the DB drops, the processor catches the exception, sleeps, and actively attempts to rebuild the connection pool until Postgres returns, successfully resuming the stream without dropping data.

4. **Dashboard Cold-Start Race Conditions**
   - *The Problem*: The dashboard tried to initialize its connection pool exactly once at the top of the script on container boot. If it booted while TimescaleDB was busy running its temporary `init.sql` schema build, the dashboard received a `Connection refused` and stayed permanently, silently broken.
   - *The Fix*: Moved the connection pool initialization into a lazy-evaluation pattern (`get_db_connection()`). If the pool is missing when a UI callback fires, it dynamically rebuilds it, fully resolving startup race conditions.

5. **TimescaleDB Hypertable Keys vs. Deduplication**
   - *The Problem*: TimescaleDB requires the time partitioning column to be part of the table's Primary Key. But to achieve idempotency, we needed to use `ON CONFLICT (order_id) DO NOTHING` to deduplicate retried messages. 
   - *The Fix*: Altered the schema to use a composite primary key `(order_id, order_purchase_timestamp)`, satisfying TimescaleDB's partitioning requirements while preserving our ability to deduplicate safely.

---

## ⚠️ Known Limitations & Tradeoffs

To keep the architecture focused and avoid over-engineering, a few intentional tradeoffs were made:

### 1. In-Memory State Loss on Restart
Rolling statistics (`category_stats`) and un-flushed window metrics currently live entirely in the processor's Python memory space. If the processor container crashes and restarts (e.g., due to a prolonged Postgres outage exceeding the retry budget), this in-memory state is silently dropped. Anomaly detection will restart from zero samples per category. *Persisting this state across restarts would require checkpointing to an external store (like Redis), which adds significant architectural complexity.*

### 2. At-Least-Once Delivery Semantics
The processor's retry logic guarantees **at-least-once** delivery for database writes. If a batch commit actually succeeds on Postgres but the acknowledgment is lost over the network before the processor receives it, the processor will retry the exact same batch.
- `raw_orders` uses `ON CONFLICT DO NOTHING` on its composite key, making its inserts safely deduplicated.
- `raw_order_items` and `anomalies` do not have unique constraints, meaning retries in this specific edge case will result in duplicate rows for those tables.

### 3. Non-Persistent Database Volume
The `timescaledb` service intentionally lacks a persistent Docker volume mapping for `/var/lib/postgresql/data`. This is a deliberate design choice to ensure **demo repeatability**. Every time the containers are destroyed and recreated, the database starts completely fresh. If you wish to persist the data between demo runs, you must add a named volume to the `timescaledb` service in `docker-compose.yml`.
