import os
import json
import time
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import math

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/streaming")
TOPIC = "orders-stream"
BATCH_SIZE = 100
BATCH_TIMEOUT = 2.0  # seconds

# Rolling stats per category
class CategoryStats:
    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, value):
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def stddev(self):
        if self.count < 2:
            return 0.0
        return math.sqrt(self.m2 / (self.count - 1))

category_stats = {}

def get_z_score(category, price):
    if category not in category_stats:
        category_stats[category] = CategoryStats()
    stats = category_stats[category]
    mean = stats.mean
    stddev = stats.stddev()
    
    # Update stats
    stats.update(price)
    
    # Calculate z-score using previous mean/stddev or 0 if not enough data
    if stats.count < 30 or stddev == 0:
        return mean, stddev, 0.0
        
    z_score = abs(price - mean) / stddev
    return mean, stddev, z_score

# Windowing state
current_window_start = None
window_orders_count = 0
window_anomaly_count = 0
window_revenue_by_category = {}
db_conn = None

def get_db_connection(max_retries=5, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(POSTGRES_URL)
        except psycopg2.OperationalError as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff_factor ** attempt)

def flush_window(window_start_dt):
    global window_orders_count, window_anomaly_count, window_revenue_by_category, db_conn
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with db_conn.cursor() as cur:
                if window_orders_count > 0:
                    cur.execute("""
                        INSERT INTO metrics_1m (window_start, orders_count, anomaly_count)
                        VALUES (%s, %s, %s)
                    """, (window_start_dt, window_orders_count, window_anomaly_count))
                    
                rev_records = []
                for cat, rev in window_revenue_by_category.items():
                    rev_records.append((window_start_dt, cat, rev))
                    
                if rev_records:
                    execute_batch(cur, """
                        INSERT INTO revenue_by_category_1m (window_start, category, revenue)
                        VALUES (%s, %s, %s)
                    """, rev_records)
                    
            db_conn.commit()
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if attempt == max_retries - 1:
                print(f"Failed to flush window after {max_retries} attempts: {e}")
                raise
            print(f"Database error during flush_window, retrying: {e}")
            time.sleep(2 ** attempt)
            try:
                if db_conn and not db_conn.closed:
                    db_conn.close()
            except:
                pass
            db_conn = get_db_connection()
            
    # Reset window
    window_orders_count = 0
    window_anomaly_count = 0
    window_revenue_by_category = {}

def flush_batch(orders, items, anomalies):
    global db_conn
    if not orders:
        return
        
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with db_conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO raw_orders (order_id, customer_id, order_status, order_purchase_timestamp, order_total_payment, primary_payment_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id, order_purchase_timestamp) DO NOTHING
                """, orders)
                
                if items:
                    execute_batch(cur, """
                        INSERT INTO raw_order_items (order_id, order_purchase_timestamp, product_id, category, price, freight_value)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, items)
                    
                if anomalies:
                    execute_values(cur, """
                        INSERT INTO anomalies (order_id, order_purchase_timestamp, product_id, category, price, rolling_mean, rolling_std, z_score)
                        VALUES %s
                    """, anomalies)
                    
            db_conn.commit()
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if attempt == max_retries - 1:
                print(f"Failed to flush batch after {max_retries} attempts: {e}")
                raise
            print(f"Database error during flush_batch, retrying: {e}")
            time.sleep(2 ** attempt)
            try:
                if db_conn and not db_conn.closed:
                    db_conn.close()
            except:
                pass
            db_conn = get_db_connection()

def process_messages():
    # Wait for DB to be ready
    time.sleep(10)
    
    global db_conn
    db_conn = get_db_connection()
    
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'processor-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC])
    
    buffer_raw_orders = []
    buffer_raw_order_items = []
    buffer_anomalies = []
    
    last_flush_time = time.time()
    
    global current_window_start, window_orders_count, window_anomaly_count, window_revenue_by_category

    print("Processor started.")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()
            
            if msg is None:
                # Flush batch if timeout exceeded
                if buffer_raw_orders and (now - last_flush_time) > BATCH_TIMEOUT:
                    flush_batch(buffer_raw_orders, buffer_raw_order_items, buffer_anomalies)
                    buffer_raw_orders.clear()
                    buffer_raw_order_items.clear()
                    buffer_anomalies.clear()
                    last_flush_time = now
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            try:
                record = json.loads(msg.value().decode('utf-8'))
                order_id = record['order_id']
                order_purchase_timestamp = record['order_purchase_timestamp']
                order_dt = datetime.fromisoformat(order_purchase_timestamp)
            except Exception as e:
                print(f"Error parsing message, skipping: {e}")
                continue
            
            # Event-time windowing logic
            event_window_start = order_dt.replace(second=0, microsecond=0)
            
            if current_window_start is None:
                current_window_start = event_window_start
                
            if event_window_start > current_window_start:
                # Watermark crossed: flush previous window
                flush_window(current_window_start)
                current_window_start = event_window_start
            
            # Accumulate window metrics
            window_orders_count += 1
            
            # Prepare row for raw_orders
            buffer_raw_orders.append((
                order_id, 
                record['customer_id'], 
                record['order_status'], 
                order_purchase_timestamp,
                record.get('order_total_payment'), 
                record.get('primary_payment_type')
            ))
            
            for item in record.get('items', []):
                category = item['category']
                price = item['price']
                product_id = item['product_id']
                
                # Revenue accumulation
                window_revenue_by_category[category] = window_revenue_by_category.get(category, 0.0) + price
                
                # Anomaly detection (ignore uncategorized as it's not a statistically meaningful bucket)
                if category != 'uncategorized':
                    mean, stddev, z_score = get_z_score(category, price)
                    
                    if z_score > 3.0:
                        window_anomaly_count += 1
                        buffer_anomalies.append((
                            order_id,
                            order_purchase_timestamp,
                            product_id,
                            category,
                            price,
                            mean,
                            stddev,
                            z_score
                        ))
                
                # Raw order items
                buffer_raw_order_items.append((
                    order_id,
                    order_purchase_timestamp,
                    product_id,
                    category,
                    price,
                    item.get('freight_value')
                ))

            if len(buffer_raw_orders) >= BATCH_SIZE or (now - last_flush_time) > BATCH_TIMEOUT:
                flush_batch(buffer_raw_orders, buffer_raw_order_items, buffer_anomalies)
                buffer_raw_orders.clear()
                buffer_raw_order_items.clear()
                buffer_anomalies.clear()
                last_flush_time = now

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Fatal error in processor loop: {e}")
    finally:
        consumer.close()
        if db_conn and not db_conn.closed:
            db_conn.close()

if __name__ == "__main__":
    process_messages()
