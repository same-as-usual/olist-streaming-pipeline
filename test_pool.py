import time
import psycopg2
from psycopg2 import pool
import random
import threading

POSTGRES_URL = "postgresql://postgres:postgres@timescaledb:5432/streaming"

print("Waiting for db...")
time.sleep(5)

db_pool = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn=POSTGRES_URL)

def get_db_connection():
    return db_pool.getconn()

def return_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

def simulate_callback():
    conn = None
    try:
        conn = get_db_connection()
        # Simulate query 1
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw_orders")
            cur.fetchone()
        
        # Simulate failure in query 2 randomly
        if random.random() < 0.3:
            raise Exception("Simulated chart query failure")
            
        with conn.cursor() as cur:
            cur.execute("SELECT max(window_start) FROM metrics_1m")
            cur.fetchone()
    except Exception as e:
        pass # catch all like the dashboard
    finally:
        if conn:
            return_db_connection(conn)

# Run multiple threads simulating callbacks
def worker():
    for _ in range(50):
        simulate_callback()
        time.sleep(0.1)

threads = []
for i in range(10): # 10 concurrent users hitting the dashboard
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

for t in threads:
    t.join()

# Final connection count
conn = get_db_connection()
try:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname='streaming'")
        print(f"Final active DB connections: {cur.fetchone()[0]}")
finally:
    return_db_connection(conn)

# Close pool
db_pool.closeall()
