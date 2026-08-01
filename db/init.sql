CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE raw_orders (
    order_id TEXT NOT NULL,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMPTZ NOT NULL,
    order_total_payment FLOAT,
    primary_payment_type TEXT,
    PRIMARY KEY (order_id, order_purchase_timestamp)
);

SELECT create_hypertable('raw_orders', 'order_purchase_timestamp');

CREATE TABLE raw_order_items (
    order_id TEXT NOT NULL,
    order_purchase_timestamp TIMESTAMPTZ NOT NULL,
    product_id TEXT NOT NULL,
    category TEXT NOT NULL,
    price FLOAT NOT NULL,
    freight_value FLOAT
);

SELECT create_hypertable('raw_order_items', 'order_purchase_timestamp');

CREATE TABLE metrics_1m (
    window_start TIMESTAMPTZ NOT NULL,
    orders_count INT NOT NULL,
    anomaly_count INT DEFAULT 0
);

SELECT create_hypertable('metrics_1m', 'window_start');

CREATE TABLE revenue_by_category_1m (
    window_start TIMESTAMPTZ NOT NULL,
    category TEXT NOT NULL,
    revenue FLOAT NOT NULL
);

SELECT create_hypertable('revenue_by_category_1m', 'window_start');

CREATE TABLE anomalies (
    order_id TEXT NOT NULL,
    order_purchase_timestamp TIMESTAMPTZ NOT NULL,
    product_id TEXT,
    category TEXT NOT NULL,
    price FLOAT NOT NULL,
    rolling_mean FLOAT,
    rolling_std FLOAT,
    z_score FLOAT
);

SELECT create_hypertable('anomalies', 'order_purchase_timestamp');
