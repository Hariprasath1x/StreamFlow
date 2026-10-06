-- Simple analytical schema for student project

CREATE TABLE IF NOT EXISTS processed_orders (
    order_id INT,
    customer_id INT,
    product VARCHAR(255),
    category VARCHAR(255),
    quantity INT,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    city VARCHAR(255),
    order_timestamp TIMESTAMP,
    order_date DATE,
    order_hour INT
);

CREATE TABLE IF NOT EXISTS invalid_orders (
    order_id INT,
    raw_data TEXT,
    error_reason VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
