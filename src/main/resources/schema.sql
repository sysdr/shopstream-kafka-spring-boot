CREATE SEQUENCE IF NOT EXISTS order_seq START WITH 1;
CREATE TABLE IF NOT EXISTS orders (
    order_id    VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL,
    sku         VARCHAR(32) NOT NULL,
    quantity    INT NOT NULL,
    total       DECIMAL(10, 2) NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    aggregate_id VARCHAR(32) NOT NULL,
    event_type   VARCHAR(32) NOT NULL,
    payload      VARCHAR(2000) NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP
);
