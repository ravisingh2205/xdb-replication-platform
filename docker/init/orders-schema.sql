-- Sample "order" domain from the legacy demo: customer, orders, order_item, payment.
-- Used for both the source and the target database in docker-compose.

CREATE TABLE IF NOT EXISTS customer (
    id          BIGINT PRIMARY KEY,
    name        VARCHAR(120) NOT NULL,
    email       VARCHAR(200),
    region      VARCHAR(10)  NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT PRIMARY KEY,
    customer_id BIGINT       NOT NULL,
    status      VARCHAR(20)  NOT NULL,
    order_date  DATE         NOT NULL,
    total       NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS order_item (
    order_id    BIGINT        NOT NULL,
    line_no     INT           NOT NULL,
    sku         VARCHAR(40)   NOT NULL,
    quantity    INT           NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS payment (
    id          BIGINT PRIMARY KEY,
    order_id    BIGINT        NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    method      VARCHAR(20)   NOT NULL,
    paid_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
