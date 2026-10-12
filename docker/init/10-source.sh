#!/bin/bash
# Source database: sample schema + data, before images for row filters, Debezium user.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/orders-schema.sql.tmpl
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
-- Full before images on UPDATE/DELETE: needed when a row moves out of a row filter.
ALTER TABLE customer   REPLICA IDENTITY FULL;
ALTER TABLE orders     REPLICA IDENTITY FULL;
ALTER TABLE order_item REPLICA IDENTITY FULL;
ALTER TABLE payment    REPLICA IDENTITY FULL;

INSERT INTO customer (id, name, email, region) VALUES
  (1, 'Asha Rao',     'asha@example.com',   'APAC'),
  (2, 'Lukas Weber',  'lukas@example.com',  'EMEA'),
  (3, 'Maria Lopez',  'maria@example.com',  'AMER'),
  (4, 'Kenji Sato',   'kenji@example.com',  'APAC');
INSERT INTO orders (id, customer_id, status, order_date, total) VALUES
  (100, 1, 'PAID', '2026-09-01', 150.00),
  (101, 2, 'NEW',  '2026-09-02',  80.50),
  (102, 4, 'PAID', '2026-09-03', 999.99);
INSERT INTO order_item VALUES (100, 1, 'SKU-1', 2, 50.00), (100, 2, 'SKU-2', 1, 50.00), (101, 1, 'SKU-3', 1, 80.50),
                              (102, 1, 'SKU-9', 1, 999.99);
INSERT INTO payment (id, order_id, amount, method) VALUES (500, 100, 150.00, 'CARD'), (501, 102, 999.99, 'UPI');

-- Least-privilege CDC user for Debezium.
CREATE ROLE dbz WITH LOGIN REPLICATION PASSWORD 'dbz';
GRANT USAGE ON SCHEMA public TO dbz;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbz;
-- publication.autocreate.mode=filtered needs to own the published tables or a pre-made publication;
-- for the demo, let it create publications on this database.
GRANT CREATE ON DATABASE orders TO dbz;
ALTER TABLE customer OWNER TO dbz; ALTER TABLE orders OWNER TO dbz;
ALTER TABLE order_item OWNER TO dbz; ALTER TABLE payment OWNER TO dbz;
SQL
