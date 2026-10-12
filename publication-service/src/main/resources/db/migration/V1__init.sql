-- publication-service owns the "publication" schema in the metadata database.
-- No other service reads these tables directly (database-per-service, enforced by schema + DB role).

CREATE TABLE source_database (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(64)  NOT NULL UNIQUE,
    db_type       VARCHAR(16)  NOT NULL,
    host          VARCHAR(255) NOT NULL,
    port          INT          NOT NULL,
    database_name VARCHAR(128) NOT NULL,
    pdb_name      VARCHAR(128),
    username      VARCHAR(128) NOT NULL,
    url_options   VARCHAR(512),
    secret_ref    VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE publication (
    id                 BIGSERIAL PRIMARY KEY,
    name               VARCHAR(64) NOT NULL UNIQUE,
    source_database_id BIGINT      NOT NULL REFERENCES source_database (id),
    tables_json        TEXT        NOT NULL,
    row_filters_json   TEXT        NOT NULL,
    partitions         INT         NOT NULL,
    snapshot_mode      VARCHAR(32) NOT NULL,
    topic_routing      VARCHAR(16) NOT NULL DEFAULT 'PER_TABLE',
    status             VARCHAR(16) NOT NULL,
    status_message     TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    version            BIGINT      NOT NULL DEFAULT 0
);

CREATE INDEX idx_publication_status ON publication (status);

-- Transactional outbox, relayed to Kafka by OutboxRelay.
CREATE TABLE xdb_outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    msg_key      VARCHAR(255) NOT NULL,
    payload      TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON xdb_outbox (id) WHERE published_at IS NULL;

-- Local (dev) secret store. Unused when xdb.secrets.provider=aws.
CREATE TABLE xdb_secret (
    ref        VARCHAR(255) PRIMARY KEY,
    ciphertext TEXT         NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
