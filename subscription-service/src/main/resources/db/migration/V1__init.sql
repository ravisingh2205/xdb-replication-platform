-- subscription-service owns the "subscription" schema in the metadata database.

CREATE TABLE target_database (
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

CREATE TABLE subscription (
    id                  BIGSERIAL PRIMARY KEY,
    name                VARCHAR(64) NOT NULL UNIQUE,
    publication_name    VARCHAR(64) NOT NULL,
    target_database_id  BIGINT      NOT NULL REFERENCES target_database (id),
    table_mappings_json TEXT        NOT NULL,
    active              BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    version             BIGINT      NOT NULL DEFAULT 0
);

CREATE INDEX idx_subscription_publication ON subscription (publication_name);

CREATE TABLE xdb_outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    msg_key      VARCHAR(255) NOT NULL,
    payload      TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON xdb_outbox (id) WHERE published_at IS NULL;

CREATE TABLE xdb_secret (
    ref        VARCHAR(255) PRIMARY KEY,
    ciphertext TEXT         NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
