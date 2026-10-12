package com.xdb.replication.common.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Transactional Outbox pattern, write side.
 *
 * <p>Services call this inside the same transaction that changes their metadata
 * tables. The event row and the metadata change commit or roll back together, so a
 * crash can never leave the database and Kafka disagreeing about a publication or
 * subscription. {@link OutboxRelay} ships the rows to Kafka afterwards.
 */
public class OutboxWriter {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OutboxWriter(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public void write(String topic, String key, Object payload) {
        try {
            String json = mapper.writeValueAsString(payload);
            jdbc.update("INSERT INTO xdb_outbox (topic, msg_key, payload) VALUES (?, ?, ?)", topic, key, json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize outbox payload for " + topic + "/" + key, e);
        }
    }

    /** Writes a Kafka tombstone (null value) so log compaction drops the key. */
    public void writeTombstone(String topic, String key) {
        jdbc.update("INSERT INTO xdb_outbox (topic, msg_key, payload) VALUES (?, ?, NULL)", topic, key);
    }
}
