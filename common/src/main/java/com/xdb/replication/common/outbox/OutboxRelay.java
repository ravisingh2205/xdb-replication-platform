package com.xdb.replication.common.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transactional Outbox pattern, relay side. Polls unpublished rows and sends them
 * to Kafka in insertion order.
 *
 * <p>{@code FOR UPDATE SKIP LOCKED} lets several replicas run the relay without
 * sending the same row twice in parallel. A row is marked published only after
 * Kafka acknowledges it, so delivery is at-least-once; consumers are idempotent
 * because config events carry full state.
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = tx;
    }

    @Scheduled(fixedDelayString = "${xdb.outbox.poll-interval-ms:500}")
    public void relay() {
        Integer sent = tx.execute(status -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT id, topic, msg_key, payload FROM xdb_outbox WHERE published_at IS NULL "
                            + "ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED", BATCH);
            for (Map<String, Object> row : rows) {
                long id = ((Number) row.get("id")).longValue();
                String topic = (String) row.get("topic");
                String key = (String) row.get("msg_key");
                String payload = (String) row.get("payload");
                try {
                    kafka.send(topic, key, payload).get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Stop at the first failure to keep per-key ordering; the rows stay unpublished.
                    log.warn("Outbox relay failed for id={} topic={}: {}", id, topic, e.getMessage());
                    return rows.indexOf(row);
                }
                jdbc.update("UPDATE xdb_outbox SET published_at = now() WHERE id = ?", id);
            }
            return rows.size();
        });
        if (sent != null && sent > 0) {
            log.debug("Outbox relay published {} event(s)", sent);
        }
    }
}
