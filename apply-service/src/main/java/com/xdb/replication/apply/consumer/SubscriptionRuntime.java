package com.xdb.replication.apply.consumer;

import com.xdb.replication.apply.adapter.DebeziumEnvelopeAdapter;
import com.xdb.replication.apply.consumer.RowFilterEvaluator.Decision;
import com.xdb.replication.apply.metrics.ApplyMetrics;
import com.xdb.replication.apply.state.RuntimeConfig;
import com.xdb.replication.apply.writer.TargetWriter;
import com.xdb.replication.common.model.ChangeEvent;
import com.xdb.replication.common.model.TableMapping;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.support.Acknowledgment;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The apply loop for ONE subscription: its own consumer group, its own connection pool,
 * its own offsets and checkpoints. A slow or broken target only stalls its own subscription.
 *
 * <h2>Per batch (one Kafka poll)</h2>
 * <ol>
 *   <li>Adapt each Debezium envelope to a {@link ChangeEvent}.</li>
 *   <li>Skip records at or below the stored checkpoint (already applied before a crash).</li>
 *   <li>Route by table mapping, apply the publication's row filters.</li>
 *   <li>UPSERT / DELETE / TRUNCATE on the target, then upsert the checkpoint row,
 *       all in <b>one target transaction</b>.</li>
 *   <li>Commit, then acknowledge so the container commits the Kafka offsets.</li>
 * </ol>
 * If the batch transaction fails, records are re-applied one transaction each to isolate the
 * bad one; the records before it are committed and the error handler retries or dead-letters
 * the failing record (see {@link ApplyExceptions}).
 *
 * <h2>Ordering</h2>
 * Kafka key = source primary key, so all changes to one row are on one partition, and a
 * partition is consumed by exactly one thread, in offset order. Different rows are applied in
 * parallel across partitions.
 */
public class SubscriptionRuntime implements BatchAcknowledgingMessageListener<String, String>,
        ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionRuntime.class);

    public enum State { STARTING, RUNNING, PAUSED, STOPPED }

    private final String name;
    private final AtomicReference<RuntimeConfig> config;
    private final HikariDataSource dataSource;
    private final TargetWriter writer;
    private final DebeziumEnvelopeAdapter adapter;
    private final ApplyMetrics metrics;
    private final Map<String, Long> checkpoints = new ConcurrentHashMap<>();

    private volatile ConcurrentMessageListenerContainer<String, String> container;
    private volatile State state = State.STARTING;
    private volatile String lastError;
    private volatile Instant lastErrorAt;
    private volatile Instant lastAppliedAt;
    private volatile long lastSourceTimestampMs;

    public SubscriptionRuntime(RuntimeConfig initial, HikariDataSource dataSource, TargetWriter writer,
                               DebeziumEnvelopeAdapter adapter, ApplyMetrics metrics) {
        this.name = initial.name();
        this.config = new AtomicReference<>(initial);
        this.dataSource = dataSource;
        this.writer = writer;
        this.adapter = adapter;
        this.metrics = metrics;
    }

    // ------------------------------------------------------------------ lifecycle

    public void attach(ConcurrentMessageListenerContainer<String, String> container) {
        this.container = container;
    }

    /** Creates the checkpoint table if needed. Tolerates an unreachable target: the consumer retries later. */
    public void prepareTarget() {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            writer.ensureCheckpointTable(c);
        } catch (SQLException e) {
            recordError("Target not ready: " + e.getMessage());
            log.warn("[{}] target not ready yet: {}", name, e.getMessage());
        }
    }

    public void start() {
        container.start();
        state = config.get().subscription().active() ? State.RUNNING : State.PAUSED;
        if (state == State.PAUSED) {
            container.pause();
        }
    }

    public void updateConfig(RuntimeConfig next) {
        config.set(next);
        if (next.subscription().active() && state == State.PAUSED) {
            container.resume();
            state = State.RUNNING;
            log.info("[{}] resumed", name);
        } else if (!next.subscription().active() && state == State.RUNNING) {
            container.pause();
            state = State.PAUSED;
            log.info("[{}] paused", name);
        }
    }

    public void stop() {
        state = State.STOPPED;
        if (container != null) {
            container.stop();
        }
        dataSource.close();
    }

    // ------------------------------------------------------------------ rebalance

    /**
     * On assignment, reload checkpoints from the target and seek past anything already applied.
     * This covers the window where the target transaction committed but the Kafka offset commit
     * did not (crash between the two): the record is never applied twice.
     */
    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        try (Connection c = dataSource.getConnection()) {
            checkpoints.putAll(writer.loadCheckpoints(c, name));
        } catch (SQLException e) {
            log.warn("[{}] could not load checkpoints, relying on committed offsets: {}", name, e.getMessage());
            return;
        }
        for (TopicPartition tp : partitions) {
            Long last = checkpoints.get(TargetWriter.checkpointKey(tp.topic(), tp.partition()));
            if (last != null && consumer.position(tp) <= last) {
                log.info("[{}] {} seeking to {} from target checkpoint", name, tp, last + 1);
                consumer.seek(tp, last + 1);
            }
        }
        log.info("[{}] assigned {}", name, partitions);
    }

    // ------------------------------------------------------------------ apply

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records.isEmpty()) {
            return;
        }
        RuntimeConfig cfg = config.get();
        long started = System.nanoTime();
        try {
            applyInTransaction(cfg, records);
        } catch (Exception batchFailure) {
            log.debug("[{}] batch of {} failed ({}); isolating the failing record", name, records.size(),
                    batchFailure.getMessage());
            for (int i = 0; i < records.size(); i++) {
                try {
                    applyInTransaction(cfg, List.of(records.get(i)));
                } catch (Exception e) {
                    if (ApplyExceptions.isSchemaError(e)) {
                        writer.invalidateMetadata();
                    }
                    RuntimeException classified = ApplyExceptions.classify(e);
                    ConsumerRecord<String, String> r = records.get(i);
                    recordError(r.topic() + "-" + r.partition() + "@" + r.offset() + ": " + classified.getMessage());
                    metrics.failed(name);
                    // Records before index i are committed; the error handler retries or dead-letters record i.
                    throw new BatchListenerFailedException(classified.getMessage(), classified, i);
                }
            }
        }
        metrics.batch(name, records.size(), System.nanoTime() - started);
        ack.acknowledge();
    }

    private void applyInTransaction(RuntimeConfig cfg, List<ConsumerRecord<String, String>> records) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            Map<String, Long> batchCheckpoints = new HashMap<>();
            Map<String, ConsumerRecord<String, String>> lastRecordPerPartition = new HashMap<>();
            try {
                for (ConsumerRecord<String, String> r : records) {
                    String key = TargetWriter.checkpointKey(r.topic(), r.partition());
                    Long applied = checkpoints.get(key);
                    if (applied != null && r.offset() <= applied) {
                        metrics.duplicate(name);
                        continue;
                    }
                    applyOne(c, cfg, r);
                    batchCheckpoints.merge(key, r.offset(), Math::max);
                    lastRecordPerPartition.put(key, r);
                }
                for (Map.Entry<String, Long> cp : batchCheckpoints.entrySet()) {
                    ConsumerRecord<String, String> r = lastRecordPerPartition.get(cp.getKey());
                    writer.saveCheckpoint(c, name, r.topic(), r.partition(), cp.getValue());
                }
                c.commit();
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(c);
                throw e;
            }
            checkpoints.putAll(batchCheckpoints);
        }
    }

    private void applyOne(Connection c, RuntimeConfig cfg, ConsumerRecord<String, String> record) throws SQLException {
        Optional<ChangeEvent> adapted = adapter.adapt(record);
        if (adapted.isEmpty()) {
            return;
        }
        ChangeEvent e = adapted.get();
        Optional<TableMapping> mapping = cfg.mappingFor(e.schema(), e.table());
        if (mapping.isEmpty()) {
            metrics.skipped(name, "unmapped");
            return;
        }
        TableMapping m = mapping.get();
        Decision decision = RowFilterEvaluator.evaluate(e, cfg.publication().rowFilters());
        switch (decision) {
            case SKIP -> {
                metrics.skipped(name, "filtered");
                return;
            }
            case DELETE_INSTEAD -> writer.delete(c, m.targetSchema(), m.targetTable(), e.key(), e.before());
            case APPLY -> {
                switch (e.operation()) {
                    case SNAPSHOT, INSERT, UPDATE -> writer.upsert(c, m.targetSchema(), m.targetTable(), e.key(), e.after());
                    case DELETE -> writer.delete(c, m.targetSchema(), m.targetTable(), e.key(), e.before());
                    case TRUNCATE -> writer.truncate(c, m.targetSchema(), m.targetTable());
                }
            }
        }
        metrics.applied(name, decision == Decision.DELETE_INSTEAD ? "DELETE" : e.operation().name(),
                e.sourceTimestampMs());
        lastAppliedAt = Instant.now();
        lastSourceTimestampMs = e.sourceTimestampMs();
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // the connection is discarded by the pool if it is broken
        }
    }

    private void recordError(String message) {
        lastError = message;
        lastErrorAt = Instant.now();
    }

    // ------------------------------------------------------------------ status

    public String name() { return name; }
    public RuntimeConfig config() { return config.get(); }
    public State state() { return state; }
    public String lastError() { return lastError; }
    public Instant lastErrorAt() { return lastErrorAt; }
    public Instant lastAppliedAt() { return lastAppliedAt; }
    public long lastSourceTimestampMs() { return lastSourceTimestampMs; }

    public Collection<TopicPartition> assignedPartitions() {
        return container == null || container.getAssignedPartitions() == null
                ? List.of() : container.getAssignedPartitions();
    }
}
