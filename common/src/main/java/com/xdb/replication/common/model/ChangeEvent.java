package com.xdb.replication.common.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Canonical, database-agnostic change event. Debezium envelopes from every
 * connector type are adapted into this shape before they reach the apply logic,
 * so target dialects never see connector-specific JSON.
 *
 * <p>Built with {@link Builder} (Builder pattern) because most fields are optional
 * depending on the operation.
 */
public final class ChangeEvent {

    private final Operation operation;
    private final String schema;
    private final String table;
    private final Map<String, Object> key;
    private final Map<String, Object> before;
    private final Map<String, Object> after;
    private final long sourceTimestampMs;
    private final String transactionId;
    private final String topic;
    private final int partition;
    private final long offset;

    private ChangeEvent(Builder b) {
        this.operation = Objects.requireNonNull(b.operation, "operation");
        this.schema = b.schema;
        this.table = Objects.requireNonNull(b.table, "table");
        this.key = unmodifiable(b.key);
        this.before = unmodifiable(b.before);
        this.after = unmodifiable(b.after);
        this.sourceTimestampMs = b.sourceTimestampMs;
        this.transactionId = b.transactionId;
        this.topic = b.topic;
        this.partition = b.partition;
        this.offset = b.offset;
    }

    private static Map<String, Object> unmodifiable(Map<String, Object> m) {
        return m == null ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(m));
    }

    /** The row image to evaluate filters against: the old image for deletes, the new image otherwise. */
    public Map<String, Object> rowImage() {
        return operation == Operation.DELETE ? before : after;
    }

    /** {@code schema.table}, or just {@code table} when the source has no schema. */
    public String qualifiedSourceTable() {
        return schema == null || schema.isBlank() ? table : schema + "." + table;
    }

    public Operation operation() { return operation; }
    public String schema() { return schema; }
    public String table() { return table; }
    public Map<String, Object> key() { return key; }
    public Map<String, Object> before() { return before; }
    public Map<String, Object> after() { return after; }
    public long sourceTimestampMs() { return sourceTimestampMs; }
    public String transactionId() { return transactionId; }
    public String topic() { return topic; }
    public int partition() { return partition; }
    public long offset() { return offset; }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ChangeEvent{" + operation + " " + qualifiedSourceTable() + " key=" + key
                + " @" + topic + "-" + partition + ":" + offset + "}";
    }

    public static final class Builder {
        private Operation operation;
        private String schema;
        private String table;
        private Map<String, Object> key;
        private Map<String, Object> before;
        private Map<String, Object> after;
        private long sourceTimestampMs;
        private String transactionId;
        private String topic;
        private int partition;
        private long offset = -1;

        public Builder operation(Operation v) { this.operation = v; return this; }
        public Builder schema(String v) { this.schema = v; return this; }
        public Builder table(String v) { this.table = v; return this; }
        public Builder key(Map<String, Object> v) { this.key = v; return this; }
        public Builder before(Map<String, Object> v) { this.before = v; return this; }
        public Builder after(Map<String, Object> v) { this.after = v; return this; }
        public Builder sourceTimestampMs(long v) { this.sourceTimestampMs = v; return this; }
        public Builder transactionId(String v) { this.transactionId = v; return this; }
        public Builder topic(String v) { this.topic = v; return this; }
        public Builder partition(int v) { this.partition = v; return this; }
        public Builder offset(long v) { this.offset = v; return this; }

        public ChangeEvent build() {
            return new ChangeEvent(this);
        }
    }
}
