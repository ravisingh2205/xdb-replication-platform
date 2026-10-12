package com.xdb.replication.apply.adapter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.common.model.ChangeEvent;
import com.xdb.replication.common.model.Operation;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Optional;

/**
 * Adapter pattern: converts a Debezium change-event envelope (as produced by the
 * Postgres, Oracle or SQL Server connector with JSON converters and
 * {@code schemas.enable=false}) into the platform's canonical {@link ChangeEvent}.
 *
 * <pre>
 * key:   {"id": 42}
 * value: {"before": {...} | null,
 *         "after":  {...} | null,
 *         "source": {"connector":"postgresql","schema":"public","table":"orders","txId":123,"lsn":...,"ts_ms":...},
 *         "op": "c" | "u" | "d" | "r" | "t",
 *         "ts_ms": ...}
 * </pre>
 *
 * The {@code source} block, not the topic name, identifies the table, so the
 * connector-specific topic layouts (SQL Server adds the database name) do not matter.
 */
public class DebeziumEnvelopeAdapter {

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() { };

    private final ObjectMapper mapper;

    public DebeziumEnvelopeAdapter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @return empty for records that carry no row change (tombstones, heartbeat or schema-change messages)
     * @throws MalformedEventException if the record is not a valid Debezium envelope (sent to the DLQ)
     */
    public Optional<ChangeEvent> adapt(ConsumerRecord<String, String> record) {
        if (record.value() == null) {
            return Optional.empty();
        }
        try {
            JsonNode value = mapper.readTree(record.value());
            JsonNode envelope = value.has("payload") && value.has("schema") ? value.get("payload") : value;
            JsonNode op = envelope.get("op");
            JsonNode source = envelope.get("source");
            if (op == null || op.isNull() || source == null) {
                return Optional.empty();
            }
            String schema = text(source, "schema");
            String table = text(source, "table");
            if (table == null) {
                return Optional.empty();
            }
            return Optional.of(ChangeEvent.builder()
                    .operation(Operation.fromDebezium(op.asText()))
                    .schema(schema)
                    .table(table)
                    .key(record.key() == null ? null : unwrapKey(mapper.readTree(record.key())))
                    .before(toMap(envelope.get("before")))
                    .after(toMap(envelope.get("after")))
                    .sourceTimestampMs(source.path("ts_ms").asLong(envelope.path("ts_ms").asLong()))
                    .transactionId(text(source, "txId"))
                    .topic(record.topic())
                    .partition(record.partition())
                    .offset(record.offset())
                    .build());
        } catch (Exception e) {
            throw new MalformedEventException("Not a Debezium envelope at " + record.topic() + "-"
                    + record.partition() + "@" + record.offset() + ": " + e.getMessage(), e);
        }
    }

    private Map<String, Object> unwrapKey(JsonNode key) {
        JsonNode k = key.has("payload") && key.has("schema") ? key.get("payload") : key;
        return toMap(k);
    }

    private Map<String, Object> toMap(JsonNode node) {
        if (node == null || node.isNull() || !node.isObject()) {
            return null;
        }
        return mapper.convertValue(node, MAP);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    /** A record that can never be applied; routed straight to the DLQ without retries. */
    public static class MalformedEventException extends RuntimeException {
        public MalformedEventException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
