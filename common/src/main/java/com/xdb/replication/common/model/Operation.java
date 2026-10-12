package com.xdb.replication.common.model;

/** Canonical change operation, mapped from Debezium's {@code op} field. */
public enum Operation {
    /** Row read during the initial snapshot (Debezium "r"). Applied as an upsert. */
    SNAPSHOT,
    INSERT,
    UPDATE,
    DELETE,
    /** TRUNCATE (Debezium "t"); only emitted by the Postgres connector when enabled. */
    TRUNCATE;

    public static Operation fromDebezium(String op) {
        if (op == null) {
            throw new IllegalArgumentException("Debezium envelope has no 'op' field");
        }
        return switch (op) {
            case "r" -> SNAPSHOT;
            case "c" -> INSERT;
            case "u" -> UPDATE;
            case "d" -> DELETE;
            case "t" -> TRUNCATE;
            default -> throw new IllegalArgumentException("Unknown Debezium op: " + op);
        };
    }

    public boolean isUpsert() {
        return this == SNAPSHOT || this == INSERT || this == UPDATE;
    }
}
