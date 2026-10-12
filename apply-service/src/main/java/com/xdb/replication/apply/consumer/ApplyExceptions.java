package com.xdb.replication.apply.consumer;

import com.xdb.replication.apply.adapter.DebeziumEnvelopeAdapter.MalformedEventException;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

/**
 * Error classification for the retry policy.
 * <ul>
 *   <li>{@link TransientTargetException}: target unreachable, deadlock, serialization failure.
 *       Retried forever with capped exponential backoff: an outage must never dead-letter data.</li>
 *   <li>{@link DataApplyException}: constraint violation, type mismatch, missing table.
 *       Retried for a bounded time (the target may be fixed), then sent to the DLQ.</li>
 *   <li>{@link MalformedEventException}: not a Debezium envelope. Straight to the DLQ.</li>
 * </ul>
 */
public final class ApplyExceptions {

    private ApplyExceptions() {
    }

    public static class TransientTargetException extends RuntimeException {
        public TransientTargetException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class DataApplyException extends RuntimeException {
        public DataApplyException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static RuntimeException classify(Exception e) {
        if (e instanceof MalformedEventException m) {
            return m;
        }
        SQLException sql = findSql(e);
        if (sql != null && isTransient(sql)) {
            return new TransientTargetException("Target temporarily unavailable: " + sql.getMessage(), e);
        }
        return new DataApplyException(e.getMessage(), e);
    }

    /** Walks the cause chain (the container wraps listener exceptions). */
    public static <T extends Throwable> T find(Throwable t, Class<T> type) {
        while (t != null) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
            t = t.getCause();
        }
        return null;
    }

    public static boolean isSchemaError(Throwable t) {
        SQLException sql = findSql(t);
        String state = sql == null ? null : sql.getSQLState();
        return state != null && (state.startsWith("42") || state.equals("22023"));
    }

    private static SQLException findSql(Throwable t) {
        return find(t, SQLException.class);
    }

    private static boolean isTransient(SQLException e) {
        if (e instanceof SQLTransientException || e instanceof SQLRecoverableException
                || e instanceof SQLNonTransientConnectionException) {
            return true;
        }
        String state = e.getSQLState();
        // 08xxx connection errors, 40xxx transaction rollback (deadlock, serialization), 57P0x admin shutdown
        return state != null && (state.startsWith("08") || state.startsWith("40") || state.startsWith("57P0")
                || state.equals("HYT00") || state.equals("HYT01"));
    }
}
