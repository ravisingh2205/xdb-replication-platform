package com.xdb.replication.common.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Publication row filter, the modern equivalent of xDB's per-table publication
 * filter (for example {@code customer.region IN ('APAC','EMEA')}).
 *
 * @param table    {@code schema.table} the filter applies to, or {@code *} for all tables
 * @param column   column name (matched case-insensitively, since Oracle upper-cases identifiers)
 * @param operator comparison operator
 * @param values   one value for EQ/NE, one or more for IN/NOT_IN
 */
public record RowFilter(String table, String column, Operator operator, List<String> values) {

    public enum Operator { EQ, NE, IN, NOT_IN }

    public RowFilter {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(operator, "operator");
        values = values == null ? List.of() : List.copyOf(values);
    }

    public boolean appliesTo(String qualifiedTable) {
        return "*".equals(table) || table.equalsIgnoreCase(qualifiedTable);
    }

    /** Evaluates the filter against a row image. A row without the column never matches. */
    public boolean matches(Map<String, Object> row) {
        Object raw = lookupIgnoreCase(row, column);
        if (raw == null) {
            return false;
        }
        String value = String.valueOf(raw);
        boolean contained = values.stream().anyMatch(v -> v.equals(value));
        return switch (operator) {
            case EQ, IN -> contained;
            case NE, NOT_IN -> !contained;
        };
    }

    private static Object lookupIgnoreCase(Map<String, Object> row, String column) {
        if (row == null) {
            return null;
        }
        Object direct = row.get(column);
        if (direct != null) {
            return direct;
        }
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey().equalsIgnoreCase(column)) {
                return e.getValue();
            }
        }
        return null;
    }
}
