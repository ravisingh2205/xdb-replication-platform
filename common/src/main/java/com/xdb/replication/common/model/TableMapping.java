package com.xdb.replication.common.model;

import java.util.Objects;

/**
 * Maps a published source table to a table on the subscription's target database.
 * Lets a subscription rename tables or move them into a different schema, which is
 * common when replicating heterogeneously (e.g. Postgres {@code public.orders} to
 * Oracle {@code SALES.ORDERS}).
 */
public record TableMapping(String sourceSchema, String sourceTable, String targetSchema, String targetTable) {

    public TableMapping {
        Objects.requireNonNull(sourceTable, "sourceTable");
        targetTable = targetTable == null || targetTable.isBlank() ? sourceTable : targetTable;
        targetSchema = targetSchema == null || targetSchema.isBlank() ? sourceSchema : targetSchema;
    }

    public String qualifiedSource() {
        return sourceSchema == null || sourceSchema.isBlank() ? sourceTable : sourceSchema + "." + sourceTable;
    }

    public boolean matchesSource(String schema, String table) {
        boolean schemaOk = sourceSchema == null || sourceSchema.isBlank() || sourceSchema.equalsIgnoreCase(schema);
        return schemaOk && sourceTable.equalsIgnoreCase(table);
    }

    /** Identity mapping: same schema and table name on the target. */
    public static TableMapping identity(String qualifiedTable) {
        int dot = qualifiedTable.indexOf('.');
        if (dot < 0) {
            return new TableMapping(null, qualifiedTable, null, qualifiedTable);
        }
        String schema = qualifiedTable.substring(0, dot);
        String table = qualifiedTable.substring(dot + 1);
        return new TableMapping(schema, table, schema, table);
    }
}
