package com.xdb.replication.apply.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Template Method pattern: INSERT, DELETE, TRUNCATE and identifier quoting are shared;
 * subclasses only implement the engine-specific upsert and checkpoint DDL.
 */
public abstract class AbstractSqlDialect implements SqlDialect {

    @Override
    public String insertSql(TableRef table, List<String> columns) {
        return "INSERT INTO " + qualified(table) + " (" + columnList(columns) + ") VALUES (" + placeholders(columns.size()) + ")";
    }

    @Override
    public String deleteSql(TableRef table, List<String> whereColumns) {
        String where = whereColumns.stream().map(c -> quote(c) + " = ?").collect(Collectors.joining(" AND "));
        return "DELETE FROM " + qualified(table) + " WHERE " + where;
    }

    @Override
    public String truncateSql(TableRef table) {
        return "TRUNCATE TABLE " + qualified(table);
    }

    @Override
    public String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String checkpointTable() {
        return "xdb_apply_checkpoint";
    }

    protected String qualified(TableRef t) {
        return t.hasSchema() ? quote(t.schema()) + "." + quote(t.table()) : quote(t.table());
    }

    protected String columnList(List<String> columns) {
        return columns.stream().map(this::quote).collect(Collectors.joining(", "));
    }

    protected static String placeholders(int n) {
        return String.join(", ", java.util.Collections.nCopies(n, "?"));
    }

    protected static List<String> nonKey(List<String> columns, List<String> keys) {
        return columns.stream().filter(c -> keys.stream().noneMatch(k -> k.equals(c))).toList();
    }
}
