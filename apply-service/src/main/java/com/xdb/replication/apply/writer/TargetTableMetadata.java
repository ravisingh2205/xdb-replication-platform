package com.xdb.replication.apply.writer;

import com.xdb.replication.apply.dialect.TableRef;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cached catalog lookups against a target database. Resolves a mapping's table name to
 * the identifier case the target actually uses (Oracle upper-cases unquoted names;
 * Postgres lower-cases them) and records each column's JDBC type for {@link ValueConverter}.
 */
public class TargetTableMetadata {

    /** Columns keyed by upper-cased name, values keep the real name and JDBC type. */
    public record Table(TableRef ref, Map<String, Column> columns, List<String> primaryKey) {

        public Optional<Column> column(String name) {
            return Optional.ofNullable(columns.get(name.toUpperCase(Locale.ROOT)));
        }
    }

    public record Column(String name, int jdbcType) {
    }

    private final Map<String, Table> cache = new ConcurrentHashMap<>();

    public Table resolve(Connection c, String schema, String table) throws SQLException {
        String key = (schema == null ? "" : schema.toUpperCase(Locale.ROOT)) + "." + table.toUpperCase(Locale.ROOT);
        Table cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        Table loaded = load(c, schema, table);
        cache.put(key, loaded);
        return loaded;
    }

    /** Called when a statement fails with a schema error, so a target-side DDL change is picked up. */
    public void invalidate() {
        cache.clear();
    }

    private Table load(Connection c, String schema, String table) throws SQLException {
        DatabaseMetaData md = c.getMetaData();
        TableRef ref = findTable(md, schema, table);
        Map<String, Column> columns = new LinkedHashMap<>();
        try (ResultSet rs = md.getColumns(null, ref.schema(), ref.table(), null)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                columns.put(name.toUpperCase(Locale.ROOT), new Column(name, rs.getInt("DATA_TYPE")));
            }
        }
        List<String> pk = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(null, ref.schema(), ref.table())) {
            while (rs.next()) {
                pk.add(rs.getString("COLUMN_NAME"));
            }
        }
        return new Table(ref, columns, pk);
    }

    private static TableRef findTable(DatabaseMetaData md, String schema, String table) throws SQLException {
        // Try the names as given, then the engine's preferred case, then a case-insensitive scan.
        for (String[] candidate : new String[][]{
                {schema, table},
                {upper(schema), table.toUpperCase(Locale.ROOT)},
                {lower(schema), table.toLowerCase(Locale.ROOT)}}) {
            try (ResultSet rs = md.getTables(null, candidate[0], candidate[1], new String[]{"TABLE"})) {
                if (rs.next()) {
                    return new TableRef(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
                }
            }
        }
        try (ResultSet rs = md.getTables(null, null, null, new String[]{"TABLE"})) {
            while (rs.next()) {
                String s = rs.getString("TABLE_SCHEM");
                String t = rs.getString("TABLE_NAME");
                boolean schemaOk = schema == null || schema.isBlank() || schema.equalsIgnoreCase(s);
                if (schemaOk && table.equalsIgnoreCase(t)) {
                    return new TableRef(s, t);
                }
            }
        }
        throw new SQLException("Target table " + (schema == null ? "" : schema + ".") + table
                + " does not exist; create it on the target before subscribing", "42P01");
    }

    private static String upper(String s) {
        return s == null ? null : s.toUpperCase(Locale.ROOT);
    }

    private static String lower(String s) {
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }
}
