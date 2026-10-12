package com.xdb.replication.apply.writer;

import com.xdb.replication.apply.dialect.SqlDialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Writes canonical row images to one target database through a {@link SqlDialect}.
 * Plain JDBC on a caller-supplied connection, so the caller controls the transaction:
 * data changes and the offset checkpoint commit together.
 *
 * <p>Key selection: the target table's primary key if it has one, otherwise the source
 * key carried in the Kafka record key, otherwise (no key at all) inserts and
 * all-column deletes.
 */
public class TargetWriter {

    private final SqlDialect dialect;
    private final TargetTableMetadata metadata = new TargetTableMetadata();

    public TargetWriter(SqlDialect dialect) {
        this.dialect = dialect;
    }

    public SqlDialect dialect() {
        return dialect;
    }

    public void invalidateMetadata() {
        metadata.invalidate();
    }

    public void upsert(Connection c, String schema, String table, Map<String, Object> sourceKey,
                       Map<String, Object> row) throws SQLException {
        TargetTableMetadata.Table t = metadata.resolve(c, schema, table);
        List<String> columns = new ArrayList<>();
        List<Param> values = new ArrayList<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            // Source columns that do not exist on the target are ignored: the target may be a subset.
            t.column(e.getKey()).ifPresent(col -> {
                columns.add(col.name());
                values.add(Param.of(e.getValue(), col.jdbcType()));
            });
        }
        if (columns.isEmpty()) {
            throw new SQLException("No source column of " + t.ref() + " exists on the target", "42703");
        }
        List<String> keys = keyColumns(t, sourceKey);
        String sql = keys.isEmpty() ? dialect.insertSql(t.ref(), columns) : dialect.upsertSql(t.ref(), keys, columns);
        execute(c, sql, values);
    }

    public void delete(Connection c, String schema, String table, Map<String, Object> sourceKey,
                       Map<String, Object> before) throws SQLException {
        TargetTableMetadata.Table t = metadata.resolve(c, schema, table);
        List<String> keys = keyColumns(t, sourceKey);
        List<String> where = new ArrayList<>();
        List<Param> values = new ArrayList<>();
        if (!keys.isEmpty()) {
            for (String k : keys) {
                Object v = lookup(sourceKey, k);
                if (v == null) {
                    v = lookup(before, k);
                }
                where.add(k);
                values.add(Param.of(v, t.column(k).map(TargetTableMetadata.Column::jdbcType).orElse(Types.OTHER)));
            }
        } else {
            // Keyless table: match on every non-null column of the before image.
            for (Map.Entry<String, Object> e : before.entrySet()) {
                if (e.getValue() != null) {
                    t.column(e.getKey()).ifPresent(col -> {
                        where.add(col.name());
                        values.add(Param.of(e.getValue(), col.jdbcType()));
                    });
                }
            }
        }
        if (where.isEmpty()) {
            throw new SQLException("Cannot delete from " + t.ref() + ": no key and no before image", "22023");
        }
        execute(c, dialect.deleteSql(t.ref(), where), values);
    }

    public void truncate(Connection c, String schema, String table) throws SQLException {
        TargetTableMetadata.Table t = metadata.resolve(c, schema, table);
        try (Statement st = c.createStatement()) {
            st.execute(dialect.truncateSql(t.ref()));
        }
    }

    // ---------------------------------------------------------------- checkpoints

    public void ensureCheckpointTable(Connection c) throws SQLException {
        DatabaseMetaData md = c.getMetaData();
        String name = dialect.checkpointTable();
        for (String candidate : List.of(name, name.toUpperCase(Locale.ROOT), name.toLowerCase(Locale.ROOT))) {
            try (ResultSet rs = md.getTables(null, null, candidate, new String[]{"TABLE"})) {
                if (rs.next()) {
                    return;
                }
            }
        }
        try (Statement st = c.createStatement()) {
            st.execute(dialect.createCheckpointTableSql());
        }
    }

    /** Last applied offset per "topic#partition" for a subscription. */
    public Map<String, Long> loadCheckpoints(Connection c, String subscription) throws SQLException {
        Map<String, Long> result = new HashMap<>();
        try (PreparedStatement ps = c.prepareStatement(dialect.selectCheckpointsSql())) {
            ps.setString(1, subscription);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(checkpointKey(rs.getString(1), rs.getInt(2)), rs.getLong(3));
                }
            }
        }
        return result;
    }

    public void saveCheckpoint(Connection c, String subscription, String topic, int partition, long offset)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(dialect.upsertCheckpointSql())) {
            ps.setString(1, subscription);
            ps.setString(2, topic);
            ps.setInt(3, partition);
            ps.setLong(4, offset);
            ps.executeUpdate();
        }
    }

    public static String checkpointKey(String topic, int partition) {
        return topic + "#" + partition;
    }

    // ---------------------------------------------------------------- helpers

    private static List<String> keyColumns(TargetTableMetadata.Table t, Map<String, Object> sourceKey) {
        if (!t.primaryKey().isEmpty()) {
            return t.primaryKey();
        }
        List<String> keys = new ArrayList<>();
        if (sourceKey != null) {
            for (String k : sourceKey.keySet()) {
                t.column(k).ifPresent(col -> keys.add(col.name()));
            }
        }
        return keys;
    }

    private static Object lookup(Map<String, Object> row, String column) {
        if (row == null) {
            return null;
        }
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey().equalsIgnoreCase(column)) {
                return e.getValue();
            }
        }
        return null;
    }

    /** A bind value plus the target column's JDBC type, so NULLs are bound with the right type on every driver. */
    private record Param(Object value, int jdbcType) {
        static Param of(Object raw, int jdbcType) {
            return new Param(ValueConverter.toJdbc(raw, jdbcType), jdbcType);
        }
    }

    private static void execute(Connection c, String sql, List<Param> values) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                Param p = values.get(i);
                if (p.value() == null) {
                    ps.setNull(i + 1, p.jdbcType() == Types.OTHER ? Types.NULL : p.jdbcType());
                } else {
                    ps.setObject(i + 1, p.value());
                }
            }
            ps.executeUpdate();
        }
    }
}
