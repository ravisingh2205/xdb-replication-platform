package com.xdb.replication.apply.dialect;

import com.xdb.replication.common.model.DbType;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** PostgreSQL and EPAS: {@code INSERT ... ON CONFLICT (key) DO UPDATE}. */
public class PostgresDialect extends AbstractSqlDialect {

    @Override
    public Set<DbType> supports() {
        return EnumSet.of(DbType.POSTGRES, DbType.EPAS);
    }

    @Override
    public String upsertSql(TableRef table, List<String> keyColumns, List<String> columns) {
        List<String> updatable = nonKey(columns, keyColumns);
        String conflict = " ON CONFLICT (" + columnList(keyColumns) + ") ";
        String action = updatable.isEmpty()
                ? "DO NOTHING"
                : "DO UPDATE SET " + updatable.stream()
                        .map(c -> quote(c) + " = EXCLUDED." + quote(c))
                        .collect(Collectors.joining(", "));
        return insertSql(table, columns) + conflict + action;
    }

    @Override
    public String createCheckpointTableSql() {
        return "CREATE TABLE IF NOT EXISTS xdb_apply_checkpoint ("
                + "subscription VARCHAR(64) NOT NULL, topic VARCHAR(255) NOT NULL, kafka_partition INT NOT NULL, "
                + "last_offset BIGINT NOT NULL, updated_at TIMESTAMP NOT NULL DEFAULT now(), "
                + "PRIMARY KEY (subscription, topic, kafka_partition))";
    }

    @Override
    public String upsertCheckpointSql() {
        return "INSERT INTO xdb_apply_checkpoint (subscription, topic, kafka_partition, last_offset, updated_at) "
                + "VALUES (?, ?, ?, ?, now()) ON CONFLICT (subscription, topic, kafka_partition) "
                + "DO UPDATE SET last_offset = EXCLUDED.last_offset, updated_at = now()";
    }
}
