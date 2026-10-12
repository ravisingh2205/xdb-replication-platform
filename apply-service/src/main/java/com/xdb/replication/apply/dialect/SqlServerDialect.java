package com.xdb.replication.apply.dialect;

import com.xdb.replication.common.model.DbType;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server: {@code MERGE ... WITH (HOLDLOCK)}. HOLDLOCK makes the match-then-insert
 * atomic, avoiding duplicate-key races under concurrent upserts.
 */
public class SqlServerDialect extends AbstractSqlDialect {

    @Override
    public Set<DbType> supports() {
        return EnumSet.of(DbType.SQLSERVER);
    }

    @Override
    public String quote(String identifier) {
        return "[" + identifier.replace("]", "]]") + "]";
    }

    @Override
    public String upsertSql(TableRef table, List<String> keyColumns, List<String> columns) {
        String on = keyColumns.stream().map(k -> "t." + quote(k) + " = s." + quote(k)).collect(Collectors.joining(" AND "));
        List<String> updatable = nonKey(columns, keyColumns);
        StringBuilder sql = new StringBuilder()
                .append("MERGE INTO ").append(qualified(table)).append(" WITH (HOLDLOCK) AS t USING (VALUES (")
                .append(placeholders(columns.size())).append(")) AS s (").append(columnList(columns)).append(") ON ")
                .append(on);
        if (!updatable.isEmpty()) {
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updatable.stream().map(c -> "t." + quote(c) + " = s." + quote(c)).collect(Collectors.joining(", ")));
        }
        sql.append(" WHEN NOT MATCHED THEN INSERT (").append(columnList(columns)).append(") VALUES (")
                .append(columns.stream().map(c -> "s." + quote(c)).collect(Collectors.joining(", "))).append(");");
        return sql.toString();
    }

    @Override
    public String createCheckpointTableSql() {
        return "CREATE TABLE xdb_apply_checkpoint ("
                + "subscription VARCHAR(64) NOT NULL, topic VARCHAR(255) NOT NULL, kafka_partition INT NOT NULL, "
                + "last_offset BIGINT NOT NULL, updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(), "
                + "CONSTRAINT pk_xdb_apply_checkpoint PRIMARY KEY (subscription, topic, kafka_partition))";
    }

    @Override
    public String upsertCheckpointSql() {
        return "MERGE INTO xdb_apply_checkpoint WITH (HOLDLOCK) AS t USING (VALUES (?, ?, ?, ?)) "
                + "AS s (subscription, topic, kafka_partition, last_offset) "
                + "ON t.subscription = s.subscription AND t.topic = s.topic AND t.kafka_partition = s.kafka_partition "
                + "WHEN MATCHED THEN UPDATE SET t.last_offset = s.last_offset, t.updated_at = SYSUTCDATETIME() "
                + "WHEN NOT MATCHED THEN INSERT (subscription, topic, kafka_partition, last_offset, updated_at) "
                + "VALUES (s.subscription, s.topic, s.kafka_partition, s.last_offset, SYSUTCDATETIME());";
    }
}
