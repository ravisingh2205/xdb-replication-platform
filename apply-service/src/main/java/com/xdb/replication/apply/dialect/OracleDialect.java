package com.xdb.replication.apply.dialect;

import com.xdb.replication.common.model.DbType;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Oracle: {@code MERGE INTO ... USING (SELECT ? AS col ... FROM dual)}. */
public class OracleDialect extends AbstractSqlDialect {

    @Override
    public Set<DbType> supports() {
        return EnumSet.of(DbType.ORACLE);
    }

    @Override
    public String upsertSql(TableRef table, List<String> keyColumns, List<String> columns) {
        String source = columns.stream().map(c -> "? AS " + quote(c)).collect(Collectors.joining(", "));
        String on = keyColumns.stream().map(k -> "t." + quote(k) + " = s." + quote(k)).collect(Collectors.joining(" AND "));
        List<String> updatable = nonKey(columns, keyColumns);
        StringBuilder sql = new StringBuilder()
                .append("MERGE INTO ").append(qualified(table)).append(" t USING (SELECT ").append(source)
                .append(" FROM dual) s ON (").append(on).append(")");
        if (!updatable.isEmpty()) {
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updatable.stream().map(c -> "t." + quote(c) + " = s." + quote(c)).collect(Collectors.joining(", ")));
        }
        sql.append(" WHEN NOT MATCHED THEN INSERT (").append(columnList(columns)).append(") VALUES (")
                .append(columns.stream().map(c -> "s." + quote(c)).collect(Collectors.joining(", "))).append(")");
        return sql.toString();
    }

    @Override
    public String checkpointTable() {
        return "XDB_APPLY_CHECKPOINT";
    }

    @Override
    public String createCheckpointTableSql() {
        return "CREATE TABLE XDB_APPLY_CHECKPOINT ("
                + "SUBSCRIPTION VARCHAR2(64) NOT NULL, TOPIC VARCHAR2(255) NOT NULL, KAFKA_PARTITION NUMBER(10) NOT NULL, "
                + "LAST_OFFSET NUMBER(19) NOT NULL, UPDATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL, "
                + "CONSTRAINT PK_XDB_APPLY_CHECKPOINT PRIMARY KEY (SUBSCRIPTION, TOPIC, KAFKA_PARTITION))";
    }

    @Override
    public String upsertCheckpointSql() {
        return "MERGE INTO XDB_APPLY_CHECKPOINT t USING (SELECT ? AS SUBSCRIPTION, ? AS TOPIC, ? AS KAFKA_PARTITION, "
                + "? AS LAST_OFFSET FROM dual) s ON (t.SUBSCRIPTION = s.SUBSCRIPTION AND t.TOPIC = s.TOPIC "
                + "AND t.KAFKA_PARTITION = s.KAFKA_PARTITION) "
                + "WHEN MATCHED THEN UPDATE SET t.LAST_OFFSET = s.LAST_OFFSET, t.UPDATED_AT = SYSTIMESTAMP "
                + "WHEN NOT MATCHED THEN INSERT (SUBSCRIPTION, TOPIC, KAFKA_PARTITION, LAST_OFFSET, UPDATED_AT) "
                + "VALUES (s.SUBSCRIPTION, s.TOPIC, s.KAFKA_PARTITION, s.LAST_OFFSET, SYSTIMESTAMP)";
    }
}
