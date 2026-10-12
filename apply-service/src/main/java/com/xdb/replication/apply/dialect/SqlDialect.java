package com.xdb.replication.apply.dialect;

import com.xdb.replication.common.model.DbType;

import java.util.List;
import java.util.Set;

/**
 * Strategy pattern for the target side: generates the SQL a target engine needs.
 * All statements use positional parameters in the order of the column list passed in.
 *
 * <p>Upserts make the apply idempotent: replaying the same INSERT/UPDATE event leaves the
 * row in the same final state, which is what turns Kafka's at-least-once delivery into an
 * effectively-once result on the target.
 */
public interface SqlDialect {

    Set<DbType> supports();

    /** Insert-or-update by key. Parameters: {@code columns} in order. */
    String upsertSql(TableRef table, List<String> keyColumns, List<String> columns);

    /** Plain insert, for tables without a key. Parameters: {@code columns} in order. */
    String insertSql(TableRef table, List<String> columns);

    /** Delete by key (or by every column when the table has no key). Parameters: {@code whereColumns}. */
    String deleteSql(TableRef table, List<String> whereColumns);

    String truncateSql(TableRef table);

    String quote(String identifier);

    // ---- checkpoint table, kept in the target DB so it commits atomically with the data ----

    String checkpointTable();

    String createCheckpointTableSql();

    /** Parameters: subscription, topic, partition, offset. */
    String upsertCheckpointSql();

    /** Parameter: subscription. Returns topic, kafka_partition, last_offset. */
    default String selectCheckpointsSql() {
        return "SELECT topic, kafka_partition, last_offset FROM " + checkpointTable() + " WHERE subscription = ?";
    }
}
