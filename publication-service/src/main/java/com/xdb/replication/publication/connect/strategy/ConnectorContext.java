package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.DatabaseEndpoint;
import com.xdb.replication.common.model.TopicRouting;

import java.util.List;

/**
 * Everything a {@link ConnectorConfigStrategy} needs to build one Debezium connector config.
 *
 * @param publication           publication name (lowercase, [a-z0-9_])
 * @param source                source database endpoint
 * @param passwordValue         the literal password, or a ConfigProvider placeholder such as {@code ${aws:xdb/source/x}}
 * @param tables                {@code schema.table} entries to capture
 * @param topicPrefix           Debezium {@code topic.prefix}; table topics become {@code <prefix>.<schema>.<table>}
 * @param partitions            partitions for each auto-created table topic
 * @param replicationFactor     replication factor for auto-created topics
 * @param snapshotMode          Debezium snapshot mode ({@code initial}, {@code no_data}, ...)
 * @param kafkaBootstrapServers brokers for the schema-history topic (Oracle, SQL Server)
 * @param topicRouting          one topic per table, per schema, or one for the whole publication
 * @param snapshotThreads       tables snapshotted in parallel during the initial snapshot
 */
public record ConnectorContext(
        String publication,
        DatabaseEndpoint source,
        String passwordValue,
        List<String> tables,
        String topicPrefix,
        int partitions,
        short replicationFactor,
        String snapshotMode,
        String kafkaBootstrapServers,
        TopicRouting topicRouting,
        int snapshotThreads) {
}
