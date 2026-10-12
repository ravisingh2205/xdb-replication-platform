package com.xdb.replication.publication.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka Connect / Debezium settings.
 *
 * @param url                         Kafka Connect REST endpoint (distributed mode cluster service)
 * @param kafkaBootstrapServers       brokers, needed by Oracle/SQL Server schema-history topics
 * @param defaultPartitions           partitions per CDC table topic, unless the publication overrides it
 * @param replicationFactor           replication factor for CDC topics (3 on MSK, 1 locally)
 * @param secretMode                  {@code inline}: pass the password in the connector config (local dev);
 *                                    {@code config-provider}: pass a placeholder resolved by a Kafka Connect
 *                                    ConfigProvider, so the password never sits in Connect's config topic
 * @param configProviderAlias         alias of the ConfigProvider on the Connect workers; the password
 *                                    becomes {@code ${<alias>:<secret name>}}
 * @param snapshotThreads             parallel snapshot threads per connector ({@code snapshot.max.threads})
 * @param reconcileIntervalMs         how often the reconciler compares desired vs actual connector state
 */
@ConfigurationProperties(prefix = "xdb.connect")
public record ConnectProperties(
        String url,
        String kafkaBootstrapServers,
        int defaultPartitions,
        short replicationFactor,
        String secretMode,
        String configProviderAlias,
        int snapshotThreads,
        long reconcileIntervalMs) {

    public ConnectProperties {
        if (defaultPartitions <= 0) defaultPartitions = 3;
        if (replicationFactor <= 0) replicationFactor = 1;
        if (secretMode == null) secretMode = "inline";
        if (configProviderAlias == null) configProviderAlias = "aws";
        if (snapshotThreads <= 0) snapshotThreads = 4;
        if (reconcileIntervalMs <= 0) reconcileIntervalMs = 30_000;
    }

    /** Placeholder resolved by the Kafka Connect worker at task start. */
    public String passwordPlaceholder(String secretName) {
        return "${" + configProviderAlias + ":" + secretName + "}";
    }

    public boolean inlineSecrets() {
        return "inline".equalsIgnoreCase(secretMode);
    }
}
