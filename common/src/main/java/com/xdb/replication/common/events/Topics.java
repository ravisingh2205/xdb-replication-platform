package com.xdb.replication.common.events;

import java.util.regex.Pattern;

/** Kafka topic naming conventions shared by every service. */
public final class Topics {

    /** Compacted topic, key = publication name. Carries full publication state (event-carried state transfer). */
    public static final String CONFIG_PUBLICATIONS = "xdb.config.publications";

    /** Compacted topic, key = subscription name. */
    public static final String CONFIG_SUBSCRIPTIONS = "xdb.config.subscriptions";

    /** Debezium writes table topics as {@code cdc.<publication>.<schema>.<table>}. */
    public static final String CDC_PREFIX = "cdc";

    private Topics() {
    }

    /** Debezium {@code topic.prefix} for a publication. */
    public static String cdcTopicPrefix(String publication) {
        return CDC_PREFIX + "." + publication;
    }

    /** Pattern matching every table topic of one publication. */
    public static Pattern cdcTopicPattern(String publication) {
        return Pattern.compile(Pattern.quote(cdcTopicPrefix(publication)) + "\\..+");
    }

    /** Debezium schema history topic (Oracle and SQL Server connectors need one). */
    public static String schemaHistoryTopic(String publication) {
        return "xdb.schemahistory." + publication;
    }

    /** Dead letter topic for records a subscription could not apply after all retries. */
    public static String deadLetterTopic(String subscription) {
        return "xdb.dlq." + subscription;
    }

    /** One consumer group per subscription, so each target has independent offsets. */
    public static String applyConsumerGroup(String subscription) {
        return "xdb-apply-" + subscription;
    }
}
