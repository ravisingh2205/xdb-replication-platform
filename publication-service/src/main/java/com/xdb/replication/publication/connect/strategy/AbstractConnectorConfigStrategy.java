package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.TopicRouting;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Template Method pattern: {@link #build} fixes the overall recipe (common settings,
 * then engine-specific settings), while subclasses supply only what differs.
 *
 * <p>The common settings encode the platform's replication contract:
 * <ul>
 *   <li><b>Key = primary key</b>: Debezium uses the row's PK as the Kafka record key, so the
 *       default partitioner sends every change of one row to the same partition. That gives
 *       per-row ordering, which is all the apply side needs.</li>
 *   <li><b>Plain JSON envelopes</b> ({@code schemas.enable=false}) with before/after/source/op.</li>
 *   <li><b>No tombstones</b>: a delete is one event with the before image.</li>
 *   <li><b>Topic auto-creation</b> with a fixed partition count, so throughput can scale with
 *       consumer parallelism.</li>
 * </ul>
 */
public abstract class AbstractConnectorConfigStrategy implements ConnectorConfigStrategy {

    @Override
    public final Map<String, String> build(ConnectorContext ctx) {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("connector.class", connectorClass());
        c.put("tasks.max", "1");
        c.put("topic.prefix", ctx.topicPrefix());

        c.put("database.hostname", ctx.source().host());
        c.put("database.port", String.valueOf(ctx.source().port()));
        c.put("database.user", ctx.source().username());
        c.put("database.password", ctx.passwordValue());

        c.put("table.include.list", String.join(",", ctx.tables()));
        c.put("snapshot.mode", ctx.snapshotMode());

        c.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put("key.converter.schemas.enable", "false");
        c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put("value.converter.schemas.enable", "false");

        c.put("tombstones.on.delete", "false");
        c.put("decimal.handling.mode", "string");
        c.put("time.precision.mode", "connect");
        c.put("heartbeat.interval.ms", "10000");

        c.put("topic.creation.default.partitions", String.valueOf(ctx.partitions()));
        c.put("topic.creation.default.replication.factor", String.valueOf(ctx.replicationFactor()));
        c.put("topic.creation.default.cleanup.policy", "delete");
        c.put("topic.creation.default.retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000));

        c.put("snapshot.max.threads", String.valueOf(Math.max(1, ctx.snapshotThreads())));

        // Producer tuning for wide publications (needs connector.client.config.override.policy=All on the worker).
        c.put("producer.override.compression.type", "lz4");
        c.put("producer.override.linger.ms", "20");
        c.put("producer.override.batch.size", "262144");

        addTopicRouting(c, ctx);

        c.put("errors.log.enable", "true");
        c.put("errors.log.include.messages", "false");

        addEngineSpecific(c, ctx);
        return c;
    }

    /**
     * Collapses table topics with Debezium's ByLogicalTableRouter SMT. The router adds a table
     * identifier to the key ({@code key.enforce.uniqueness}), so rows of different tables with
     * the same PK value never collide and per-row ordering is kept. The apply side reads the
     * table from the envelope's {@code source} block, so it needs no change.
     */
    private static void addTopicRouting(Map<String, String> c, ConnectorContext ctx) {
        TopicRouting routing = ctx.topicRouting() == null ? TopicRouting.PER_TABLE : ctx.topicRouting();
        if (routing == TopicRouting.PER_TABLE) {
            return;
        }
        String prefix = java.util.regex.Pattern.quote(ctx.topicPrefix());
        c.put("transforms", "route");
        c.put("transforms.route.type", "io.debezium.transforms.ByLogicalTableRouter");
        c.put("transforms.route.key.enforce.uniqueness", "true");
        if (routing == TopicRouting.PER_SCHEMA) {
            // <prefix>.<schema>.<table> (or <prefix>.<db>.<schema>.<table>)  ->  drop the table segment
            c.put("transforms.route.topic.regex", "(" + prefix + ")\\.(.+)\\.[^.]+");
            c.put("transforms.route.topic.replacement", "$1.$2");
        } else {
            c.put("transforms.route.topic.regex", "(" + prefix + ")\\..+");
            c.put("transforms.route.topic.replacement", "$1.all");
        }
    }

    protected abstract String connectorClass();

    protected abstract void addEngineSpecific(Map<String, String> config, ConnectorContext ctx);

    /** Schema history topic, required by connectors that must replay DDL (Oracle, SQL Server). */
    protected static void addSchemaHistory(Map<String, String> c, ConnectorContext ctx) {
        c.put("schema.history.internal.kafka.bootstrap.servers", ctx.kafkaBootstrapServers());
        c.put("schema.history.internal.kafka.topic", "xdb.schemahistory." + ctx.publication());
        c.put("schema.history.internal.store.only.captured.tables.ddl", "true");
    }

    /** Replication slot and Postgres publication names allow only [a-z0-9_]. */
    protected static String sanitize(String s) {
        return s.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }
}
