package com.xdb.replication.publication;

import com.xdb.replication.common.model.DatabaseEndpoint;
import com.xdb.replication.common.model.DbType;
import com.xdb.replication.common.model.TopicRouting;
import com.xdb.replication.publication.connect.strategy.ConnectorConfigFactory;
import com.xdb.replication.publication.connect.strategy.ConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorConfigStrategyTest {

    private final ConnectorConfigFactory factory = ConnectorConfigFactory.defaults();

    private ConnectorContext ctx(DbType type, String pdb) {
        return ctx(type, pdb, TopicRouting.PER_TABLE);
    }

    private ConnectorContext ctx(DbType type, String pdb, TopicRouting routing) {
        var ep = new DatabaseEndpoint("src", type, "db-host", type.defaultPort(), "ORCL", "dbz", "source/src", pdb, null);
        return new ConnectorContext("orders_pub", ep, "secret", List.of("public.customer", "public.orders"),
                "cdc.orders_pub", 6, (short) 3, "initial", "kafka:9092", routing, 4);
    }

    @Test
    void perTableRoutingAddsNoTransform() {
        assertThat(factory.build(ctx(DbType.POSTGRES, null))).doesNotContainKey("transforms");
    }

    @Test
    void singleRoutingCollapsesAllTablesIntoOneTopic() {
        Map<String, String> c = factory.build(ctx(DbType.POSTGRES, null, TopicRouting.SINGLE));
        assertThat(c).containsEntry("transforms.route.type", "io.debezium.transforms.ByLogicalTableRouter")
                .containsEntry("transforms.route.key.enforce.uniqueness", "true")
                .containsEntry("transforms.route.topic.replacement", "$1.all");
        String regex = c.get("transforms.route.topic.regex");
        assertThat("cdc.orders_pub.public.orders".replaceAll(regex, "$1.all")).isEqualTo("cdc.orders_pub.all");
    }

    @Test
    void perSchemaRoutingDropsTheTableSegment() {
        Map<String, String> c = factory.build(ctx(DbType.SQLSERVER, null, TopicRouting.PER_SCHEMA));
        String regex = c.get("transforms.route.topic.regex");
        assertThat("cdc.orders_pub.public.orders".replaceAll(regex, "$1.$2")).isEqualTo("cdc.orders_pub.public");
        assertThat("cdc.orders_pub.salesdb.dbo.orders".replaceAll(regex, "$1.$2")).isEqualTo("cdc.orders_pub.salesdb.dbo");
    }

    @Test
    void postgresUsesPgoutputSlotAndFilteredPublication() {
        Map<String, String> c = factory.build(ctx(DbType.POSTGRES, null));
        assertThat(c).containsEntry("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .containsEntry("plugin.name", "pgoutput")
                .containsEntry("slot.name", "xdb_orders_pub")
                .containsEntry("publication.name", "xdb_orders_pub_pub")
                .containsEntry("publication.autocreate.mode", "filtered")
                .containsEntry("topic.prefix", "cdc.orders_pub")
                .containsEntry("table.include.list", "public.customer,public.orders")
                .containsEntry("topic.creation.default.partitions", "6")
                .doesNotContainKey("schema.history.internal.kafka.topic");
    }

    @Test
    void epasSharesThePostgresStrategy() {
        assertThat(factory.build(ctx(DbType.EPAS, null)))
                .containsEntry("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    }

    @Test
    void oracleUsesLogMinerSchemaHistoryAndPdb() {
        Map<String, String> c = factory.build(ctx(DbType.ORACLE, "ORCLPDB1"));
        assertThat(c).containsEntry("connector.class", "io.debezium.connector.oracle.OracleConnector")
                .containsEntry("database.connection.adapter", "logminer")
                .containsEntry("database.pdb.name", "ORCLPDB1")
                .containsEntry("table.include.list", "PUBLIC.CUSTOMER,PUBLIC.ORDERS")
                .containsEntry("schema.history.internal.kafka.topic", "xdb.schemahistory.orders_pub");
    }

    @Test
    void sqlServerUsesDatabaseNames() {
        Map<String, String> c = factory.build(ctx(DbType.SQLSERVER, null));
        assertThat(c).containsEntry("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .containsEntry("database.names", "ORCL")
                .containsKey("schema.history.internal.kafka.bootstrap.servers");
    }

    @Test
    void everyConnectorKeysByPrimaryKeyAsPlainJson() {
        for (DbType t : DbType.values()) {
            Map<String, String> c = factory.build(ctx(t, null));
            assertThat(c).containsEntry("key.converter.schemas.enable", "false")
                    .containsEntry("value.converter.schemas.enable", "false")
                    .containsEntry("tombstones.on.delete", "false");
        }
    }
}
