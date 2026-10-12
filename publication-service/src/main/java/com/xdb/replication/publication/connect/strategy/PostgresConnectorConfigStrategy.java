package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.DbType;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL and EDB Postgres Advanced Server: WAL-based CDC via logical decoding.
 *
 * <p>Replaces both legacy xDB capture modes (triggers + shadow tables, and its own WAL
 * decoding). Debezium creates a logical replication slot so the WAL is retained until
 * changes are confirmed, and a Postgres publication limited to the published tables
 * ({@code publication.autocreate.mode=filtered}). The source needs {@code wal_level=logical}.
 */
public class PostgresConnectorConfigStrategy extends AbstractConnectorConfigStrategy {

    @Override
    public Set<DbType> supports() {
        return EnumSet.of(DbType.POSTGRES, DbType.EPAS);
    }

    @Override
    protected String connectorClass() {
        return "io.debezium.connector.postgresql.PostgresConnector";
    }

    @Override
    protected void addEngineSpecific(Map<String, String> c, ConnectorContext ctx) {
        String base = "xdb_" + sanitize(ctx.publication());
        c.put("database.dbname", ctx.source().database());
        c.put("plugin.name", "pgoutput");
        c.put("slot.name", base);
        c.put("publication.name", base + "_pub");
        c.put("publication.autocreate.mode", "filtered");
        // Keep the slot when the connector stops, so no WAL is lost across restarts or redeploys.
        // Tables without a primary key need REPLICA IDENTITY FULL to emit a usable before-image.
        c.put("slot.drop.on.stop", "false");
    }
}
