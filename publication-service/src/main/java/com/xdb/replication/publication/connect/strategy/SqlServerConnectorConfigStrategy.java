package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.DbType;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * SQL Server: CDC through SQL Server's built-in change tables, which the SQL Server Agent
 * fills from the transaction log. Prerequisites: {@code sys.sp_cdc_enable_db} on the
 * database and {@code sys.sp_cdc_enable_table} for each published table.
 *
 * <p>Note: SQL Server topics include the database name ({@code <prefix>.<db>.<schema>.<table>}).
 * The apply side routes by the envelope's {@code source} block, not the topic name, so this is transparent.
 */
public class SqlServerConnectorConfigStrategy extends AbstractConnectorConfigStrategy {

    @Override
    public Set<DbType> supports() {
        return EnumSet.of(DbType.SQLSERVER);
    }

    @Override
    protected String connectorClass() {
        return "io.debezium.connector.sqlserver.SqlServerConnector";
    }

    @Override
    protected void addEngineSpecific(Map<String, String> c, ConnectorContext ctx) {
        c.put("database.names", ctx.source().database());
        c.put("database.encrypt", "true");
        c.put("database.trustServerCertificate", "true");
        addSchemaHistory(c, ctx);
    }
}
