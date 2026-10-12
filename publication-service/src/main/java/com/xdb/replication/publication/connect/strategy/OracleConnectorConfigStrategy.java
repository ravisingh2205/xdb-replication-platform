package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.DbType;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Oracle: redo-log based CDC through LogMiner.
 *
 * <p>Source prerequisites: ARCHIVELOG mode, supplemental logging (database-level minimal,
 * plus ALL COLUMNS on captured tables), and a LogMiner user. The Kafka Connect image must
 * contain the Oracle JDBC driver (ojdbc11), which Debezium does not ship for licensing reasons.
 */
public class OracleConnectorConfigStrategy extends AbstractConnectorConfigStrategy {

    @Override
    public Set<DbType> supports() {
        return EnumSet.of(DbType.ORACLE);
    }

    @Override
    protected String connectorClass() {
        return "io.debezium.connector.oracle.OracleConnector";
    }

    @Override
    protected void addEngineSpecific(Map<String, String> c, ConnectorContext ctx) {
        c.put("database.dbname", ctx.source().database());
        if (ctx.source().pdbName() != null && !ctx.source().pdbName().isBlank()) {
            c.put("database.pdb.name", ctx.source().pdbName());
        }
        c.put("database.connection.adapter", "logminer");
        c.put("log.mining.strategy", "online_catalog");
        c.put("log.mining.query.filter.mode", "in");
        // Oracle identifiers are upper-case unless quoted.
        c.put("table.include.list", String.join(",", ctx.tables()).toUpperCase());
        addSchemaHistory(c, ctx);
    }
}
