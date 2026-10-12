package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.DbType;

import java.util.Map;
import java.util.Set;

/**
 * Strategy pattern: one implementation per source database family, each producing the
 * Debezium connector config for its CDC mechanism.
 * <ul>
 *   <li>Postgres / EPAS: WAL logical decoding through the {@code pgoutput} plugin and a replication slot</li>
 *   <li>Oracle: redo/archive logs read through LogMiner</li>
 *   <li>SQL Server: the database's own CDC change tables (which are populated from the transaction log)</li>
 * </ul>
 */
public interface ConnectorConfigStrategy {

    Set<DbType> supports();

    Map<String, String> build(ConnectorContext ctx);
}
