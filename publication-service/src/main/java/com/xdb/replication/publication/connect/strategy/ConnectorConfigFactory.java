package com.xdb.replication.publication.connect.strategy;

import com.xdb.replication.common.model.DbType;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Factory pattern: resolves the {@link ConnectorConfigStrategy} for a database type.
 * Adding a new source engine (e.g. MySQL) means adding one strategy class; nothing
 * else in the publication flow changes (Open/Closed principle).
 */
public class ConnectorConfigFactory {

    private final Map<DbType, ConnectorConfigStrategy> strategies = new EnumMap<>(DbType.class);

    public ConnectorConfigFactory(List<ConnectorConfigStrategy> all) {
        for (ConnectorConfigStrategy s : all) {
            for (DbType t : s.supports()) {
                if (strategies.putIfAbsent(t, s) != null) {
                    throw new IllegalStateException("Two connector strategies registered for " + t);
                }
            }
        }
    }

    public static ConnectorConfigFactory defaults() {
        return new ConnectorConfigFactory(List.of(
                new PostgresConnectorConfigStrategy(),
                new OracleConnectorConfigStrategy(),
                new SqlServerConnectorConfigStrategy()));
    }

    public Map<String, String> build(ConnectorContext ctx) {
        ConnectorConfigStrategy s = strategies.get(ctx.source().dbType());
        if (s == null) {
            throw new IllegalArgumentException("No CDC connector strategy for " + ctx.source().dbType());
        }
        return s.build(ctx);
    }
}
