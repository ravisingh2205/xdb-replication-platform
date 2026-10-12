package com.xdb.replication.apply.dialect;

import com.xdb.replication.common.model.DbType;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Factory pattern: resolves the {@link SqlDialect} for a target database type. */
public final class DialectFactory {

    private static final Map<DbType, SqlDialect> DIALECTS = new EnumMap<>(DbType.class);

    static {
        for (SqlDialect d : List.of(new PostgresDialect(), new OracleDialect(), new SqlServerDialect())) {
            d.supports().forEach(t -> DIALECTS.put(t, d));
        }
    }

    private DialectFactory() {
    }

    public static SqlDialect forType(DbType type) {
        SqlDialect d = DIALECTS.get(type);
        if (d == null) {
            throw new IllegalArgumentException("No SQL dialect for target type " + type);
        }
        return d;
    }
}
