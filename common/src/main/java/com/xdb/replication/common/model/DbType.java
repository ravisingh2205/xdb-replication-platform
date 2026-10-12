package com.xdb.replication.common.model;

/**
 * Supported source/target database engines. The same enum drives both
 * the Debezium connector selection (source side) and the SQL dialect (target side).
 */
public enum DbType {

    POSTGRES(5432),
    /** EDB Postgres Advanced Server: Postgres wire protocol, logical decoding via pgoutput. */
    EPAS(5444),
    ORACLE(1521),
    SQLSERVER(1433);

    private final int defaultPort;

    DbType(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    public int defaultPort() {
        return defaultPort;
    }

    public boolean isPostgresFamily() {
        return this == POSTGRES || this == EPAS;
    }

    /** Builds a JDBC URL; {@code options} are appended in the driver's own syntax. */
    public String jdbcUrl(String host, int port, String database, String options) {
        String base = jdbcUrl(host, port, database);
        if (options == null || options.isBlank()) {
            return base;
        }
        return switch (this) {
            case POSTGRES, EPAS -> base + "?" + options.replaceFirst("^[?&]", "");
            case SQLSERVER -> base + ";" + options.replaceFirst("^;", "");
            // Oracle thin URLs carry options as connection properties, not URL suffixes.
            case ORACLE -> base;
        };
    }

    public String jdbcUrl(String host, int port, String database) {
        return switch (this) {
            case POSTGRES, EPAS -> "jdbc:postgresql://%s:%d/%s".formatted(host, port, database);
            case ORACLE -> "jdbc:oracle:thin:@//%s:%d/%s".formatted(host, port, database);
            case SQLSERVER -> "jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=true;trustServerCertificate=true"
                    .formatted(host, port, database);
        };
    }
}
