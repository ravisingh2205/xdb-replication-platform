package com.xdb.replication.common.model;

/**
 * Connection details for a source or target database. The password is never part of
 * this record; {@code secretRef} points into the configured {@code SecretStore}
 * (AWS Secrets Manager in production).
 *
 * @param pdbName    Oracle pluggable database name (Oracle only; null otherwise)
 * @param urlOptions extra JDBC URL options, e.g. {@code sslmode=require} (the console's "URL Options" field)
 */
public record DatabaseEndpoint(
        String name,
        DbType dbType,
        String host,
        int port,
        String database,
        String username,
        String secretRef,
        String pdbName,
        String urlOptions) {

    public String jdbcUrl() {
        String db = dbType == DbType.ORACLE && pdbName != null && !pdbName.isBlank() ? pdbName : database;
        return dbType.jdbcUrl(host, port, db, urlOptions);
    }
}
