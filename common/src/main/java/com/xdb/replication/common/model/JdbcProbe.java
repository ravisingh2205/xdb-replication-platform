package com.xdb.replication.common.model;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Backs the console's "Test" button and the table picker in "Create Publication":
 * opens a short-lived JDBC connection to a source or target database.
 */
public final class JdbcProbe {

    public record Result(boolean ok, String message, String product, String version) {
    }

    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
            "information_schema", "pg_catalog", "pg_toast", "sys", "system", "outln", "xdb", "ctxsys", "mdsys",
            "olapsys", "wmsys", "ordsys", "dbsnmp", "appqossys", "audsys", "gsmadmin_internal", "lbacsys",
            "guest", "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator",
            "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter", "cdc",
            "sys_catalog", "dbo_system");

    private JdbcProbe() {
    }

    public static Result test(DatabaseEndpoint ep, String password) {
        DriverManager.setLoginTimeout(10);
        try (Connection c = DriverManager.getConnection(ep.jdbcUrl(), ep.username(), password)) {
            DatabaseMetaData md = c.getMetaData();
            return new Result(true, "Connection successful", md.getDatabaseProductName(), md.getDatabaseProductVersion());
        } catch (SQLException e) {
            return new Result(false, e.getMessage(), null, null);
        }
    }

    /** User tables as {@code schema.table}, excluding system and CDC schemas. */
    public static List<String> listTables(DatabaseEndpoint ep, String password) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(ep.jdbcUrl(), ep.username(), password);
             ResultSet rs = c.getMetaData().getTables(null, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String table = rs.getString("TABLE_NAME");
                if (schema == null || SYSTEM_SCHEMAS.contains(schema.toLowerCase(Locale.ROOT))
                        || schema.toLowerCase(Locale.ROOT).startsWith("apex_")
                        || table.toLowerCase(Locale.ROOT).startsWith("xdb_apply_checkpoint")) {
                    continue;
                }
                tables.add(schema + "." + table);
            }
        }
        tables.sort(String.CASE_INSENSITIVE_ORDER);
        return tables;
    }
}
