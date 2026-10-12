package com.xdb.replication.apply;

import com.xdb.replication.apply.dialect.DialectFactory;
import com.xdb.replication.apply.dialect.SqlDialect;
import com.xdb.replication.apply.dialect.TableRef;
import com.xdb.replication.common.model.DbType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlDialectTest {

    private final TableRef orders = new TableRef("public", "orders");
    private final List<String> keys = List.of("id");
    private final List<String> cols = List.of("id", "status", "amount");

    @Test
    void postgresUpsertUsesOnConflict() {
        String sql = DialectFactory.forType(DbType.POSTGRES).upsertSql(orders, keys, cols);
        assertThat(sql).isEqualTo("INSERT INTO \"public\".\"orders\" (\"id\", \"status\", \"amount\") VALUES (?, ?, ?)"
                + " ON CONFLICT (\"id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\", \"amount\" = EXCLUDED.\"amount\"");
    }

    @Test
    void postgresKeyOnlyTableDoesNothingOnConflict() {
        String sql = DialectFactory.forType(DbType.EPAS).upsertSql(orders, keys, List.of("id"));
        assertThat(sql).endsWith("ON CONFLICT (\"id\") DO NOTHING");
    }

    @Test
    void oracleUpsertUsesMergeFromDual() {
        String sql = DialectFactory.forType(DbType.ORACLE).upsertSql(new TableRef("SALES", "ORDERS"), List.of("ID"),
                List.of("ID", "STATUS"));
        assertThat(sql).isEqualTo("MERGE INTO \"SALES\".\"ORDERS\" t USING (SELECT ? AS \"ID\", ? AS \"STATUS\" FROM dual) s"
                + " ON (t.\"ID\" = s.\"ID\") WHEN MATCHED THEN UPDATE SET t.\"STATUS\" = s.\"STATUS\""
                + " WHEN NOT MATCHED THEN INSERT (\"ID\", \"STATUS\") VALUES (s.\"ID\", s.\"STATUS\")");
    }

    @Test
    void sqlServerUpsertUsesMergeWithHoldlockAndBrackets() {
        String sql = DialectFactory.forType(DbType.SQLSERVER).upsertSql(new TableRef("dbo", "orders"), keys, cols);
        assertThat(sql).startsWith("MERGE INTO [dbo].[orders] WITH (HOLDLOCK) AS t USING (VALUES (?, ?, ?)) AS s ([id], [status], [amount])")
                .contains("ON t.[id] = s.[id]")
                .endsWith(";");
    }

    @Test
    void deleteUsesEveryWhereColumn() {
        SqlDialect pg = DialectFactory.forType(DbType.POSTGRES);
        assertThat(pg.deleteSql(orders, List.of("a", "b")))
                .isEqualTo("DELETE FROM \"public\".\"orders\" WHERE \"a\" = ? AND \"b\" = ?");
    }

    @Test
    void quotingEscapesEmbeddedQuotes() {
        assertThat(DialectFactory.forType(DbType.POSTGRES).quote("we\"ird")).isEqualTo("\"we\"\"ird\"");
        assertThat(DialectFactory.forType(DbType.SQLSERVER).quote("we]ird")).isEqualTo("[we]]ird]");
    }

    @Test
    void everyDialectHasCheckpointSqlWithFourParameters() {
        for (DbType t : DbType.values()) {
            SqlDialect d = DialectFactory.forType(t);
            assertThat(d.createCheckpointTableSql()).containsIgnoringCase("xdb_apply_checkpoint");
            assertThat(d.upsertCheckpointSql().chars().filter(c -> c == '?').count()).isEqualTo(4);
        }
    }
}
