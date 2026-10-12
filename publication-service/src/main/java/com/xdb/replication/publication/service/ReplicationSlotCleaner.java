package com.xdb.replication.publication.service;

import com.xdb.replication.common.secrets.SecretStore;
import com.xdb.replication.publication.domain.Publication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Drops the logical replication slot and Postgres publication that Debezium created
 * for a publication (Postgres / EPAS only). Best effort: a failure is logged with the
 * SQL to run manually, because an orphan slot makes the source retain WAL indefinitely.
 */
@Component
public class ReplicationSlotCleaner {

    private static final Logger log = LoggerFactory.getLogger(ReplicationSlotCleaner.class);

    private final SecretStore secrets;

    public ReplicationSlotCleaner(SecretStore secrets) {
        this.secrets = secrets;
    }

    public void cleanup(Publication p) {
        var source = p.getSourceDatabase().toEndpoint();
        if (!source.dbType().isPostgresFamily()) {
            return;
        }
        String slot = "xdb_" + p.getName().toLowerCase().replaceAll("[^a-z0-9_]", "_");
        String pgPublication = slot + "_pub";
        try (Connection c = DriverManager.getConnection(source.jdbcUrl(), source.username(), secrets.get(source.secretRef()))) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ? AND NOT active")) {
                ps.setString(1, slot);
                ps.execute();
            }
            try (Statement st = c.createStatement()) {
                st.execute("DROP PUBLICATION IF EXISTS " + pgPublication);
            }
            log.info("Dropped replication slot {} and publication {} on {}", slot, pgPublication, source.name());
        } catch (Exception e) {
            log.error("Could not drop replication slot {} on {}. Run manually: SELECT pg_drop_replication_slot('{}'); "
                    + "DROP PUBLICATION IF EXISTS {};", slot, source.name(), slot, pgPublication, e);
        }
    }
}
