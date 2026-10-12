package com.xdb.replication.publication.service;

import com.xdb.replication.publication.connect.ConnectorProvisioner;
import com.xdb.replication.publication.connect.ConnectorStatus;
import com.xdb.replication.publication.connect.KafkaConnectClient;
import com.xdb.replication.publication.domain.Publication;
import com.xdb.replication.publication.domain.PublicationStatus;
import com.xdb.replication.publication.repository.PublicationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.EnumSet;
import java.util.Optional;

/**
 * Reconciliation loop (the same idea as a Kubernetes controller): periodically compare
 * the desired state in metadata with the actual connector state in Kafka Connect and
 * converge them.
 * <ul>
 *   <li>Connector missing (Connect cluster rebuilt, earlier registration failed): re-create it.</li>
 *   <li>Connector or task FAILED: restart failed tasks and surface the error in the UI.</li>
 *   <li>All tasks RUNNING: mark the publication RUNNING.</li>
 * </ul>
 * This replaces the legacy Quartz-driven polling with self-healing behaviour.
 */
@Component
public class ConnectorReconciler {

    private static final Logger log = LoggerFactory.getLogger(ConnectorReconciler.class);

    private final PublicationRepository repo;
    private final PublicationService publications;
    private final KafkaConnectClient connect;
    private final ConnectorProvisioner provisioner;

    public ConnectorReconciler(PublicationRepository repo, PublicationService publications,
                               KafkaConnectClient connect, ConnectorProvisioner provisioner) {
        this.repo = repo;
        this.publications = publications;
        this.connect = connect;
        this.provisioner = provisioner;
    }

    @Scheduled(initialDelayString = "15000", fixedDelayString = "${xdb.connect.reconcile-interval-ms:30000}")
    public void reconcile() {
        var desiredActive = EnumSet.of(PublicationStatus.PROVISIONING, PublicationStatus.RUNNING, PublicationStatus.FAILED);
        for (Publication p : repo.findByStatusIn(desiredActive)) {
            try {
                reconcileOne(p);
            } catch (RuntimeException e) {
                // Connect unreachable or circuit open: try again next round.
                log.warn("Reconcile of publication {} skipped: {}", p.getName(), e.getMessage());
            }
        }
    }

    private void reconcileOne(Publication p) {
        Optional<ConnectorStatus> status = connect.status(p.connectorName());
        if (status.isEmpty()) {
            log.info("Connector {} missing, re-creating", p.connectorName());
            provisioner.provision(p);
            publications.changeStatus(p.getName(), PublicationStatus.PROVISIONING, "Connector (re)created");
            return;
        }
        ConnectorStatus s = status.get();
        if (s.hasFailure()) {
            log.warn("Connector {} failed: {}; restarting failed tasks", p.connectorName(), s.failureSummary());
            publications.changeStatus(p.getName(), PublicationStatus.FAILED, s.failureSummary());
            connect.restart(p.connectorName());
        } else if (s.isRunning()) {
            publications.changeStatus(p.getName(), PublicationStatus.RUNNING, "Streaming");
        }
    }
}
