package com.xdb.replication.publication.service;

import com.xdb.replication.common.events.ConfigEventType;
import com.xdb.replication.common.events.Topics;
import com.xdb.replication.common.model.RowFilter;
import com.xdb.replication.common.outbox.OutboxWriter;
import com.xdb.replication.common.web.ApiExceptions;
import com.xdb.replication.publication.api.Dtos.PublicationRequest;
import com.xdb.replication.publication.api.Dtos.PublicationUpdateRequest;
import com.xdb.replication.publication.config.ConnectProperties;
import com.xdb.replication.publication.connect.ConnectorProvisioner;
import com.xdb.replication.publication.connect.ConnectorStatus;
import com.xdb.replication.publication.connect.KafkaConnectClient;
import com.xdb.replication.publication.domain.Publication;
import com.xdb.replication.publication.domain.PublicationStatus;
import com.xdb.replication.publication.domain.SourceDatabase;
import com.xdb.replication.publication.repository.PublicationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Publication use cases. Pattern used throughout: change metadata and write the outbox
 * event in one local transaction, then call Kafka Connect outside the transaction.
 * If the Connect call fails, the publication is marked FAILED and the
 * {@link ConnectorReconciler} keeps trying, so the system converges to the desired state.
 */
@Service
public class PublicationService {

    private static final Logger log = LoggerFactory.getLogger(PublicationService.class);

    private final PublicationRepository repo;
    private final SourceDatabaseService sources;
    private final OutboxWriter outbox;
    private final ConnectorProvisioner provisioner;
    private final KafkaConnectClient connect;
    private final ReplicationSlotCleaner slotCleaner;
    private final ConnectProperties props;
    private final TransactionTemplate tx;

    public PublicationService(PublicationRepository repo, SourceDatabaseService sources, OutboxWriter outbox,
                              ConnectorProvisioner provisioner, KafkaConnectClient connect,
                              ReplicationSlotCleaner slotCleaner, ConnectProperties props, TransactionTemplate tx) {
        this.repo = repo;
        this.sources = sources;
        this.outbox = outbox;
        this.provisioner = provisioner;
        this.connect = connect;
        this.slotCleaner = slotCleaner;
        this.props = props;
        this.tx = tx;
    }

    public Publication create(PublicationRequest req) {
        Publication saved = tx.execute(s -> {
            if (repo.existsByName(req.name())) {
                throw new ApiExceptions.ConflictException("Publication '" + req.name() + "' already exists");
            }
            SourceDatabase source = sources.get(req.sourceDatabase());
            validateFilters(req.tables(), req.rowFilters());
            Publication p = new Publication(req.name(), source, req.tables(), req.rowFilters(),
                    req.partitions() != null ? req.partitions() : props.defaultPartitions(),
                    req.snapshotMode() != null ? req.snapshotMode() : "initial",
                    req.topicRouting());
            p = repo.save(p);
            publishEvent(p);
            return p;
        });
        provisionOrMarkFailed(saved.getName());
        return get(saved.getName());
    }

    public Publication update(String name, PublicationUpdateRequest req) {
        tx.executeWithoutResult(s -> {
            Publication p = get(name);
            validateFilters(req.tables(), req.rowFilters());
            p.updateDefinition(req.tables(), req.rowFilters());
            publishEvent(p);
        });
        // PUT config updates table.include.list. Newly added tables need an incremental
        // snapshot (Debezium signalling) to backfill existing rows; see README.
        provisionOrMarkFailed(name);
        return get(name);
    }

    public List<Publication> list() {
        return tx.execute(s -> repo.findAll());
    }

    public Publication get(String name) {
        return repo.findByName(name)
                .orElseThrow(() -> new ApiExceptions.NotFoundException("Publication '" + name + "' not found"));
    }

    public Optional<ConnectorStatus> connectorStatus(String name) {
        return connect.status(get(name).connectorName());
    }

    public Map<String, String> connectorConfig(String name) {
        return provisioner.previewConfig(get(name));
    }

    public Publication pause(String name) {
        connect.pause(get(name).connectorName());
        changeStatus(name, PublicationStatus.PAUSED, "Paused by user");
        return get(name);
    }

    public Publication resume(String name) {
        connect.resume(get(name).connectorName());
        changeStatus(name, PublicationStatus.PROVISIONING, "Resuming");
        return get(name);
    }

    public Publication restart(String name) {
        connect.restart(get(name).connectorName());
        changeStatus(name, PublicationStatus.PROVISIONING, "Restart requested");
        return get(name);
    }

    public void delete(String name) {
        Publication p = get(name);
        connect.delete(p.connectorName());
        // Debezium keeps the Postgres replication slot after the connector is gone; an orphan slot
        // pins WAL forever and can fill the source disk, so drop it explicitly.
        slotCleaner.cleanup(p);
        tx.executeWithoutResult(s -> {
            Publication current = get(name);
            outbox.write(Topics.CONFIG_PUBLICATIONS, name, current.toEvent(ConfigEventType.DELETED));
            outbox.writeTombstone(Topics.CONFIG_PUBLICATIONS, name);
            repo.delete(current);
        });
    }

    /** Used by the reconciler and by user actions. Emits an event only when the status changes. */
    public void changeStatus(String name, PublicationStatus status, String message) {
        tx.executeWithoutResult(s -> repo.findByName(name).ifPresent(p -> {
            if (p.changeStatus(status, message)) {
                publishEvent(p);
            }
        }));
    }

    void provisionOrMarkFailed(String name) {
        try {
            provisioner.provision(get(name));
        } catch (RuntimeException e) {
            log.warn("Provisioning connector for publication {} failed: {}", name, e.getMessage());
            changeStatus(name, PublicationStatus.FAILED, "Connector registration failed: " + e.getMessage());
            if (e instanceof IllegalArgumentException) {
                throw e;
            }
        }
    }

    private void publishEvent(Publication p) {
        outbox.write(Topics.CONFIG_PUBLICATIONS, p.getName(), p.toEvent(ConfigEventType.UPSERT));
    }

    private static void validateFilters(List<String> tables, List<RowFilter> filters) {
        if (filters == null) {
            return;
        }
        for (RowFilter f : filters) {
            boolean known = "*".equals(f.table()) || tables.stream().anyMatch(t -> t.equalsIgnoreCase(f.table()));
            if (!known) {
                throw new IllegalArgumentException("Row filter references unpublished table " + f.table());
            }
            if (f.values().isEmpty()) {
                throw new IllegalArgumentException("Row filter on " + f.table() + "." + f.column() + " has no values");
            }
        }
    }
}
