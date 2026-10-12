package com.xdb.replication.publication.domain;

import com.xdb.replication.common.events.ConfigEventType;
import com.xdb.replication.common.events.PublicationEvent;
import com.xdb.replication.common.events.Topics;
import com.xdb.replication.common.model.RowFilter;
import com.xdb.replication.common.model.TopicRouting;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A publication: a set of tables (plus optional row filters) on one source database.
 * Legacy step 2, "Creating a Publication". Each publication owns exactly one Debezium connector.
 */
@Entity
@Table(name = "publication")
public class Publication {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @ManyToOne(fetch = FetchType.EAGER, optional = false)
    @JoinColumn(name = "source_database_id")
    private SourceDatabase sourceDatabase;

    @Convert(converter = JsonConverters.StringListConverter.class)
    @Column(name = "tables_json", nullable = false, columnDefinition = "text")
    private List<String> tables = new ArrayList<>();

    @Convert(converter = JsonConverters.RowFilterListConverter.class)
    @Column(name = "row_filters_json", nullable = false, columnDefinition = "text")
    private List<RowFilter> rowFilters = new ArrayList<>();

    private int partitions;

    @Column(name = "snapshot_mode", nullable = false)
    private String snapshotMode;

    @Enumerated(EnumType.STRING)
    @Column(name = "topic_routing", nullable = false)
    private TopicRouting topicRouting = TopicRouting.PER_TABLE;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PublicationStatus status = PublicationStatus.PROVISIONING;

    @Column(name = "status_message", columnDefinition = "text")
    private String statusMessage;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Version
    private long version;

    protected Publication() {
    }

    public Publication(String name, SourceDatabase sourceDatabase, List<String> tables,
                       List<RowFilter> rowFilters, int partitions, String snapshotMode, TopicRouting topicRouting) {
        this.name = name;
        this.sourceDatabase = sourceDatabase;
        this.tables = new ArrayList<>(tables);
        this.rowFilters = new ArrayList<>(rowFilters == null ? List.of() : rowFilters);
        this.partitions = partitions;
        this.snapshotMode = snapshotMode;
        this.topicRouting = topicRouting == null ? TopicRouting.PER_TABLE : topicRouting;
    }

    /** One connector per publication; the name is stable so PUT /connectors/{name}/config is idempotent. */
    public String connectorName() {
        return "xdb-" + name;
    }

    public String topicPrefix() {
        return Topics.cdcTopicPrefix(name);
    }

    public void updateDefinition(List<String> tables, List<RowFilter> rowFilters) {
        this.tables = new ArrayList<>(tables);
        this.rowFilters = new ArrayList<>(rowFilters == null ? List.of() : rowFilters);
        this.updatedAt = Instant.now();
    }

    public boolean changeStatus(PublicationStatus newStatus, String message) {
        boolean changed = this.status != newStatus;
        this.status = newStatus;
        this.statusMessage = message;
        if (changed) {
            this.updatedAt = Instant.now();
        }
        return changed;
    }

    public PublicationEvent toEvent(ConfigEventType type) {
        return new PublicationEvent(type, name, sourceDatabase.getName(), sourceDatabase.getDbType(),
                tables, rowFilters, topicPrefix(), topicRouting, status.name(), Instant.now());
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public SourceDatabase getSourceDatabase() { return sourceDatabase; }
    public List<String> getTables() { return List.copyOf(tables); }
    public List<RowFilter> getRowFilters() { return List.copyOf(rowFilters); }
    public int getPartitions() { return partitions; }
    public String getSnapshotMode() { return snapshotMode; }
    public TopicRouting getTopicRouting() { return topicRouting; }
    public PublicationStatus getStatus() { return status; }
    public String getStatusMessage() { return statusMessage; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
