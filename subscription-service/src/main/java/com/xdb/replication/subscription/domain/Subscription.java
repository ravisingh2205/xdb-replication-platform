package com.xdb.replication.subscription.domain;

import com.xdb.replication.common.events.ConfigEventType;
import com.xdb.replication.common.events.SubscriptionEvent;
import com.xdb.replication.common.model.TableMapping;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
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
 * A subscription: which publication to consume, which target to write to, and how
 * source tables map to target tables. Legacy step 4, "Creating a subscription (subname -> pubname)".
 */
@Entity
@Table(name = "subscription")
public class Subscription {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    /** Owned by publication-service; referenced by name only (no cross-service foreign key). */
    @Column(name = "publication_name", nullable = false)
    private String publicationName;

    @ManyToOne(fetch = FetchType.EAGER, optional = false)
    @JoinColumn(name = "target_database_id")
    private TargetDatabase targetDatabase;

    @Convert(converter = TableMappingListConverter.class)
    @Column(name = "table_mappings_json", nullable = false, columnDefinition = "text")
    private List<TableMapping> tableMappings = new ArrayList<>();

    @Column(nullable = false)
    private boolean active = true;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Version
    private long version;

    protected Subscription() {
    }

    public Subscription(String name, String publicationName, TargetDatabase targetDatabase, List<TableMapping> mappings) {
        this.name = name;
        this.publicationName = publicationName;
        this.targetDatabase = targetDatabase;
        this.tableMappings = new ArrayList<>(mappings);
    }

    public void updateMappings(List<TableMapping> mappings) {
        this.tableMappings = new ArrayList<>(mappings);
        this.updatedAt = Instant.now();
    }

    public void setActive(boolean active) {
        this.active = active;
        this.updatedAt = Instant.now();
    }

    public SubscriptionEvent toEvent(ConfigEventType type) {
        return new SubscriptionEvent(type, name, publicationName, targetDatabase.toEndpoint(),
                tableMappings, active, Instant.now());
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getPublicationName() { return publicationName; }
    public TargetDatabase getTargetDatabase() { return targetDatabase; }
    public List<TableMapping> getTableMappings() { return List.copyOf(tableMappings); }
    public boolean isActive() { return active; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
