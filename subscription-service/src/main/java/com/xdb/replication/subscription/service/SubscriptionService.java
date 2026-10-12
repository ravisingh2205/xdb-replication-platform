package com.xdb.replication.subscription.service;

import com.xdb.replication.common.events.ConfigEventType;
import com.xdb.replication.common.events.Topics;
import com.xdb.replication.common.model.TableMapping;
import com.xdb.replication.common.outbox.OutboxWriter;
import com.xdb.replication.common.web.ApiExceptions;
import com.xdb.replication.subscription.api.Dtos.SubscriptionRequest;
import com.xdb.replication.subscription.client.PublicationClient;
import com.xdb.replication.subscription.client.PublicationClient.PublicationView;
import com.xdb.replication.subscription.domain.Subscription;
import com.xdb.replication.subscription.domain.TargetDatabase;
import com.xdb.replication.subscription.repository.SubscriptionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Subscription use cases. Every state change writes a {@code SubscriptionEvent} to the outbox
 * in the same transaction; the apply-service reacts to those events (start, reconfigure, pause
 * or stop the subscription's consumer). The two services never call each other on the data path.
 */
@Service
public class SubscriptionService {

    private final SubscriptionRepository repo;
    private final TargetDatabaseService targets;
    private final PublicationClient publications;
    private final OutboxWriter outbox;

    public SubscriptionService(SubscriptionRepository repo, TargetDatabaseService targets,
                               PublicationClient publications, OutboxWriter outbox) {
        this.repo = repo;
        this.targets = targets;
        this.publications = publications;
        this.outbox = outbox;
    }

    @Transactional
    public Subscription create(SubscriptionRequest req) {
        if (repo.existsByName(req.name())) {
            throw new ApiExceptions.ConflictException("Subscription '" + req.name() + "' already exists");
        }
        PublicationView pub = publications.find(req.publication())
                .orElseThrow(() -> new ApiExceptions.NotFoundException("Publication '" + req.publication() + "' not found"));
        TargetDatabase target = targets.get(req.targetDatabase());

        List<TableMapping> mappings = req.tableMappings() == null || req.tableMappings().isEmpty()
                ? pub.tables().stream().map(TableMapping::identity).toList()
                : req.tableMappings();
        validateMappings(pub, mappings);

        Subscription s = repo.save(new Subscription(req.name(), pub.name(), target, mappings));
        emit(s);
        return s;
    }

    @Transactional
    public Subscription updateMappings(String name, List<TableMapping> mappings) {
        Subscription s = get(name);
        PublicationView pub = publications.find(s.getPublicationName())
                .orElseThrow(() -> new ApiExceptions.NotFoundException("Publication '" + s.getPublicationName() + "' not found"));
        validateMappings(pub, mappings);
        s.updateMappings(mappings);
        emit(s);
        return s;
    }

    @Transactional(readOnly = true)
    public List<Subscription> list() {
        return repo.findAll();
    }

    @Transactional(readOnly = true)
    public Subscription get(String name) {
        return repo.findByName(name)
                .orElseThrow(() -> new ApiExceptions.NotFoundException("Subscription '" + name + "' not found"));
    }

    @Transactional
    public Subscription setActive(String name, boolean active) {
        Subscription s = get(name);
        if (s.isActive() != active) {
            s.setActive(active);
            emit(s);
        }
        return s;
    }

    @Transactional
    public void delete(String name) {
        Subscription s = get(name);
        outbox.write(Topics.CONFIG_SUBSCRIPTIONS, name, s.toEvent(ConfigEventType.DELETED));
        outbox.writeTombstone(Topics.CONFIG_SUBSCRIPTIONS, name);
        repo.delete(s);
    }

    private void emit(Subscription s) {
        outbox.write(Topics.CONFIG_SUBSCRIPTIONS, s.getName(), s.toEvent(ConfigEventType.UPSERT));
    }

    private static void validateMappings(PublicationView pub, List<TableMapping> mappings) {
        for (TableMapping m : mappings) {
            boolean published = pub.tables().stream().anyMatch(t -> t.equalsIgnoreCase(m.qualifiedSource()));
            if (!published) {
                throw new IllegalArgumentException("Table " + m.qualifiedSource()
                        + " is not part of publication " + pub.name() + " " + pub.tables());
            }
        }
    }
}
