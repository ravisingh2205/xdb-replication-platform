package com.xdb.replication.apply.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.apply.consumer.SubscriptionRuntimeManager;
import com.xdb.replication.common.events.ConfigEventType;
import com.xdb.replication.common.events.PublicationEvent;
import com.xdb.replication.common.events.SubscriptionEvent;
import com.xdb.replication.common.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event-Carried State Transfer: the apply-service never calls publication-service or
 * subscription-service to learn its configuration. Each pod replays the two compacted
 * config topics from offset 0 on startup (manual partition assignment, so every pod sees
 * every event), keeps the latest state in memory, and reconciles runtimes as events arrive.
 *
 * <p>This answers the three questions in the architecture diagram for each subscription:
 * which publication, which target DB, which tables/mappings.
 */
@Component
public class ConfigStateStore {

    private static final Logger log = LoggerFactory.getLogger(ConfigStateStore.class);

    private final Map<String, PublicationEvent> publications = new ConcurrentHashMap<>();
    private final Map<String, SubscriptionEvent> subscriptions = new ConcurrentHashMap<>();

    private final ObjectMapper mapper;
    private final SubscriptionRuntimeManager runtimes;

    public ConfigStateStore(ObjectMapper mapper, SubscriptionRuntimeManager runtimes) {
        this.mapper = mapper;
        this.runtimes = runtimes;
    }

    @KafkaListener(id = "xdb-config-publications", groupId = "xdb-apply-config-${HOSTNAME:local}",
            topicPartitions = @TopicPartition(topic = Topics.CONFIG_PUBLICATIONS,
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")))
    public void onPublication(ConsumerRecord<String, String> record) throws Exception {
        String name = record.key();
        PublicationEvent event = record.value() == null ? null : mapper.readValue(record.value(), PublicationEvent.class);
        if (event == null || event.type() == ConfigEventType.DELETED) {
            publications.remove(name);
        } else {
            publications.put(name, event);
        }
        // Every subscription of this publication may need its filters/topics updated.
        subscriptions.values().stream()
                .filter(s -> s.publication().equals(name))
                .forEach(s -> reconcile(s.name()));
    }

    @KafkaListener(id = "xdb-config-subscriptions", groupId = "xdb-apply-config-${HOSTNAME:local}",
            topicPartitions = @TopicPartition(topic = Topics.CONFIG_SUBSCRIPTIONS,
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")))
    public void onSubscription(ConsumerRecord<String, String> record) throws Exception {
        String name = record.key();
        SubscriptionEvent event = record.value() == null ? null : mapper.readValue(record.value(), SubscriptionEvent.class);
        if (event == null || event.type() == ConfigEventType.DELETED) {
            subscriptions.remove(name);
        } else {
            subscriptions.put(name, event);
        }
        reconcile(name);
    }

    /**
     * Safety net: retries runtimes that failed to start (e.g. subscription-service was down
     * when credentials were fetched). Reconcile is idempotent, so running it often is harmless.
     */
    @Scheduled(initialDelay = 30_000, fixedDelay = 30_000)
    public void reconcileAll() {
        subscriptions.keySet().forEach(this::reconcile);
    }

    private void reconcile(String subscription) {
        SubscriptionEvent sub = subscriptions.get(subscription);
        PublicationEvent pub = sub == null ? null : publications.get(sub.publication());
        if (sub != null && pub == null) {
            log.debug("[{}] waiting for publication {}", subscription, sub.publication());
        }
        runtimes.reconcile(subscription, sub == null || pub == null ? null : new RuntimeConfig(sub, pub));
    }
}
