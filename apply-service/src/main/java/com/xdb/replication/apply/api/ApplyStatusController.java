package com.xdb.replication.apply.api;

import com.xdb.replication.apply.consumer.SubscriptionRuntime;
import com.xdb.replication.apply.consumer.SubscriptionRuntimeManager;
import com.xdb.replication.apply.metrics.ApplyMetrics;
import com.xdb.replication.common.events.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Runtime view for the console's monitoring panel: state, consumer-group lag per
 * partition, counters and last error, per subscription, as seen by this pod.
 */
@RestController
@RequestMapping("/api/apply")
public class ApplyStatusController {

    public record PartitionLag(String topic, int partition, long committed, long end, long lag) {
    }

    public record SubscriptionStatus(String subscription, String publication, String target, String targetType,
                                     String state, String consumerGroup, List<String> assignedToThisPod,
                                     long totalLag, List<PartitionLag> partitions, double applied, double skipped,
                                     double duplicatesSuppressed, double deadLettered, Instant lastAppliedAt,
                                     Long replicationDelayMs, String lastError, Instant lastErrorAt) {
    }

    private final SubscriptionRuntimeManager manager;
    private final AdminClient admin;
    private final ApplyMetrics metrics;

    public ApplyStatusController(SubscriptionRuntimeManager manager, AdminClient admin, ApplyMetrics metrics) {
        this.manager = manager;
        this.admin = admin;
        this.metrics = metrics;
    }

    @GetMapping("/subscriptions")
    public List<SubscriptionStatus> subscriptions() {
        return manager.runtimes().stream()
                .sorted(Comparator.comparing(SubscriptionRuntime::name))
                .map(this::status)
                .toList();
    }

    private SubscriptionStatus status(SubscriptionRuntime r) {
        var cfg = r.config();
        String group = Topics.applyConsumerGroup(r.name());
        List<PartitionLag> lags = lag(group);
        long lastTs = r.lastSourceTimestampMs();
        return new SubscriptionStatus(
                r.name(), cfg.publication().name(), cfg.subscription().target().name(),
                cfg.subscription().target().dbType().name(), r.state().name(), group,
                r.assignedPartitions().stream().map(TopicPartition::toString).sorted().toList(),
                lags.stream().mapToLong(PartitionLag::lag).sum(), lags,
                metrics.count("xdb.apply.events", r.name()),
                metrics.count("xdb.apply.skipped", r.name()),
                metrics.count("xdb.apply.duplicates", r.name()),
                metrics.count("xdb.apply.dead.lettered", r.name()),
                r.lastAppliedAt(),
                lastTs > 0 ? Math.max(0, System.currentTimeMillis() - lastTs) : null,
                r.lastError(), r.lastErrorAt());
    }

    /** Lag = log end offset minus the group's committed offset, per partition. */
    private List<PartitionLag> lag(String group) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            if (committed.isEmpty()) {
                return List.of();
            }
            Map<TopicPartition, OffsetSpec> request = committed.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(request).all().get(5, TimeUnit.SECONDS);
            return committed.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .map(e -> {
                        long end = ends.get(e.getKey()).offset();
                        long c = e.getValue().offset();
                        return new PartitionLag(e.getKey().topic(), e.getKey().partition(), c, end, Math.max(0, end - c));
                    })
                    .sorted(Comparator.comparing(PartitionLag::topic).thenComparing(PartitionLag::partition))
                    .toList();
        } catch (Exception e) {
            return List.of();
        }
    }
}
