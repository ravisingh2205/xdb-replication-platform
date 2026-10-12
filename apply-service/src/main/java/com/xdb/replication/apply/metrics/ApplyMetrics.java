package com.xdb.replication.apply.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Apply metrics, scraped by Prometheus / CloudWatch Container Insights:
 * <ul>
 *   <li>{@code xdb_apply_events_total{subscription,op}}: rows applied</li>
 *   <li>{@code xdb_apply_skipped_total{subscription,reason}}: filtered or unmapped</li>
 *   <li>{@code xdb_apply_duplicates_total}: redeliveries suppressed by the checkpoint</li>
 *   <li>{@code xdb_apply_failures_total}, {@code xdb_apply_dead_lettered_total}</li>
 *   <li>{@code xdb_apply_batch_seconds}: batch transaction time</li>
 *   <li>{@code xdb_apply_replication_latency_ms}: source commit to target apply (end-to-end lag)</li>
 * </ul>
 * Kafka consumer lag ({@code kafka_consumer_fetch_manager_records_lag_max}) comes from the
 * Micrometer consumer listener; KEDA scales on lag directly from Kafka.
 */
@Component
public class ApplyMetrics {

    private final MeterRegistry registry;

    public ApplyMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void applied(String subscription, String op, long sourceTimestampMs) {
        Counter.builder("xdb.apply.events").tag("subscription", subscription).tag("op", op)
                .register(registry).increment();
        if (sourceTimestampMs > 0) {
            DistributionSummary.builder("xdb.apply.replication.latency").baseUnit("ms")
                    .tag("subscription", subscription).publishPercentiles(0.5, 0.95, 0.99)
                    .register(registry).record(Math.max(0, System.currentTimeMillis() - sourceTimestampMs));
        }
    }

    public void skipped(String subscription, String reason) {
        Counter.builder("xdb.apply.skipped").tag("subscription", subscription).tag("reason", reason)
                .register(registry).increment();
    }

    public void duplicate(String subscription) {
        Counter.builder("xdb.apply.duplicates").tag("subscription", subscription).register(registry).increment();
    }

    public void failed(String subscription) {
        Counter.builder("xdb.apply.failures").tag("subscription", subscription).register(registry).increment();
    }

    public void deadLettered(String subscription) {
        Counter.builder("xdb.apply.dead.lettered").tag("subscription", subscription).register(registry).increment();
    }

    public void batch(String subscription, int size, long nanos) {
        Timer.builder("xdb.apply.batch").tag("subscription", subscription)
                .register(registry).record(Duration.ofNanos(nanos));
        DistributionSummary.builder("xdb.apply.batch.size").tag("subscription", subscription)
                .register(registry).record(size);
    }

    public double count(String name, String subscription) {
        return registry.find(name).tag("subscription", subscription).counters().stream()
                .mapToDouble(Counter::count).sum();
    }
}
