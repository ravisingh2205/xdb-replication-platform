package com.xdb.replication.apply.consumer;

import com.xdb.replication.apply.adapter.DebeziumEnvelopeAdapter;
import com.xdb.replication.apply.adapter.DebeziumEnvelopeAdapter.MalformedEventException;
import com.xdb.replication.apply.config.ApplyProperties;
import com.xdb.replication.apply.consumer.ApplyExceptions.TransientTargetException;
import com.xdb.replication.apply.credentials.CredentialProvider;
import com.xdb.replication.apply.dialect.DialectFactory;
import com.xdb.replication.apply.metrics.ApplyMetrics;
import com.xdb.replication.apply.state.RuntimeConfig;
import com.xdb.replication.apply.writer.TargetWriter;
import com.xdb.replication.common.events.Topics;
import com.xdb.replication.common.model.DatabaseEndpoint;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Starts, reconfigures, pauses and stops one {@link SubscriptionRuntime} per subscription,
 * driven by config events (see {@code ConfigStateStore}). This is the modern replacement for
 * the legacy scheduler: nothing polls on a timer; the consumer streams continuously.
 *
 * <p>Each runtime gets:
 * <ul>
 *   <li>a consumer group {@code xdb-apply-<subscription>} subscribed to {@code cdc.<publication>.*}</li>
 *   <li>a Hikari pool to the subscription's target</li>
 *   <li>an error handler: exponential backoff, then a DLQ topic {@code xdb.dlq.<subscription>}</li>
 * </ul>
 */
@Component
public class SubscriptionRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionRuntimeManager.class);

    private final Map<String, SubscriptionRuntime> runtimes = new ConcurrentHashMap<>();

    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;
    private final CredentialProvider credentials;
    private final DebeziumEnvelopeAdapter adapter;
    private final ApplyMetrics metrics;
    private final ApplyProperties props;

    public SubscriptionRuntimeManager(ConsumerFactory<String, String> consumerFactory,
                                      KafkaTemplate<String, String> kafkaTemplate, KafkaAdmin kafkaAdmin,
                                      CredentialProvider credentials, DebeziumEnvelopeAdapter adapter,
                                      ApplyMetrics metrics, ApplyProperties props) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
        this.credentials = credentials;
        this.adapter = adapter;
        this.metrics = metrics;
        this.props = props;
    }

    /**
     * Converges the running state of one subscription to the desired state.
     * {@code desired == null} means the subscription or its publication no longer exists.
     */
    public synchronized void reconcile(String subscription, RuntimeConfig desired) {
        SubscriptionRuntime current = runtimes.get(subscription);
        if (desired == null) {
            if (current != null) {
                log.info("[{}] stopping: subscription or publication removed", subscription);
                runtimes.remove(subscription).stop();
            }
            return;
        }
        if (current != null && current.config().requiresRestart(desired)) {
            log.info("[{}] publication or target changed: restarting consumer and pool", subscription);
            runtimes.remove(subscription).stop();
            current = null;
        }
        if (current != null) {
            current.updateConfig(desired);
            return;
        }
        try {
            SubscriptionRuntime runtime = create(desired);
            runtimes.put(subscription, runtime);
            runtime.start();
            log.info("[{}] started: group={} topics={} target={}", subscription,
                    Topics.applyConsumerGroup(subscription), Topics.cdcTopicPattern(desired.publication().name()),
                    desired.subscription().target().jdbcUrl());
        } catch (RuntimeException e) {
            log.error("[{}] could not start: {}", subscription, e.getMessage(), e);
        }
    }

    public Collection<SubscriptionRuntime> runtimes() {
        return List.copyOf(runtimes.values());
    }

    @PreDestroy
    public synchronized void shutdown() {
        runtimes.values().forEach(SubscriptionRuntime::stop);
        runtimes.clear();
    }

    // ------------------------------------------------------------------ construction

    private SubscriptionRuntime create(RuntimeConfig cfg) {
        String name = cfg.name();
        DatabaseEndpoint target = cfg.subscription().target();

        HikariDataSource ds = dataSource(name, target);
        TargetWriter writer = new TargetWriter(DialectFactory.forType(target.dbType()));
        SubscriptionRuntime runtime = new SubscriptionRuntime(cfg, ds, writer, adapter, metrics);
        runtime.prepareTarget();

        kafkaAdmin.createOrModifyTopics(deadLetterTopic(name));

        ContainerProperties cp = new ContainerProperties(Topics.cdcTopicPattern(cfg.publication().name()));
        cp.setGroupId(Topics.applyConsumerGroup(name));
        cp.setClientId("xdb-apply-" + name);
        cp.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        cp.setMessageListener(runtime);
        cp.setConsumerRebalanceListener(runtime);
        cp.setObservationEnabled(true);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, cp);
        container.setConcurrency(props.concurrency());
        container.setBeanName("xdb-apply-" + name);
        container.setCommonErrorHandler(errorHandler(name));
        runtime.attach(container);
        return runtime;
    }

    private HikariDataSource dataSource(String subscription, DatabaseEndpoint target) {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("xdb-target-" + subscription);
        hc.setJdbcUrl(target.jdbcUrl());
        hc.setUsername(target.username());
        hc.setPassword(credentials.password(target));
        hc.setAutoCommit(false);
        hc.setMaximumPoolSize(props.targetPoolSize());
        hc.setMinimumIdle(1);
        hc.setConnectionTimeout(10_000);
        // Do not fail startup when the target is down; the consumer's retry loop handles it.
        hc.setInitializationFailTimeout(-1);
        return new HikariDataSource(hc);
    }

    private NewTopic deadLetterTopic(String subscription) {
        return TopicBuilder.name(Topics.deadLetterTopic(subscription))
                .partitions(1)
                .replicas(props.dlqReplicationFactor())
                .build();
    }

    /**
     * Retry policy by error class (see {@link ApplyExceptions}):
     * transient errors back off exponentially <b>forever</b> (capped interval), keeping the
     * partition in order and dead-lettering nothing during an outage; data errors back off
     * for a bounded time then go to the DLQ; malformed records go to the DLQ immediately.
     */
    private DefaultErrorHandler errorHandler(String subscription) {
        DeadLetterPublishingRecoverer dlq = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(Topics.deadLetterTopic(subscription), -1));

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            log.error("[{}] dead-lettering {}-{}@{}: {}", subscription, record.topic(), record.partition(),
                    record.offset(), ex.getMessage());
            metrics.deadLettered(subscription);
            dlq.accept(record, ex);
        }, dataErrorBackOff());

        handler.setBackOffFunction((record, ex) -> {
            if (ApplyExceptions.find(ex, MalformedEventException.class) != null) {
                return new FixedBackOff(0, 0);
            }
            if (ApplyExceptions.find(ex, TransientTargetException.class) != null) {
                return transientBackOff();
            }
            return dataErrorBackOff();
        });
        handler.addNotRetryableExceptions(MalformedEventException.class);
        handler.setCommitRecovered(true);
        return handler;
    }

    private BackOff transientBackOff() {
        ExponentialBackOff b = new ExponentialBackOff(props.retryInitialInterval().toMillis(), 2.0);
        b.setMaxInterval(props.retryMaxInterval().toMillis());
        b.setMaxElapsedTime(Long.MAX_VALUE);
        return b;
    }

    private BackOff dataErrorBackOff() {
        ExponentialBackOff b = new ExponentialBackOff(props.retryInitialInterval().toMillis(), 2.0);
        b.setMaxInterval(props.retryMaxInterval().toMillis());
        b.setMaxElapsedTime(props.dataErrorMaxElapsed().toMillis());
        return b;
    }
}
