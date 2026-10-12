package com.xdb.replication.apply.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.apply.adapter.DebeziumEnvelopeAdapter;
import com.xdb.replication.apply.credentials.CredentialProvider;
import com.xdb.replication.apply.credentials.SecretStoreCredentialProvider;
import com.xdb.replication.apply.credentials.SubscriptionServiceCredentialProvider;
import com.xdb.replication.common.secrets.SecretStore;
import io.micrometer.core.instrument.MeterRegistry;
import com.xdb.replication.common.events.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.web.client.RestClient;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ApplyKafkaConfig {

    /**
     * Consumer settings for CDC consumption:
     * <ul>
     *   <li>auto-commit off: offsets are committed only after the target transaction commits</li>
     *   <li>{@code earliest}: a new subscription starts with the snapshot records</li>
     *   <li>{@code CooperativeStickyAssignor}: incremental rebalances, so scaling a pod moves only
     *       some partitions instead of stopping the whole group (matters with hundreds of partitions)</li>
     *   <li>short metadata max age: topics for newly published tables are picked up by the regex quickly</li>
     * </ul>
     */
    @Bean
    ConsumerFactory<String, String> consumerFactory(KafkaProperties kafka, ObjectProvider<SslBundles> ssl,
                                                    ApplyProperties props, MeterRegistry registry) {
        Map<String, Object> cfg = new HashMap<>(kafka.buildConsumerProperties(ssl.getIfAvailable()));
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.maxPollRecords());
        cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        cfg.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 30_000);
        cfg.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(cfg);
        factory.addListener(new MicrometerConsumerListener<>(registry));
        return factory;
    }

    /** Declared here too (idempotent) so the apply-service can start before the other services. */
    @Bean
    KafkaAdmin.NewTopics configTopics(ApplyProperties props) {
        return new KafkaAdmin.NewTopics(
                compacted(Topics.CONFIG_PUBLICATIONS, props.dlqReplicationFactor()),
                compacted(Topics.CONFIG_SUBSCRIPTIONS, props.dlqReplicationFactor()));
    }

    private static NewTopic compacted(String name, short replicas) {
        return TopicBuilder.name(name).partitions(1).replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT).build();
    }

    @Bean(destroyMethod = "close")
    AdminClient adminClient(KafkaAdmin admin) {
        return AdminClient.create(admin.getConfigurationProperties());
    }

    @Bean
    DebeziumEnvelopeAdapter debeziumEnvelopeAdapter(ObjectMapper mapper) {
        return new DebeziumEnvelopeAdapter(mapper);
    }

    /** AWS: read Secrets Manager directly. Local: ask subscription-service. */
    @Bean
    CredentialProvider credentialProvider(ApplyProperties props, ObjectProvider<SecretStore> secretStore,
                                          RestClient.Builder rest) {
        if ("aws".equalsIgnoreCase(props.credentials())) {
            SecretStore store = secretStore.getIfAvailable();
            if (store == null) {
                throw new IllegalStateException("xdb.apply.credentials=aws requires xdb.secrets.provider=aws");
            }
            return new SecretStoreCredentialProvider(store);
        }
        return new SubscriptionServiceCredentialProvider(rest, props);
    }
}
