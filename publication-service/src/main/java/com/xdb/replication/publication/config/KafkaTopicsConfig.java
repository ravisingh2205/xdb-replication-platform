package com.xdb.replication.publication.config;

import com.xdb.replication.common.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    /**
     * Compacted: Kafka keeps the latest event per publication name, so any consumer
     * reading from the beginning rebuilds the full current configuration.
     */
    @Bean
    NewTopic publicationConfigTopic(ConnectProperties props) {
        return TopicBuilder.name(Topics.CONFIG_PUBLICATIONS)
                .partitions(1)
                .replicas(props.replicationFactor())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
    }
}
