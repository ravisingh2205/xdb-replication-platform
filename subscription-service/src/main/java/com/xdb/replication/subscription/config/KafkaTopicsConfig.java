package com.xdb.replication.subscription.config;

import com.xdb.replication.common.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    /** Compacted: the latest state of every subscription, keyed by name. */
    @Bean
    NewTopic subscriptionConfigTopic(SubscriptionProperties props) {
        return TopicBuilder.name(Topics.CONFIG_SUBSCRIPTIONS)
                .partitions(1)
                .replicas(props.replicationFactor())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
    }
}
