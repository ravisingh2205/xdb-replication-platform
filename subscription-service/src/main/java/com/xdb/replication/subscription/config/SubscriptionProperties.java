package com.xdb.replication.subscription.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param publicationServiceUrl in-cluster URL of publication-service (service-to-service, not via the gateway)
 * @param internalToken         shared token required on {@code /internal/**}; that path is never routed by the gateway
 * @param replicationFactor     replication factor for the subscription config topic
 */
@ConfigurationProperties(prefix = "xdb.subscription")
public record SubscriptionProperties(String publicationServiceUrl, String internalToken, short replicationFactor) {

    public SubscriptionProperties {
        if (replicationFactor <= 0) replicationFactor = 1;
    }
}
