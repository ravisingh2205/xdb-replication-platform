package com.xdb.replication.apply.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Apply-side tuning.
 *
 * @param concurrency            consumer threads per subscription per pod (at most the topic partition count is useful)
 * @param maxPollRecords         records per poll, i.e. the maximum size of one target transaction
 * @param credentials            {@code remote}: ask subscription-service; {@code aws}: read AWS Secrets Manager directly
 * @param subscriptionServiceUrl in-cluster URL of subscription-service (remote credentials mode)
 * @param internalToken          token for subscription-service's {@code /internal} endpoint
 * @param retryInitialInterval   first backoff after a failed record
 * @param retryMaxInterval       cap for one backoff sleep (must stay below max.poll.interval.ms)
 * @param dataErrorMaxElapsed    give up on a <i>data</i> error after this long and send the record to the DLQ;
 *                               transient errors (target down) retry forever so nothing is dead-lettered by an outage
 * @param targetPoolSize         Hikari pool size per subscription target
 */
@ConfigurationProperties(prefix = "xdb.apply")
public record ApplyProperties(
        int concurrency,
        int maxPollRecords,
        String credentials,
        String subscriptionServiceUrl,
        String internalToken,
        Duration retryInitialInterval,
        Duration retryMaxInterval,
        Duration dataErrorMaxElapsed,
        int targetPoolSize,
        short dlqReplicationFactor) {

    public ApplyProperties {
        if (concurrency <= 0) concurrency = 3;
        if (maxPollRecords <= 0) maxPollRecords = 500;
        if (credentials == null) credentials = "remote";
        if (retryInitialInterval == null) retryInitialInterval = Duration.ofSeconds(1);
        if (retryMaxInterval == null) retryMaxInterval = Duration.ofSeconds(30);
        if (dataErrorMaxElapsed == null) dataErrorMaxElapsed = Duration.ofMinutes(2);
        if (targetPoolSize <= 0) targetPoolSize = concurrency + 1;
        if (dlqReplicationFactor <= 0) dlqReplicationFactor = 1;
    }
}
