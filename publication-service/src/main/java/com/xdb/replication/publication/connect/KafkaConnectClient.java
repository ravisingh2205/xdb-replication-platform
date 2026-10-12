package com.xdb.replication.publication.connect;

import com.xdb.replication.common.web.ApiExceptions;
import com.xdb.replication.publication.config.ConnectProperties;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.Map;
import java.util.Optional;

/**
 * Thin client for the Kafka Connect REST API (a Gateway to an external system).
 *
 * <p>Calls are wrapped in Resilience4j <b>Retry</b> (exponential backoff for transient errors
 * such as a Connect rebalance returning 409) and a <b>Circuit Breaker</b> (stop hammering
 * Connect when it is down; fail fast so API calls return 502 quickly). Settings live in
 * {@code application.yml} under {@code resilience4j.*.instances.kafkaConnect}.
 */
@Component
public class KafkaConnectClient {

    private static final String CB = "kafkaConnect";

    private final RestClient http;

    public KafkaConnectClient(RestClient.Builder builder, ConnectProperties props) {
        this.http = builder.baseUrl(props.url()).build();
    }

    /** Create-or-update. {@code PUT /connectors/{name}/config} is idempotent, so retries are safe. */
    @Retry(name = CB)
    @CircuitBreaker(name = CB)
    public void upsertConnector(String name, Map<String, String> config) {
        call(() -> http.put().uri("/connectors/{name}/config", name)
                .contentType(MediaType.APPLICATION_JSON)
                .body(config)
                .retrieve()
                .toBodilessEntity(), "upsert connector " + name);
    }

    @Retry(name = CB)
    @CircuitBreaker(name = CB)
    public Optional<ConnectorStatus> status(String name) {
        try {
            return Optional.ofNullable(http.get().uri("/connectors/{name}/status", name)
                    .retrieve().body(ConnectorStatus.class));
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        } catch (RestClientException e) {
            throw new ApiExceptions.UpstreamException("Kafka Connect status failed for " + name, e);
        }
    }

    @Retry(name = CB)
    @CircuitBreaker(name = CB)
    public void pause(String name) {
        call(() -> http.put().uri("/connectors/{name}/pause", name).retrieve().toBodilessEntity(), "pause " + name);
    }

    @Retry(name = CB)
    @CircuitBreaker(name = CB)
    public void resume(String name) {
        call(() -> http.put().uri("/connectors/{name}/resume", name).retrieve().toBodilessEntity(), "resume " + name);
    }

    /** Restarts the connector and only its failed tasks. */
    @Retry(name = CB)
    @CircuitBreaker(name = CB)
    public void restart(String name) {
        call(() -> http.post().uri("/connectors/{name}/restart?includeTasks=true&onlyFailed=true", name)
                .retrieve().toBodilessEntity(), "restart " + name);
    }

    @Retry(name = CB)
    @CircuitBreaker(name = CB)
    public void delete(String name) {
        try {
            http.delete().uri("/connectors/{name}", name).retrieve().toBodilessEntity();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
                throw new ApiExceptions.UpstreamException("Kafka Connect delete failed for " + name, e);
            }
        } catch (RestClientException e) {
            throw new ApiExceptions.UpstreamException("Kafka Connect delete failed for " + name, e);
        }
    }

    private void call(Runnable r, String what) {
        try {
            r.run();
        } catch (HttpClientErrorException.BadRequest e) {
            // Invalid connector config: retrying will not help, surface the validation message.
            throw new IllegalArgumentException("Kafka Connect rejected " + what + ": " + e.getResponseBodyAsString());
        } catch (RestClientException e) {
            throw new ApiExceptions.UpstreamException("Kafka Connect call failed: " + what, e);
        }
    }
}
