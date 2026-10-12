package com.xdb.replication.subscription.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.xdb.replication.common.web.ApiExceptions;
import com.xdb.replication.subscription.config.SubscriptionProperties;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.Optional;

/**
 * Synchronous call to publication-service, used only at configuration time to validate
 * a subscription. The replication data path never depends on this call: it runs on Kafka.
 */
@Component
public class PublicationClient {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PublicationView(String name, String sourceDatabase, String sourceDbType,
                                  List<String> tables, String topicPrefix, String status) {
    }

    private final RestClient http;

    public PublicationClient(RestClient.Builder builder, SubscriptionProperties props) {
        this.http = builder.baseUrl(props.publicationServiceUrl()).build();
    }

    @Retry(name = "publicationService")
    @CircuitBreaker(name = "publicationService")
    public Optional<PublicationView> find(String name) {
        try {
            return Optional.ofNullable(http.get().uri("/api/publications/{name}", name)
                    .retrieve().body(PublicationView.class));
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        } catch (RestClientException e) {
            throw new ApiExceptions.UpstreamException("publication-service unavailable", e);
        }
    }
}
