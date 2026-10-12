package com.xdb.replication.apply.credentials;

import com.xdb.replication.apply.config.ApplyProperties;
import com.xdb.replication.common.model.DatabaseEndpoint;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * Local/dev mode: asks subscription-service for the target password over its
 * internal endpoint. Called once per (re)start of a subscription runtime.
 */
public class SubscriptionServiceCredentialProvider implements CredentialProvider {

    private final RestClient http;
    private final String token;

    public SubscriptionServiceCredentialProvider(RestClient.Builder builder, ApplyProperties props) {
        this.http = builder.baseUrl(props.subscriptionServiceUrl()).build();
        this.token = props.internalToken();
    }

    @Override
    @SuppressWarnings("unchecked")
    public String password(DatabaseEndpoint target) {
        Map<String, Object> creds = http.get()
                .uri("/internal/target-databases/{name}/credentials", target.name())
                .header("X-Internal-Token", token)
                .retrieve()
                .body(Map.class);
        if (creds == null || creds.get("password") == null) {
            throw new IllegalStateException("No credentials returned for target " + target.name());
        }
        return creds.get("password").toString();
    }
}
