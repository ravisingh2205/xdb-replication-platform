package com.xdb.replication.subscription.api;

import com.xdb.replication.subscription.api.Dtos.Credentials;
import com.xdb.replication.subscription.config.SubscriptionProperties;
import com.xdb.replication.subscription.service.TargetDatabaseService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

/**
 * Service-to-service endpoint used by the apply-service in local mode to fetch target
 * credentials. Defence in depth: the gateway never routes {@code /internal/**}, a Kubernetes
 * NetworkPolicy only lets apply-service pods reach it, and a shared token is required.
 * On AWS the apply-service reads AWS Secrets Manager directly and this endpoint is unused.
 */
@RestController
@RequestMapping("/internal")
public class InternalController {

    private final TargetDatabaseService targets;
    private final byte[] token;

    public InternalController(TargetDatabaseService targets, SubscriptionProperties props) {
        this.targets = targets;
        this.token = props.internalToken() == null ? new byte[0] : props.internalToken().getBytes(StandardCharsets.UTF_8);
    }

    @GetMapping("/target-databases/{name}/credentials")
    public Credentials credentials(@PathVariable String name,
                                   @RequestHeader(value = "X-Internal-Token", required = false) String presented) {
        if (token.length == 0 || presented == null
                || !MessageDigest.isEqual(token, presented.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN);
        }
        return targets.credentials(name);
    }
}
