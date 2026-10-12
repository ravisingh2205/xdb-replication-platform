package com.xdb.replication.common.secrets;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DeleteSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

/**
 * Production secret store backed by AWS Secrets Manager. Pods get credentials through
 * IRSA (IAM Roles for Service Accounts); no static AWS keys live in the cluster.
 * Secret names are prefixed (default {@code xdb/}) so the IAM policy can be scoped to them.
 */
public class AwsSecretsManagerSecretStore implements SecretStore {

    private final SecretsManagerClient client;
    private final String prefix;

    public AwsSecretsManagerSecretStore(SecretsManagerClient client, String prefix) {
        this.client = client;
        this.prefix = prefix;
    }

    @Override
    public String put(String ref, String secret) {
        String name = prefix + ref;
        try {
            client.putSecretValue(PutSecretValueRequest.builder().secretId(name).secretString(secret).build());
        } catch (ResourceNotFoundException e) {
            client.createSecret(CreateSecretRequest.builder().name(name).secretString(secret).build());
        }
        return ref;
    }

    @Override
    public String get(String ref) {
        return client.getSecretValue(GetSecretValueRequest.builder().secretId(prefix + ref).build()).secretString();
    }

    @Override
    public void delete(String ref) {
        client.deleteSecret(DeleteSecretRequest.builder().secretId(prefix + ref).recoveryWindowInDays(7L).build());
    }

    /** Full Secrets Manager name, used to build Kafka Connect config-provider placeholders. */
    public String secretName(String ref) {
        return prefix + ref;
    }
}
