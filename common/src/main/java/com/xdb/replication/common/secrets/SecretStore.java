package com.xdb.replication.common.secrets;

/**
 * Stores database passwords outside the metadata tables (Strategy pattern).
 * <ul>
 *   <li>{@code xdb.secrets.provider=aws}: {@link AwsSecretsManagerSecretStore}, used on EKS via IRSA.</li>
 *   <li>{@code xdb.secrets.provider=local} (default): {@link LocalEncryptedSecretStore}, AES-GCM
 *       encrypted rows in the service's own database, for Docker Compose and laptops.</li>
 * </ul>
 */
public interface SecretStore {

    /** Creates or replaces the secret and returns the reference to persist in metadata. */
    String put(String ref, String secret);

    String get(String ref);

    void delete(String ref);
}
