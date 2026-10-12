package com.xdb.replication.apply.credentials;

import com.xdb.replication.common.model.DatabaseEndpoint;
import com.xdb.replication.common.secrets.SecretStore;

/** AWS mode: reads the target password straight from AWS Secrets Manager (via IRSA). */
public class SecretStoreCredentialProvider implements CredentialProvider {

    private final SecretStore secrets;

    public SecretStoreCredentialProvider(SecretStore secrets) {
        this.secrets = secrets;
    }

    @Override
    public String password(DatabaseEndpoint target) {
        return secrets.get(target.secretRef());
    }
}
