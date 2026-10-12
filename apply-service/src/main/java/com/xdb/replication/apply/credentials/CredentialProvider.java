package com.xdb.replication.apply.credentials;

import com.xdb.replication.common.model.DatabaseEndpoint;

/** Resolves the password for a target database (Strategy pattern). */
public interface CredentialProvider {

    String password(DatabaseEndpoint target);
}
