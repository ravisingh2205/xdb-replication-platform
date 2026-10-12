package com.xdb.replication.subscription.service;

import com.xdb.replication.common.secrets.SecretStore;
import com.xdb.replication.common.web.ApiExceptions;
import com.xdb.replication.subscription.api.Dtos.Credentials;
import com.xdb.replication.subscription.api.Dtos.TargetDatabaseRequest;
import com.xdb.replication.subscription.domain.TargetDatabase;
import com.xdb.replication.subscription.repository.SubscriptionRepository;
import com.xdb.replication.subscription.repository.TargetDatabaseRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.xdb.replication.common.model.JdbcProbe;
import com.xdb.replication.subscription.api.Dtos.ConnectionTestRequest;

import java.sql.SQLException;
import java.util.List;

@Service
public class TargetDatabaseService {

    private final TargetDatabaseRepository repo;
    private final SubscriptionRepository subscriptions;
    private final SecretStore secrets;

    public TargetDatabaseService(TargetDatabaseRepository repo, SubscriptionRepository subscriptions, SecretStore secrets) {
        this.repo = repo;
        this.subscriptions = subscriptions;
        this.secrets = secrets;
    }

    @Transactional
    public TargetDatabase create(TargetDatabaseRequest req) {
        if (repo.existsByName(req.name())) {
            throw new ApiExceptions.ConflictException("Target database '" + req.name() + "' already exists");
        }
        String secretRef = secrets.put("target/" + req.name(), req.password());
        int port = req.port() != null ? req.port() : req.dbType().defaultPort();
        return repo.save(new TargetDatabase(req.name(), req.dbType(), req.host(), port,
                req.databaseName(), req.pdbName(), req.username(), secretRef, req.urlOptions()));
    }

    @Transactional(readOnly = true)
    public List<TargetDatabase> list() {
        return repo.findAll();
    }

    @Transactional(readOnly = true)
    public TargetDatabase get(String name) {
        return repo.findByName(name)
                .orElseThrow(() -> new ApiExceptions.NotFoundException("Target database '" + name + "' not found"));
    }

    /** For the apply-service only (internal endpoint). */
    @Transactional(readOnly = true)
    public Credentials credentials(String name) {
        TargetDatabase t = get(name);
        return new Credentials(t.getUsername(), secrets.get(t.getSecretRef()));
    }

    public JdbcProbe.Result test(ConnectionTestRequest req) {
        return JdbcProbe.test(req.toEndpoint(), req.password());
    }

    @Transactional(readOnly = true)
    public List<String> tables(String name) {
        TargetDatabase db = get(name);
        try {
            return JdbcProbe.listTables(db.toEndpoint(), secrets.get(db.getSecretRef()));
        } catch (SQLException e) {
            throw new ApiExceptions.UpstreamException("Cannot read tables from " + name + ": " + e.getMessage(), e);
        }
    }

    @Transactional
    public void delete(String name) {
        TargetDatabase t = get(name);
        if (subscriptions.existsByTargetDatabaseId(t.getId())) {
            throw new ApiExceptions.ConflictException("Target database '" + name + "' still has subscriptions");
        }
        repo.delete(t);
        secrets.delete(t.getSecretRef());
    }
}
