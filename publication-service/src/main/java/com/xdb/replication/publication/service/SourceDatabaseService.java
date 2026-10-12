package com.xdb.replication.publication.service;

import com.xdb.replication.common.secrets.SecretStore;
import com.xdb.replication.common.web.ApiExceptions;
import com.xdb.replication.publication.api.Dtos.SourceDatabaseRequest;
import com.xdb.replication.publication.domain.SourceDatabase;
import com.xdb.replication.publication.repository.PublicationRepository;
import com.xdb.replication.publication.repository.SourceDatabaseRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.xdb.replication.common.model.JdbcProbe;
import com.xdb.replication.publication.api.Dtos.ConnectionTestRequest;

import java.sql.SQLException;
import java.util.List;

@Service
public class SourceDatabaseService {

    private final SourceDatabaseRepository repo;
    private final PublicationRepository publications;
    private final SecretStore secrets;

    public SourceDatabaseService(SourceDatabaseRepository repo, PublicationRepository publications, SecretStore secrets) {
        this.repo = repo;
        this.publications = publications;
        this.secrets = secrets;
    }

    @Transactional
    public SourceDatabase create(SourceDatabaseRequest req) {
        if (repo.existsByName(req.name())) {
            throw new ApiExceptions.ConflictException("Source database '" + req.name() + "' already exists");
        }
        // The password goes to the SecretStore; only the reference is stored in metadata.
        String secretRef = secrets.put("source/" + req.name(), req.password());
        int port = req.port() != null ? req.port() : req.dbType().defaultPort();
        return repo.save(new SourceDatabase(req.name(), req.dbType(), req.host(), port,
                req.databaseName(), req.pdbName(), req.username(), secretRef, req.urlOptions()));
    }

    @Transactional(readOnly = true)
    public List<SourceDatabase> list() {
        return repo.findAll();
    }

    @Transactional(readOnly = true)
    public SourceDatabase get(String name) {
        return repo.findByName(name)
                .orElseThrow(() -> new ApiExceptions.NotFoundException("Source database '" + name + "' not found"));
    }

    public JdbcProbe.Result test(ConnectionTestRequest req) {
        return JdbcProbe.test(req.toEndpoint(), req.password());
    }

    /** Live table list from the source, for the "Create Publication" table picker. */
    @Transactional(readOnly = true)
    public List<String> tables(String name) {
        SourceDatabase db = get(name);
        try {
            return JdbcProbe.listTables(db.toEndpoint(), secrets.get(db.getSecretRef()));
        } catch (SQLException e) {
            throw new ApiExceptions.UpstreamException("Cannot read tables from " + name + ": " + e.getMessage(), e);
        }
    }

    @Transactional
    public void delete(String name) {
        SourceDatabase db = get(name);
        if (publications.existsBySourceDatabaseId(db.getId())) {
            throw new ApiExceptions.ConflictException("Source database '" + name + "' still has publications");
        }
        repo.delete(db);
        secrets.delete(db.getSecretRef());
    }
}
