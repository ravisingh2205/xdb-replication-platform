package com.xdb.replication.subscription.domain;

import com.xdb.replication.common.model.DatabaseEndpoint;
import com.xdb.replication.common.model.DbType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/** A registered target ("subscription") database: legacy step 3, "Adding Subscription DB". */
@Entity
@Table(name = "target_database")
public class TargetDatabase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "db_type", nullable = false)
    private DbType dbType;

    @Column(nullable = false)
    private String host;

    private int port;

    @Column(name = "database_name", nullable = false)
    private String databaseName;

    @Column(name = "pdb_name")
    private String pdbName;

    @Column(nullable = false)
    private String username;

    @Column(name = "url_options")
    private String urlOptions;

    @Column(name = "secret_ref", nullable = false)
    private String secretRef;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    protected TargetDatabase() {
    }

    public TargetDatabase(String name, DbType dbType, String host, int port, String databaseName,
                          String pdbName, String username, String secretRef, String urlOptions) {
        this.name = name;
        this.dbType = dbType;
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.pdbName = pdbName;
        this.username = username;
        this.secretRef = secretRef;
        this.urlOptions = urlOptions;
    }

    public DatabaseEndpoint toEndpoint() {
        return new DatabaseEndpoint(name, dbType, host, port, databaseName, username, secretRef, pdbName, urlOptions);
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public DbType getDbType() { return dbType; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getDatabaseName() { return databaseName; }
    public String getPdbName() { return pdbName; }
    public String getUsername() { return username; }
    public String getSecretRef() { return secretRef; }
    public String getUrlOptions() { return urlOptions; }
    public Instant getCreatedAt() { return createdAt; }
}
