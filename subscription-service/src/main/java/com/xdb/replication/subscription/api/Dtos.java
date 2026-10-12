package com.xdb.replication.subscription.api;

import com.xdb.replication.common.model.DbType;
import com.xdb.replication.common.model.TableMapping;
import com.xdb.replication.subscription.domain.Subscription;
import com.xdb.replication.subscription.domain.TargetDatabase;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.time.Instant;
import java.util.List;

public final class Dtos {

    private Dtos() {
    }

    static final String NAME_REGEX = "^[a-z][a-z0-9_]{2,40}$";
    static final String NAME_MESSAGE = "lowercase letters, digits, underscore; 3-41 chars; starts with a letter";

    public record TargetDatabaseRequest(
            @NotBlank @Pattern(regexp = NAME_REGEX, message = NAME_MESSAGE) String name,
            @NotNull DbType dbType,
            @NotBlank String host,
            Integer port,
            @NotBlank String databaseName,
            String pdbName,
            @NotBlank String username,
            @NotBlank String password,
            String urlOptions) {
    }

    /** Body of the console's "Test" button: same fields as a registration, no name required. */
    public record ConnectionTestRequest(
            @NotNull DbType dbType,
            @NotBlank String host,
            Integer port,
            @NotBlank String databaseName,
            String pdbName,
            @NotBlank String username,
            @NotBlank String password,
            String urlOptions) {

        public com.xdb.replication.common.model.DatabaseEndpoint toEndpoint() {
            return new com.xdb.replication.common.model.DatabaseEndpoint("test", dbType, host,
                    port != null ? port : dbType.defaultPort(), databaseName, username, null, pdbName, urlOptions);
        }
    }

    public record TargetDatabaseResponse(Long id, String name, DbType dbType, String host, int port,
                                         String databaseName, String pdbName, String username, String urlOptions,
                                         Instant createdAt) {
        public static TargetDatabaseResponse from(TargetDatabase t) {
            return new TargetDatabaseResponse(t.getId(), t.getName(), t.getDbType(), t.getHost(), t.getPort(),
                    t.getDatabaseName(), t.getPdbName(), t.getUsername(), t.getUrlOptions(), t.getCreatedAt());
        }
    }

    /** Omitting {@code tableMappings} maps every published table to the same schema.table on the target. */
    public record SubscriptionRequest(
            @NotBlank @Pattern(regexp = NAME_REGEX, message = NAME_MESSAGE) String name,
            @NotBlank String publication,
            @NotBlank String targetDatabase,
            List<TableMapping> tableMappings) {
    }

    public record SubscriptionUpdateRequest(@NotEmpty List<TableMapping> tableMappings) {
    }

    public record SubscriptionResponse(Long id, String name, String publication, String targetDatabase,
                                       DbType targetDbType, List<TableMapping> tableMappings, boolean active,
                                       String consumerGroup, Instant createdAt, Instant updatedAt) {
        public static SubscriptionResponse from(Subscription s) {
            return new SubscriptionResponse(s.getId(), s.getName(), s.getPublicationName(),
                    s.getTargetDatabase().getName(), s.getTargetDatabase().getDbType(), s.getTableMappings(),
                    s.isActive(), com.xdb.replication.common.events.Topics.applyConsumerGroup(s.getName()),
                    s.getCreatedAt(), s.getUpdatedAt());
        }
    }

    public record Credentials(String username, String password) {
    }
}
