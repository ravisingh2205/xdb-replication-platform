package com.xdb.replication.publication.api;

import com.xdb.replication.common.model.DbType;
import com.xdb.replication.common.model.RowFilter;
import com.xdb.replication.common.model.TopicRouting;
import com.xdb.replication.publication.domain.Publication;
import com.xdb.replication.publication.domain.SourceDatabase;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.time.Instant;
import java.util.List;

/** Request/response DTOs. Entities never leave the service boundary. */
public final class Dtos {

    private Dtos() {
    }

    static final String NAME_REGEX = "^[a-z][a-z0-9_]{2,40}$";
    static final String NAME_MESSAGE = "lowercase letters, digits, underscore; 3-41 chars; starts with a letter";

    public record SourceDatabaseRequest(
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

    public record SourceDatabaseResponse(Long id, String name, DbType dbType, String host, int port,
                                         String databaseName, String pdbName, String username, String urlOptions,
                                         Instant createdAt) {
        public static SourceDatabaseResponse from(SourceDatabase s) {
            return new SourceDatabaseResponse(s.getId(), s.getName(), s.getDbType(), s.getHost(), s.getPort(),
                    s.getDatabaseName(), s.getPdbName(), s.getUsername(), s.getUrlOptions(), s.getCreatedAt());
        }
    }

    public record PublicationRequest(
            @NotBlank @Pattern(regexp = NAME_REGEX, message = NAME_MESSAGE) String name,
            @NotBlank String sourceDatabase,
            @NotEmpty List<@Pattern(regexp = "^[A-Za-z0-9_$#]+\\.[A-Za-z0-9_$#]+$",
                    message = "tables must be schema.table") String> tables,
            List<RowFilter> rowFilters,
            @Min(1) @Max(64) Integer partitions,
            @Pattern(regexp = "initial|no_data|when_needed|never|always") String snapshotMode,
            TopicRouting topicRouting) {
    }

    public record PublicationUpdateRequest(
            @NotEmpty List<String> tables,
            List<RowFilter> rowFilters) {
    }

    public record PublicationResponse(Long id, String name, String sourceDatabase, DbType sourceDbType,
                                      List<String> tables, List<RowFilter> rowFilters, int partitions,
                                      String snapshotMode, TopicRouting topicRouting, String topicPrefix,
                                      String connectorName,
                                      String status, String statusMessage, Instant createdAt, Instant updatedAt) {
        public static PublicationResponse from(Publication p) {
            return new PublicationResponse(p.getId(), p.getName(), p.getSourceDatabase().getName(),
                    p.getSourceDatabase().getDbType(), p.getTables(), p.getRowFilters(), p.getPartitions(),
                    p.getSnapshotMode(), p.getTopicRouting(), p.topicPrefix(), p.connectorName(), p.getStatus().name(),
                    p.getStatusMessage(), p.getCreatedAt(), p.getUpdatedAt());
        }
    }
}
