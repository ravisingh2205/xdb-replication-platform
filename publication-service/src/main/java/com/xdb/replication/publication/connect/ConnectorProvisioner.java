package com.xdb.replication.publication.connect;

import com.xdb.replication.common.secrets.AwsSecretsManagerSecretStore;
import com.xdb.replication.common.secrets.SecretStore;
import com.xdb.replication.publication.config.ConnectProperties;
import com.xdb.replication.publication.connect.strategy.ConnectorConfigFactory;
import com.xdb.replication.publication.connect.strategy.ConnectorContext;
import com.xdb.replication.publication.domain.Publication;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Facade over "turn a publication into a running Debezium connector":
 * resolve the password, pick the strategy, register through Kafka Connect.
 */
@Component
public class ConnectorProvisioner {

    private final ConnectorConfigFactory factory = ConnectorConfigFactory.defaults();
    private final KafkaConnectClient connect;
    private final SecretStore secrets;
    private final ConnectProperties props;

    public ConnectorProvisioner(KafkaConnectClient connect, SecretStore secrets, ConnectProperties props) {
        this.connect = connect;
        this.secrets = secrets;
        this.props = props;
    }

    public Map<String, String> buildConfig(Publication p) {
        var source = p.getSourceDatabase().toEndpoint();
        ConnectorContext ctx = new ConnectorContext(
                p.getName(),
                source,
                passwordValue(source.secretRef()),
                p.getTables(),
                p.topicPrefix(),
                p.getPartitions(),
                props.replicationFactor(),
                p.getSnapshotMode(),
                props.kafkaBootstrapServers(),
                p.getTopicRouting(),
                props.snapshotThreads());
        return factory.build(ctx);
    }

    public void provision(Publication p) {
        connect.upsertConnector(p.connectorName(), buildConfig(p));
    }

    /**
     * Inline mode puts the decrypted password in the config (dev only). Config-provider mode
     * puts a placeholder that the Connect worker resolves from AWS Secrets Manager at task
     * start, so the secret never lands in Connect's internal config topic.
     */
    private String passwordValue(String secretRef) {
        if (props.inlineSecrets()) {
            return secrets.get(secretRef);
        }
        String secretName = secrets instanceof AwsSecretsManagerSecretStore aws ? aws.secretName(secretRef) : secretRef;
        return props.passwordPlaceholder(secretName);
    }

    /** Same as {@link #buildConfig} but with the password masked, safe to return from the API. */
    public Map<String, String> previewConfig(Publication p) {
        Map<String, String> config = new java.util.LinkedHashMap<>(buildConfig(p));
        config.computeIfPresent("database.password", (k, v) -> props.inlineSecrets() ? "********" : v);
        return config;
    }
}
