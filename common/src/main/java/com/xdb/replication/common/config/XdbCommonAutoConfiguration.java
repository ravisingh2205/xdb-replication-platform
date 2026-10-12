package com.xdb.replication.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.common.outbox.OutboxRelay;
import com.xdb.replication.common.outbox.OutboxWriter;
import com.xdb.replication.common.secrets.AwsSecretsManagerSecretStore;
import com.xdb.replication.common.secrets.LocalEncryptedSecretStore;
import com.xdb.replication.common.secrets.SecretStore;
import com.xdb.replication.common.web.GlobalExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Auto-configuration shared by the services. Every bean is conditional, so a service
 * only gets what its classpath and properties ask for.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
public class XdbCommonAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestControllerAdvice")
    static class WebConfig {
        @Bean
        @ConditionalOnMissingBean
        GlobalExceptionHandler xdbGlobalExceptionHandler() {
            return new GlobalExceptionHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.jdbc.core.JdbcTemplate")
    @ConditionalOnProperty(name = "xdb.secrets.provider", havingValue = "local", matchIfMissing = true)
    static class LocalSecretsConfig {
        @Bean
        @ConditionalOnBean(JdbcTemplate.class)
        @ConditionalOnMissingBean(SecretStore.class)
        SecretStore localEncryptedSecretStore(JdbcTemplate jdbc, @Value("${xdb.secrets.local-key}") String key) {
            return new LocalEncryptedSecretStore(jdbc, key);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "software.amazon.awssdk.services.secretsmanager.SecretsManagerClient")
    @ConditionalOnProperty(name = "xdb.secrets.provider", havingValue = "aws")
    static class AwsSecretsConfig {
        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        SecretsManagerClient secretsManagerClient() {
            // Region and credentials come from the default provider chain (IRSA on EKS).
            return SecretsManagerClient.create();
        }

        @Bean
        @ConditionalOnMissingBean(SecretStore.class)
        SecretStore awsSecretsManagerSecretStore(SecretsManagerClient client,
                                                 @Value("${xdb.secrets.aws-prefix:xdb/}") String prefix) {
            return new AwsSecretsManagerSecretStore(client, prefix);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @ConditionalOnClass(name = {"org.springframework.jdbc.core.JdbcTemplate",
            "org.springframework.kafka.core.KafkaTemplate"})
    @ConditionalOnProperty(name = "xdb.outbox.enabled", havingValue = "true")
    static class OutboxConfig {
        @Bean
        @ConditionalOnBean(JdbcTemplate.class)
        OutboxWriter outboxWriter(JdbcTemplate jdbc, ObjectMapper mapper) {
            return new OutboxWriter(jdbc, mapper);
        }

        @Bean
        @ConditionalOnBean({JdbcTemplate.class, KafkaTemplate.class})
        OutboxRelay outboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, PlatformTransactionManager txm) {
            return new OutboxRelay(jdbc, kafka, new TransactionTemplate(txm));
        }
    }
}
