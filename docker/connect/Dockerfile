# Kafka Connect worker with Debezium source connectors (Postgres/EPAS, Oracle, SQL Server).
FROM quay.io/debezium/connect:2.7.3.Final

# Oracle JDBC driver: Debezium does not ship it for licensing reasons.
ARG OJDBC_VERSION=23.5.0.24.07
RUN curl -fsSL -o /kafka/connect/debezium-connector-oracle/ojdbc11.jar \
      https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar

# AWS Secrets Manager ConfigProvider (Lenses secret-provider). Lets connector configs carry
# ${aws:xdb/source/<name>} placeholders instead of passwords. Only needed for the AWS deployment.
ARG SECRET_PROVIDER_VERSION=2.3.0
RUN mkdir -p /kafka/connect/secret-provider && curl -fsSL -o /kafka/connect/secret-provider/secret-provider.jar \
      https://github.com/lensesio/secret-provider/releases/download/${SECRET_PROVIDER_VERSION}/secret-provider-${SECRET_PROVIDER_VERSION}-all.jar || \
    echo "secret-provider download skipped; use inline secret mode locally"

# Amazon MSK IAM authentication for the Connect workers and the Debezium producers.
ARG MSK_IAM_VERSION=2.2.0
RUN curl -fsSL -o /kafka/libs/aws-msk-iam-auth.jar \
      https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_VERSION}/aws-msk-iam-auth-${MSK_IAM_VERSION}-all.jar || \
    echo "aws-msk-iam-auth download skipped; only needed on MSK"
