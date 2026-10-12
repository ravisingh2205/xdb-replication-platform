# Platform secrets in AWS Secrets Manager (synced into the cluster by External Secrets).
# Source/target DB passwords are created at runtime by the services under xdb/source/* and xdb/target/*.
resource "aws_kms_key" "secrets" {
  description         = "${local.name} Secrets Manager"
  enable_key_rotation = true
}

resource "aws_secretsmanager_secret" "metadata_db" {
  name       = "${var.name}/platform/metadata-db"
  kms_key_id = aws_kms_key.secrets.arn
}

resource "aws_secretsmanager_secret_version" "metadata_db" {
  secret_id = aws_secretsmanager_secret.metadata_db.id
  secret_string = jsonencode({
    username = aws_db_instance.metadata.username
    password = random_password.metadata_db.result
    host     = aws_db_instance.metadata.address
  })
}

resource "random_password" "internal_token" {
  length  = 48
  special = false
}

resource "aws_secretsmanager_secret" "internal_token" {
  name       = "${var.name}/platform/internal-token"
  kms_key_id = aws_kms_key.secrets.arn
}

resource "aws_secretsmanager_secret_version" "internal_token" {
  secret_id     = aws_secretsmanager_secret.internal_token.id
  secret_string = random_password.internal_token.result
}

# Unused when xdb.secrets.provider=aws, but the services require the property to be set.
resource "random_password" "local_key" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "local_key" {
  name       = "${var.name}/platform/local-key"
  kms_key_id = aws_kms_key.secrets.arn
}

resource "aws_secretsmanager_secret_version" "local_key" {
  secret_id     = aws_secretsmanager_secret.local_key.id
  secret_string = random_password.local_key.result
}
