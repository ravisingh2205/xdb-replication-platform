# RDS for PostgreSQL: the metadata/config store (publication and subscription schemas).
resource "aws_security_group" "rds" {
  name        = "${local.name}-metadata-db"
  description = "Metadata DB: Postgres from EKS nodes only"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
}

resource "aws_db_subnet_group" "metadata" {
  name       = "${local.name}-metadata"
  subnet_ids = module.vpc.private_subnets
}

resource "random_password" "metadata_db" {
  length  = 32
  special = false
}

resource "aws_db_instance" "metadata" {
  identifier     = "${local.name}-metadata"
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.rds_instance_class

  db_name  = "xdb_metadata"
  username = "xdb"
  password = random_password.metadata_db.result

  allocated_storage     = 50
  max_allocated_storage = 200
  storage_encrypted     = true

  multi_az               = var.environment == "prod"
  db_subnet_group_name   = aws_db_subnet_group.metadata.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  backup_retention_period         = 7
  deletion_protection             = var.environment == "prod"
  skip_final_snapshot             = var.environment != "prod"
  final_snapshot_identifier       = "${local.name}-metadata-final"
  performance_insights_enabled    = true
  enabled_cloudwatch_logs_exports = ["postgresql"]
}
