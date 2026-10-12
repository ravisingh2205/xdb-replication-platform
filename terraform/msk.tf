# Amazon MSK: CDC topics, config topics, DLQs, Kafka Connect internal topics.
resource "aws_security_group" "msk" {
  name        = "${local.name}-msk"
  description = "MSK brokers: IAM/TLS from EKS nodes only"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Kafka SASL/IAM over TLS"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_kms_key" "msk" {
  description         = "${local.name} MSK encryption at rest"
  enable_key_rotation = true
}

resource "aws_msk_configuration" "this" {
  name           = "${local.name}-config"
  kafka_versions = [var.msk_kafka_version]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    unclean.leader.election.enable=false
    log.retention.hours=168
  PROPS
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 14
}

resource "aws_msk_cluster" "this" {
  cluster_name           = local.name
  kafka_version          = var.msk_kafka_version
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = var.msk_broker_storage_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  # Per-partition metrics include consumer-group lag (MaxOffsetLag / SumOffsetLag) for alarms.
  enhanced_monitoring = "PER_TOPIC_PER_PARTITION"

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

locals {
  # arn:aws:kafka:<region>:<acct>:cluster/<name>/<uuid>  ->  topic/... and group/... ARNs
  msk_topic_arn_prefix = replace(aws_msk_cluster.this.arn, ":cluster/", ":topic/")
  msk_group_arn_prefix = replace(aws_msk_cluster.this.arn, ":cluster/", ":group/")
}
