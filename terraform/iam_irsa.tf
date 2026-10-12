# IRSA: one IAM role per Kubernetes service account, least privilege. No static AWS keys in the cluster.
data "aws_caller_identity" "current" {}

locals {
  secrets_prefix_arn = "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:${var.name}/"

  msk_connect = {
    Effect   = "Allow"
    Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    Resource = [aws_msk_cluster.this.arn]
  }
  msk_topics_rw = {
    Effect = "Allow"
    Action = [
      "kafka-cluster:CreateTopic", "kafka-cluster:DescribeTopic", "kafka-cluster:AlterTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration", "kafka-cluster:WriteData", "kafka-cluster:ReadData",
    ]
    Resource = ["${local.msk_topic_arn_prefix}/*"]
  }
  msk_groups = {
    Effect   = "Allow"
    Action   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    Resource = ["${local.msk_group_arn_prefix}/*"]
  }
}

# publication-service: writes config events, manages xdb/source/* secrets.
resource "aws_iam_policy" "publication" {
  name = "${local.name}-publication-service"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.msk_connect, local.msk_topics_rw, local.msk_groups,
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:CreateSecret", "secretsmanager:PutSecretValue", "secretsmanager:DeleteSecret"]
        Resource = ["${local.secrets_prefix_arn}source/*"]
      },
      { Effect = "Allow", Action = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"], Resource = [aws_kms_key.secrets.arn] },
    ]
  })
}

# subscription-service: writes config events, manages xdb/target/* secrets.
resource "aws_iam_policy" "subscription" {
  name = "${local.name}-subscription-service"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.msk_connect, local.msk_topics_rw, local.msk_groups,
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:CreateSecret", "secretsmanager:PutSecretValue", "secretsmanager:DeleteSecret"]
        Resource = ["${local.secrets_prefix_arn}target/*"]
      },
      { Effect = "Allow", Action = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"], Resource = [aws_kms_key.secrets.arn] },
    ]
  })
}

# apply-service: reads CDC + config topics, writes DLQs, commits group offsets, reads target passwords.
resource "aws_iam_policy" "apply" {
  name = "${local.name}-apply-service"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.msk_connect, local.msk_topics_rw, local.msk_groups,
      { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = ["${local.secrets_prefix_arn}target/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = [aws_kms_key.secrets.arn] },
    ]
  })
}

# Kafka Connect: Debezium writes CDC topics; the ConfigProvider reads source DB passwords.
resource "aws_iam_policy" "connect" {
  name = "${local.name}-kafka-connect"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.msk_connect, local.msk_topics_rw, local.msk_groups,
      { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = ["${local.secrets_prefix_arn}source/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = [aws_kms_key.secrets.arn] },
    ]
  })
}

# KEDA operator: read consumer-group lag.
resource "aws_iam_policy" "keda" {
  name = "${local.name}-keda"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.msk_connect,
      { Effect = "Allow", Action = ["kafka-cluster:DescribeTopic"], Resource = ["${local.msk_topic_arn_prefix}/*"] },
      { Effect = "Allow", Action = ["kafka-cluster:DescribeGroup"], Resource = ["${local.msk_group_arn_prefix}/*"] },
    ]
  })
}

locals {
  irsa = {
    "publication-service"  = { sa = "xdb:publication-service", policy = aws_iam_policy.publication.arn }
    "subscription-service" = { sa = "xdb:subscription-service", policy = aws_iam_policy.subscription.arn }
    "apply-service"        = { sa = "xdb:apply-service", policy = aws_iam_policy.apply.arn }
    "kafka-connect"        = { sa = "xdb:kafka-connect", policy = aws_iam_policy.connect.arn }
    "keda-operator"        = { sa = "keda:keda-operator", policy = aws_iam_policy.keda.arn }
  }
}

module "irsa" {
  source   = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version  = "~> 5.44"
  for_each = local.irsa

  role_name = "${local.name}-${each.key}"
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = [each.value.sa]
    }
  }
  role_policy_arns = { main = each.value.policy }
}

module "irsa_lb_controller" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name                              = "${local.name}-aws-load-balancer-controller"
  attach_load_balancer_controller_policy = true
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-load-balancer-controller"]
    }
  }
}

module "irsa_external_secrets" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name                             = "${local.name}-external-secrets"
  attach_external_secrets_policy        = true
  external_secrets_secrets_manager_arns = ["${local.secrets_prefix_arn}platform/*"]
  external_secrets_kms_key_arns         = [aws_kms_key.secrets.arn]
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["external-secrets:external-secrets"]
    }
  }
}
