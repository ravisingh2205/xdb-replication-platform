# EKS runs every xDB workload: gateway, publication/subscription/apply services, Kafka Connect, UI.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.name
  cluster_version = var.eks_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access       = true
  cluster_endpoint_public_access_cidrs = var.eks_admin_cidrs

  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = true

  # Envelope encryption of Kubernetes secrets with KMS.
  create_kms_key = true
  cluster_encryption_config = { resources = ["secrets"] }

  cluster_addons = {
    coredns                         = {}
    kube-proxy                      = {}
    vpc-cni                         = { before_compute = true, configuration_values = jsonencode({ enableNetworkPolicy = "true" }) }
    eks-pod-identity-agent          = {}
    amazon-cloudwatch-observability = {} # Container Insights: logs + metrics to CloudWatch
  }

  eks_managed_node_groups = {
    general = {
      instance_types = var.node_instance_types
      min_size       = var.node_min
      max_size       = var.node_max
      desired_size   = var.node_min
      capacity_type  = "ON_DEMAND"
      iam_role_additional_policies = {
        cloudwatch = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
      }
    }
  }
}
