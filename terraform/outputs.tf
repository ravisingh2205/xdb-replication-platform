output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "configure_kubectl" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "msk_bootstrap_brokers_sasl_iam" {
  description = "KAFKA_BOOTSTRAP_SERVERS for the services and Kafka Connect"
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "rds_endpoint" {
  value = aws_db_instance.metadata.address
}

output "ecr_repositories" {
  value = { for k, r in aws_ecr_repository.this : k => r.repository_url }
}

output "irsa_role_arns" {
  description = "Annotate the matching Kubernetes service accounts with these (k8s/overlays/aws/irsa-patch.yaml)"
  value       = { for k, m in module.irsa : k => m.iam_role_arn }
}
