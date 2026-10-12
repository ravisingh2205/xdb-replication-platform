variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "name" {
  description = "Prefix for resource names"
  type        = string
  default     = "xdb"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "eks_version" {
  type    = string
  default = "1.31"
}

variable "node_instance_types" {
  description = "Instance types for the EKS managed node group"
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "node_min" {
  type    = number
  default = 3
}

variable "node_max" {
  type    = number
  default = 9
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "msk_kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "msk_broker_storage_gb" {
  type    = number
  default = 500
}

variable "rds_instance_class" {
  type    = string
  default = "db.t4g.medium"
}

variable "eks_admin_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "alarm_sns_topic_arn" {
  description = "SNS topic for CloudWatch alarms (optional)"
  type        = string
  default     = ""
}
