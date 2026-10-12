# Remote state in S3 with DynamoDB locking. Create the bucket and table once (bootstrap),
# then fill these in or pass them with -backend-config.
terraform {
  backend "s3" {
    # bucket         = "xdb-terraform-state-<account-id>"
    # key            = "xdb-replication/terraform.tfstate"
    # region         = "ap-south-1"
    # dynamodb_table = "xdb-terraform-locks"
    # encrypt        = true
  }
}
