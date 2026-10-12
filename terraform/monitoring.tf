# CloudWatch alarms on the two signals that matter for replication health.

# Consumer lag of an apply consumer group (MSK enhanced monitoring publishes it per group/topic).
resource "aws_cloudwatch_metric_alarm" "apply_lag" {
  alarm_name          = "${local.name}-apply-consumer-lag"
  alarm_description   = "xDB apply-service is falling behind (sum of lag across CDC topics)"
  namespace           = "AWS/Kafka"
  metric_name         = "SumOffsetLag"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 100000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    "Cluster Name"   = aws_msk_cluster.this.cluster_name
    "Consumer Group" = "xdb-apply-orders_sub"
  }
  alarm_actions = var.alarm_sns_topic_arn == "" ? [] : [var.alarm_sns_topic_arn]
}

# Broker disk: CDC retention must not fill the brokers.
resource "aws_cloudwatch_metric_alarm" "msk_disk" {
  alarm_name          = "${local.name}-msk-disk"
  namespace           = "AWS/Kafka"
  metric_name         = "KafkaDataLogsDiskUsed"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  dimensions = {
    "Cluster Name" = aws_msk_cluster.this.cluster_name
  }
  alarm_actions = var.alarm_sns_topic_arn == "" ? [] : [var.alarm_sns_topic_arn]
}
