resource "aws_cloudwatch_log_metric_filter" "error" {
  name           = "error"
  pattern        = ""
  log_group_name = aws_cloudwatch_log_group.errors_log_group.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "Errors"
    value     = "1"
  }
}

resource "aws_sns_topic" "error" {
  name = "error"
}

resource "aws_sns_topic_policy" "sns_policy" {
  arn    = aws_sns_topic.error.arn
  policy = <<POLICY
  {
      "Version":"2012-10-17",
      "Statement":[{
          "Effect": "Allow",
          "Principal": {"Service":"s3.amazonaws.com"},
          "Action": "SNS:Publish",
          "Resource":  "${aws_sns_topic.error.arn}"
      }]
  }
  POLICY
}

resource "aws_sns_topic_subscription" "email-target" {
  topic_arn = aws_sns_topic.error.arn
  protocol  = "email"
  endpoint  = "kastriotdumani1@gmail.com"
}

resource "aws_cloudwatch_metric_alarm" "error_alarm" {
  alarm_name          = "error"
  metric_name         = aws_cloudwatch_log_metric_filter.error.name
  threshold           = "0"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "60"
  namespace           = "Errors"
  alarm_actions       = [aws_sns_topic_subscription.email-target.arn, aws_sns_topic_policy.sns_policy.arn]
}
