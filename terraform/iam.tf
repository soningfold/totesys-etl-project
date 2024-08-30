resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-etl-lambda-"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
} # Lambda - Role

resource "aws_iam_role" "iam_for_sfn" {
  name_prefix        = "role-etl-sfn-"
  assume_role_policy = data.aws_iam_policy_document.state_machine_assume_role_policy.json
} # Step Func - Role

resource "aws_iam_role" "iam_for_scheduler" {
  name_prefix        = "role-scheduler-"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role_policy.json
} # Scheduler - Role

data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
} # Lambda - Assume Role

data "aws_iam_policy_document" "state_machine_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
} # Step Func - Assume Role

data "aws_iam_policy_document" "scheduler_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]

  }
} # Scheduler - Assume Role

data "aws_iam_policy_document" "s3_list_bucket" {
  statement {

    actions = ["s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.processed_data_bucket.arn}",
      "${aws_s3_bucket.raw_data_bucket.arn}"
    ]
  }
} # Lambda - s3 buckets

data "aws_iam_policy_document" "s3_list_all_buckets" {
  statement {

    actions = ["s3:ListAllMyBuckets"]

    resources = ["*"]
  }
} # Lambda - list all s3 buckets

data "aws_iam_policy_document" "secrets_manager_access_secrets" {
  statement {
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:ListSecrets"]

    resources = ["*"]
  }
} # Lambda - allow secrets access

data "aws_iam_policy_document" "s3_read_write_object" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"]

    resources = [
      "*"
    ]
  }
} # Lambda - s3 objects

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*:*"
    ]
  }
} # Lambda - cloudwatch

resource "aws_cloudwatch_log_group" "errors_log_group" {
  name = "/aws/lambda/errors_log_group"
}

data "aws_iam_policy_document" "lambda_invoke_document" {
  statement {
    actions = ["lambda:InvokeFunction"]

    resources = [
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.extract_lambda}:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda}:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda}:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.extract_lambda}",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda}",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda}"
    ]
  }
} # Step func - Lambda invoke

data "aws_iam_policy_document" "x_ray_document" {
  statement {
    actions = ["xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
    "xray:GetSamplingTargets"]
    resources = ["*"]
  }
} # Step func - xray

data "aws_iam_policy_document" "step_function_cw_document" {
  statement {
    actions = ["logs:CreateLogDelivery",
      "logs:CreateLogStream",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutLogEvents",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]
    resources = ["*"]
  }
} # Step func - Cloudwatch

data "aws_iam_policy_document" "step_function_sns_document" {
  statement {
    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.error.arn]
  }
} # Step func = Sns

data "aws_iam_policy_document" "scheduler_document" {
  statement {
    actions = ["states:StartExecution"]

    resources = ["*"]
  }
} # Scheduler - Step func execution


resource "aws_iam_policy" "s3_read_write_object_policy" {
  name_prefix = "s3-object-policy-etl-lambdas-"
  policy      = data.aws_iam_policy_document.s3_read_write_object.json
} # Lambda - Policy

resource "aws_iam_policy" "s3_list_bucket_policy" {
  name_prefix = "s3-bucket-policy-etl-lambdas-"
  policy      = data.aws_iam_policy_document.s3_list_bucket.json
} # Lambda - Policy

resource "aws_iam_policy" "s3_list_all_buckets_policy" {
  name_prefix = "s3-all-bucket-policy-etl-lambdas-"
  policy      = data.aws_iam_policy_document.s3_list_all_buckets.json
} # Lambda - Policy

resource "aws_iam_policy" "secrets_manager_get_secret_policy" {
  name_prefix = "secrets-manager-get-secrets-lambda-"
  policy      = data.aws_iam_policy_document.secrets_manager_access_secrets.json
} # Lambda - Policy

resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cloudwatch-policy-etl-lambdas-"
  policy      = data.aws_iam_policy_document.cw_document.json
} # Lambda - Policy

resource "aws_iam_policy" "lambda_invoke_policy" {
  name_prefix = "step-func-lambda-invoke-"
  policy      = data.aws_iam_policy_document.lambda_invoke_document.json
} # Step Func - Policy

resource "aws_iam_policy" "xray_policy" {
  name_prefix = "step-func-xray-"
  policy      = data.aws_iam_policy_document.x_ray_document.json
} # Step Func - Policy

resource "aws_iam_policy" "step_cw_policy" {
  name_prefix = "step-func-cw-"
  policy      = data.aws_iam_policy_document.step_function_cw_document.json
} # Step Func - Policy

resource "aws_iam_policy" "step_sns_policy" {
  name_prefix = "step-func-sns-"
  policy      = data.aws_iam_policy_document.step_function_sns_document.json
} # Step Func - Policy

resource "aws_iam_policy" "scheduler_policy" {
  name_prefix = "scheduler-policy-"
  policy      = data.aws_iam_policy_document.scheduler_document.json
} # Scheduler - Policy


resource "aws_iam_role_policy_attachment" "s3_read_write_object_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_object_policy.arn
} # Lambda - Attach

resource "aws_iam_role_policy_attachment" "s3_list_bucket_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_list_bucket_policy.arn
} # Lambda - Attach

resource "aws_iam_role_policy_attachment" "s3_list_all_buckets_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_list_all_buckets_policy.arn
} # Lambda - Attach

resource "aws_iam_role_policy_attachment" "secrets_manager_get_secret_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_get_secret_policy.arn
} # Lambda - Attach

resource "aws_iam_role_policy_attachment" "cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
} # Lambda - Attach

resource "aws_iam_role_policy_attachment" "lambda_invoke_policy_attachment" {
  role       = aws_iam_role.iam_for_sfn.name
  policy_arn = aws_iam_policy.lambda_invoke_policy.arn
} # Step Func - Attach

resource "aws_iam_role_policy_attachment" "xray_policy_attachment" {
  role       = aws_iam_role.iam_for_sfn.name
  policy_arn = aws_iam_policy.xray_policy.arn
} # Step Func - Attach

resource "aws_iam_role_policy_attachment" "step_function_cw_policy_attachment" {
  role       = aws_iam_role.iam_for_sfn.name
  policy_arn = aws_iam_policy.step_cw_policy.arn
} # Step Func - Attach

resource "aws_iam_role_policy_attachment" "step_function_sns_policy_attachment" {
  role       = aws_iam_role.iam_for_sfn.name
  policy_arn = aws_iam_policy.step_sns_policy.arn
} # Step Func - Attach

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.iam_for_scheduler.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
} # Scheduler - Attach
