data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source {
    content  = file("${path.module}/../src/lambda_functions/extract.py")
    filename = "extract.py"
  }

  source {
    content  = file("${path.module}/../src/utils/extract_utils.py")
    filename = "src/utils/extract_utils.py"
  }

  output_path = "${path.module}/../zip_code/extract.zip"
}

data "archive_file" "load_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source {
    content  = file("${path.module}/../src/lambda_functions/load.py")
    filename = "load.py"
  }
  source {
    content  = file("${path.module}/../src/utils/load_utils.py")
    filename = "src/utils/load_utils.py"
  }
  output_path      = "${path.module}/../zip_code/load.zip"
}

data "archive_file" "transform_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source {
    content  = file("${path.module}/../src/lambda_functions/transform.py")
    filename = "transform.py"
  }

  source {
    content  = file("${path.module}/../src/utils/transform_utils.py")
    filename = "src/utils/transform_utils.py"
  }

  output_path = "${path.module}/../zip_code/transform.zip"
}

resource "aws_s3_object" "extract_lambda_zip" {
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/extract.zip"
  key    = "extract_lambda.zip"
  etag   = filebase64sha256(data.archive_file.extract_lambda.output_path)
  metadata = {
    last_updated = timestamp()
  }
}

resource "aws_s3_object" "load_lambda_zip" {
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/load.zip"
  key    = "load_lambda.zip"
  etag   = filebase64sha256(data.archive_file.load_lambda.output_path)
  metadata = {
    last_updated = timestamp()
  }
}

resource "aws_s3_object" "transform_lambda_zip" {
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/transform.zip"
  key    = "transform.zip"
  etag   = filebase64sha256(data.archive_file.load_lambda.output_path)
  metadata = {
    last_updated = timestamp()
  }
}

resource "aws_lambda_function" "extract_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.extract_lambda_zip.key
  function_name    = "extract"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.extract_lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "extract.lambda_handler"
  timeout          = 120
}

resource "aws_lambda_function" "load_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.load_lambda_zip.key
  function_name    = "load"
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.load_lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "load.lambda_handler"
  timeout          = 120
}

resource "aws_lambda_function" "transform_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.transform_lambda_zip.key
  function_name    = "transform"
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.transform_lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "transform.lambda_handler"
  timeout          = 120
}
