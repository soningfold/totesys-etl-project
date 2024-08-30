resource "aws_s3_bucket" "lambda_bucket" {
  bucket_prefix = var.lambda_bucket
  tags = {
    Name        = "Code bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "raw_data_bucket" {
  bucket_prefix = var.raw_data

  tags = {
    Name        = "Ingested data storage - raw"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "processed_data_bucket" {
  bucket_prefix = var.processed_data

  tags = {
    Name        = "Processed data storage - Parquet format"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "athena_queries" {
  bucket_prefix = var.athena_queries

  tags = {
    Name        = "Storage to store athena queries"
    Environment = "Dev"
  }
}