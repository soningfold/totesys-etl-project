
data "archive_file" "extract_layer" { # create a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir      = "${path.module}/../layer/extract_layer" 
  output_path      = "${path.module}/../zip_code/extract_layer.zip"
}

data "archive_file" "transform_layer" { # create a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir      = "${path.module}/../layer/transform_layer" 
  output_path      = "${path.module}/../zip_code/transform_layer.zip"
}

data "archive_file" "load_layer" { # create a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir      = "${path.module}/../layer/load_layer" 
  output_path      = "${path.module}/../zip_code/load_layer.zip"
}

resource "aws_s3_object" "extract_layer_zip" { #Upload the layer zip to the code_lambda_bucket.
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/extract_layer.zip" 
  key    = "extract_layer.zip"
}

resource "aws_s3_object" "transform_layer_zip" { #Upload the layer zip to the code_lambda_bucket.
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/transform_layer.zip" 
  key    = "transform_layer.zip"
}

resource "aws_s3_object" "load_layer_zip" { #Upload the layer zip to the code_lambda_bucket.
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/load_layer.zip" 
  key    = "load_layer.zip"
}

resource "aws_lambda_layer_version" "extract_lambda_layer" { #create layer
  layer_name = "extract_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.lambda_bucket.bucket # or aws_s3_bucket.lambda_bucket.id ?
  s3_key              = aws_s3_object.extract_layer_zip.key
  depends_on          = [aws_s3_object.extract_layer_zip] # triggered only if the zip file is uploaded to the bucket
}

resource "aws_lambda_layer_version" "transform_lambda_layer" { #create layer
  layer_name = "transform_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.lambda_bucket.bucket # or aws_s3_bucket.lambda_bucket.id ?
  s3_key              = aws_s3_object.transform_layer_zip.key
  depends_on          = [aws_s3_object.transform_layer_zip] # triggered only if the zip file is uploaded to the bucket

}

resource "aws_lambda_layer_version" "load_lambda_layer" { #create layer
  layer_name = "load_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.lambda_bucket.bucket # or aws_s3_bucket.lambda_bucket.id ?
  s3_key              = aws_s3_object.load_layer_zip.key
  depends_on          = [aws_s3_object.load_layer_zip] # triggered only if the zip file is uploaded to the bucket
}
