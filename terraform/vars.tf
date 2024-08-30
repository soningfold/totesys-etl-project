variable "lambda_bucket" {
  type    = string
  default = "totesys-lambda-"
}

variable "raw_data" {
  type    = string
  default = "totesys-raw-data-"
}

variable "processed_data" {
  type    = string
  default = "totesys-processed-data-"
}

variable "athena_queries" {
  type    = string
  default = "totesys-athena-queries-"
}

variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "transform_lambda" {
  type    = string
  default = "transform"
}

variable "load_lambda" {
  type    = string
  default = "load"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

# variables for secrets manager
variable "DB_UN" {
  description = "Database username"
  type        = string
  sensitive   = true # does not print to console
}

variable "DB_PW" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "DB_NAME" {
  description = "Database name"
  type        = string
  sensitive   = true
}

variable "DB_HT" {
  description = "Database host"
  type        = string
  sensitive   = true
}

variable "DB_PT" {
  description = "Database port"
  type        = string
  sensitive   = true
}

variable "DW_PW" {
  description = "Data warehouse password"
  type        = string
  sensitive   = true
}

variable "DW_NAME" {
  description = "Data warehouse name"
  type        = string
  sensitive   = true
}

variable "DW_HT" {
  description = "Data warehouse host"
  type        = string
  sensitive   = true
}