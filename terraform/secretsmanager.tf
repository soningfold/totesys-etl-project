resource "aws_secretsmanager_secret" "db_credentials_" {
  name_prefix = "totesys-credentials-"
}

resource "aws_secretsmanager_secret_version" "db_credentials_" {
  secret_id     = aws_secretsmanager_secret.db_credentials_.id
  secret_string = jsonencode({
    user = var.DB_UN
    password = var.DB_PW
    host     = var.DB_HT
    database = var.DB_NAME
    port     = var.DB_PT
  })

  depends_on = [aws_secretsmanager_secret.db_credentials_]
}


resource "aws_secretsmanager_secret" "dw_credentials_" {
  name_prefix = "totesys-data-warehouse-credentials-"
}

resource "aws_secretsmanager_secret_version" "dw_credentials_" {
  secret_id     = aws_secretsmanager_secret.dw_credentials_.id
  secret_string = jsonencode({
    user = var.DB_UN
    port     = var.DB_PT
    password = var.DW_PW
    name     = var.DW_NAME
    host     = var.DW_HT
  })

  depends_on = [aws_secretsmanager_secret.dw_credentials_]
}
