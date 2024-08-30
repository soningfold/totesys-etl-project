import boto3
import logging
import polars as pl
import os
from botocore.exceptions import ClientError


def finds_data_buckets():
    """
    This function finds the raw data and processed data buckets on AWS S3.

    Contains error handling for if either or both data buckets are missing.

    Returns:
        raw_data_bucket (string): string containing full name of the raw data bucket
        processed_data_bucket (string): string containing full name of the processed data bucket
    """
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found_processed = False
    found_raw = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            raw_data_bucket = bucket["Name"]
            found_raw = True
        if bucket["Name"].startswith("totesys-processed-data-"):
            processed_data_bucket = bucket["Name"]
            found_processed = True
        if found_raw and found_processed:
            break

    if not found_raw and not found_processed:
        logging.error("No buckets found")
        #return "No buckets found"
        raise Exception("No buckets found")
    elif not found_raw:
        logging.error("No raw data bucket found")
        #return "No raw data bucket found"
        raise Exception("No raw data bucket found")
    elif not found_processed:
        logging.error("No processed data bucket found")
        #return "No processed data bucket found"
        raise Exception("No processed data bucket found")

    return raw_data_bucket, processed_data_bucket


def create_star_schema_from_sales_order_csv_file(prefix):
    """
    This function looks for csv files in our raw data bucket, downloads the ones needed
    to create the star schema for our minimal viable product, reads them in as polars
    dataframes, reformats the dataframes into our star schema, saves the dataframes as
    parquet files, and uploads the parquet files to our processed data bucket

    Args:
        prefix - used to retrieve csv files from specific folder and save parquet to specific folder
    """
    s3_client = boto3.client("s3")
    raw_data_bucket, processed_data_bucket = finds_data_buckets()

    try:
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/sales_order_new.csv",
            Key=f"/history/{prefix}sales_order_differences.csv",
        )
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/staff_new.csv",
            Key=f"/history/{prefix}staff_differences.csv",
        )
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/counterparty_new.csv",
            Key=f"/history/{prefix}counterparty_differences.csv",
        )
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/currency_new.csv",
            Key=f"/history/{prefix}currency_differences.csv",
        )
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/address_new.csv",
            Key=f"/history/{prefix}address_differences.csv",
        )
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/design_new.csv",
            Key=f"/history/{prefix}design_differences.csv",
        )
        s3_client.download_file(
            Bucket=raw_data_bucket,
            Filename="/tmp/department_new.csv",
            Key=f"/history/{prefix}department_differences.csv",
        )
    except ClientError as e:
        logging.error(e)
        raise Exception("Failed to download file")
    
    fact_sales_order = pl.read_csv("/tmp/sales_order_new.csv")
    dim_staff = pl.read_csv("/tmp/staff_new.csv")
    dim_counterparty = pl.read_csv("/tmp/counterparty_new.csv")
    dim_currency = pl.read_csv("/tmp/currency_new.csv")
    dim_location = pl.read_csv("/tmp/address_new.csv")
    dim_design = pl.read_csv("/tmp/design_new.csv")
    department = pl.read_csv("/tmp/department_new.csv")

    fact_sales_order = fact_sales_order.with_columns(
        pl.col("created_at").str.to_datetime().cast(pl.Date).alias("created_date"),
        pl.col("created_at").str.to_datetime().cast(pl.Time).alias("created_time"),
    )
    fact_sales_order = fact_sales_order.drop("created_at")
    fact_sales_order = fact_sales_order.with_columns(
        pl.col("last_updated")
        .str.to_datetime()
        .cast(pl.Date)
        .alias("last_updated_date"),
        pl.col("last_updated")
        .str.to_datetime()
        .cast(pl.Time)
        .alias("last_updated_time"),
    )
    fact_sales_order = fact_sales_order.drop("last_updated")
    fact_sales_order = fact_sales_order.with_columns(
        pl.col("agreed_delivery_date").str.to_datetime().cast(pl.Date)
    )
    fact_sales_order = fact_sales_order.with_columns(
        pl.col("agreed_payment_date").str.to_datetime().cast(pl.Date)
    )

    dim_date = pl.DataFrame(
        pl.concat(
            [
                fact_sales_order["created_date"],
                fact_sales_order["last_updated_date"],
                fact_sales_order["agreed_delivery_date"],
                fact_sales_order["last_updated_date"],
                fact_sales_order["agreed_payment_date"],
            ]
        )
    ).unique()
    dim_date = dim_date.with_row_index("date_id", offset=1)
    dim_date = dim_date.with_columns(
        pl.col("created_date").alias("date_id"),
        pl.col("created_date").dt.year().alias("year"),
        pl.col("created_date").dt.month().alias("month"),
        pl.col("created_date").dt.day().alias("day"),
        pl.col("created_date").dt.quarter().alias("quarter"),
        pl.col("created_date").dt.weekday().alias("day_of_week"),
        pl.col("created_date").dt.to_string("%A").alias("day_name"),
        pl.col("created_date").dt.to_string("%B").alias("month_name"),
    )

    dim_staff = dim_staff.join(
        department, left_on="department_id", right_on="department_id"
    ).sort("staff_id")
    dim_staff = dim_staff.drop(
        [
            "department_id",
            "manager",
            "created_at_right",
            "last_updated_right",
            "created_at",
            "last_updated",
        ]
    )

    dim_location = dim_location.with_row_index("location_id", offset=1)
    dim_location = dim_location.drop(["created_at", "last_updated", "address_id"])

    dim_counterparty = dim_counterparty.join(
        dim_location,
        right_on=pl.col("location_id").cast(pl.Int64),
        left_on=pl.col("legal_address_id").cast(pl.Int64),
    )
    dim_counterparty = dim_counterparty.drop(
        [
            "created_at",
            "last_updated",
            "legal_address_id",
            "commercial_contact",
            "location_id",
            "delivery_contact",
        ]
    )
    dim_counterparty = dim_counterparty.rename(
        {
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        }
    )

    if dim_currency.height == 3:
        dim_currency = dim_currency.with_columns(
            currency_name=pl.Series(["Great British Pound", "US Dollars", "Euros"])
        )
    else:
        dim_currency = dim_currency.with_columns(
            currency_name=None
        )
    dim_currency = dim_currency.drop(["last_updated", "created_at"])

    dim_design = dim_design.sort("design_id")
    dim_design = dim_design.drop(["created_at", "last_updated"])

    fact_sales_order.write_parquet("/tmp/fact_sales_order.parquet")
    dim_staff.write_parquet("/tmp/dim_staff.parquet")
    dim_design.write_parquet("/tmp/dim_design.parquet")
    dim_currency.write_parquet("/tmp/dim_currency.parquet")
    dim_counterparty.write_parquet("/tmp/dim_counterparty.parquet")
    dim_location.write_parquet("/tmp/dim_location.parquet")
    dim_date.write_parquet("/tmp/dim_date.parquet")

    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/fact_sales_order.parquet",
        Key=f"/history/{prefix}/fact_sales_order.parquet",
    )
    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/dim_design.parquet",
        Key=f"/history/{prefix}/dim_design.parquet",
    )
    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/dim_staff.parquet",
        Key=f"/history/{prefix}/dim_staff.parquet",
    )
    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/dim_location.parquet",
        Key=f"/history/{prefix}/dim_location.parquet",
    )
    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/dim_counterparty.parquet",
        Key=f"/history/{prefix}/dim_counterparty.parquet",
    )
    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/dim_date.parquet",
        Key=f"/history/{prefix}/dim_date.parquet",
    )
    s3_client.upload_file(
        Bucket=processed_data_bucket,
        Filename="/tmp/dim_currency.parquet",
        Key=f"/history/{prefix}/dim_currency.parquet",
    )

    for file in os.listdir("/tmp/"):
        if "csv" in file or "parquet" in file:
            os.remove(f"/tmp/{file}")
