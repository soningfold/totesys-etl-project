import boto3
import logging
import os
from src.utils.extract_utils import create_time_based_path, get_secret, connect_to_bucket, connect_to_db, query_db, create_and_upload_csv, compare_csvs
from botocore.exceptions import ClientError

"""
RAW DATA BUCKET STRUCTURE:
source/
├─ address_new.csv
├─ counterparty_new.csv
├─ currency_new.csv
├─ department_new.csv
├─ design_new.csv
├─ payment_new.csv
├─ payment_type_new.csv
├─ purchase_order_new.csv
├─ sales_order_new.csv
├─ staff_new.csv
├─ transaction_new.csv
history/
├─ year/
│  ├─ month/
│  │  ├─ day/
│  │  │  ├─ hh:mm:ss/
│  │  │  │  ├─ address_differences.csv
│  │  │  │  ├─ counterparty_differences.csv
│  │  │  │  ├─ currency_differences.csv
│  │  │  │  ├─ department_differences.csv
│  │  │  │  ├─ design_differences.csv
│  │  │  │  ├─ payment_differences.csv
│  │  │  │  ├─ payment_type_differences.csv
│  │  │  │  ├─ purchase_order_differences.csv
│  │  │  │  ├─ sales_order_differences.csv
│  │  │  │  ├─ staff_differences.csv
│  │  │  │  ├─ transaction_differences.csv
"""

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SOURCE_PATH = "/source/"
SOURCE_FILE_SUFFIX = "_new"
HISTORY_PATH = "/history/"
DATA_TABLES = [
    "sales_order",
    "design",
    "currency",
    "staff",
    "counterparty",
    "address",
    "department",
    "purchase_order",
    "payment_type",
    "payment",
    "transaction",
]


def lambda_handler(event, context):
    """
    Wrapper function that runs utils functions together.
    This function establishes a connection to the Totesys online database
    using credentials obtained from an AWS secret.
    It retrieves the latest data tables and compares them with previously
    stored versions (found in the /source/ directory with an "_original" suffix).
    The differences between the current and previous tables are saved
    as CSV files, organized in a directory structure based on
    the current date and time (year/month/day/hh:mm:ss).
    Finally, the existing CSV files in the /source/ directory,
    which hold the complete data tables, are updated with the latest content.
    """

    db_credentials = get_secret()
    s3_client = boto3.client("s3")
    raw_data_bucket = connect_to_bucket(s3_client)
    time_path = create_time_based_path()
    bucket_content = s3_client.list_objects(Bucket=raw_data_bucket)

    if bucket_content.get("Contents"):
        bucket_files = [dict_["Key"] for dict_ in bucket_content["Contents"]]
    else:
        bucket_files = []

    try:
        conn = connect_to_db(db_credentials)
        for data_table_name in DATA_TABLES:
            file_data = query_db(data_table_name, conn)
            first_call_bool = (
                f"{SOURCE_PATH}{data_table_name}{SOURCE_FILE_SUFFIX}.csv"
                not in bucket_files
            )
            create_and_upload_csv(
                file_data,
                s3_client,
                raw_data_bucket,
                data_table_name,
                time_path,
                first_call_bool,
            )

            if not first_call_bool:

                # save a copy of _new from /source to /tmp, where it can be manipulated by the lambda function
                s3_client.download_file(
                    Bucket=raw_data_bucket,
                    Key=f"{SOURCE_PATH}{data_table_name}{SOURCE_FILE_SUFFIX}.csv",
                    Filename=f"/tmp/{data_table_name}.csv",
                )

                changes_csv = compare_csvs(data_table_name)

                # save the _differences file to history
                s3_client.upload_file(
                    Bucket=raw_data_bucket,
                    Filename=f"/tmp/{changes_csv}",
                    Key=f"{HISTORY_PATH}{time_path}{changes_csv}",
                )

                # replace /source/*_new with /tmp/*_new
                s3_client.upload_file(
                    Bucket=raw_data_bucket,
                    Filename=f"/tmp/{data_table_name}_new.csv",
                    Key=f"{SOURCE_PATH}{data_table_name}{SOURCE_FILE_SUFFIX}.csv",
                )

                # removing the temporary files
                os.remove(f"/tmp/{data_table_name}.csv")
                os.remove(f"/tmp/{data_table_name}_new.csv")

        logging.info(f"Successfully uploaded raw data to {raw_data_bucket}")

    except ClientError as e:
        logging.error(e)
        raise Exception(f"Connection to database failed: {e}")

    finally:
        if "conn" in locals():
            conn.close()

    return {"time_prefix": time_path}
