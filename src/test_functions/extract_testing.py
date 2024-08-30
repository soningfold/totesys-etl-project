import boto3
import logging
import os
import csv
from datetime import datetime as dt
from pg8000.native import Connection, Error
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv
from io import StringIO

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
load_dotenv(env_file)

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
DATABASE = os.getenv("PG_DATABASE")
HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")

data_tables = [
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

year = dt.now().year
month = dt.now().month
day = dt.now().day
hour = dt.now().hour
minute = dt.now().minute
second = dt.now().second

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    save_file_path_prefix = "./data/table_data/"
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            raw_data_bucket = bucket["Name"]
            found = True
            break

    if not found:
        logging.error("No raw data bucket found")
        return "No raw data bucket found"

    time_prefix = f"{year}/{month}/{day}/{hour}-{minute}-{second}/"

    try:
        conn = Connection(
            user=PG_USER,
            password=PG_PASSWORD,
            host=HOST,
            database=DATABASE,
            port=PORT,
        )

        for data_table in data_tables:
            query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{data_table}';"
            column_names = conn.run(query)

            header = []
            for column in column_names:
                header.append(column[0])

            query = f"SELECT * FROM {data_table};"
            data_rows = conn.run(query)

            file_to_save = StringIO()
            file_data = [header] + data_rows
            csv.writer(file_to_save).writerows(file_data)
            file_to_save = bytes(file_to_save.getvalue(), encoding="utf-8")

            try:
                s3_client.put_object(
                    Body=file_to_save,
                    Bucket=raw_data_bucket,
                    Key=f"{time_prefix}{data_table}.csv",
                )

            except ClientError as e:
                logging.error(e)
                return "Failed to upload file"

            with open(
                f"{save_file_path_prefix}{data_table}.csv", "w", newline=""
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(data_rows)

        logging.info(f"Successfully uploaded raw data to {raw_data_bucket}")

    except Error as e:
        logging.error(e["M"])
        return f"Connection to database failed: {e['M']}"

    finally:
        conn.close()

    return f"Successfully uploaded raw data to {raw_data_bucket}"
