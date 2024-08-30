import boto3
import logging
import csv
import json
from datetime import datetime as dt
from pg8000.native import Connection
from botocore.exceptions import ClientError
from io import StringIO

HISTORY_PATH = "/history/"
SOURCE_PATH = "/source/"
SOURCE_FILE_SUFFIX = "_new"
DIFFERENCES_FILE_SUFFIX = "_differences"
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


def create_time_based_path():
    """
    Retrieves the current time at which the lambda function is invoked for use in
    the file structure and in returning a value for the lambda handler
    """
    current_time = dt.now()
    year = current_time.year
    month = current_time.month
    day = current_time.day
    hour = current_time.hour
    if len(str(month)) == 1:
        month = "0" + str(month)
    if len(str(day)) == 1:
        day = "0" + str(day)
    if len(str(hour)) == 1:
        hour = "0" + str(hour)
    minute = current_time.minute
    if len(str(minute)) == 1:
        minute = "0" + str(minute)
    second = current_time.second
    if len(str(second)) == 1:
        second = "0" + str(second)
    return f"{year}/{month}/{day}/{hour}:{minute}:{second}/"


def get_secret(secret_prefix="totesys-credentials-"):
    """
    Initialises a boto3 secrets manager client and retrieves secret from secrets manager
    based on argument given, with the default argument set to the database credentials.
    The secret returned should be a dictionary with 5 keys:
    user - the username for the database
    password - the password for the user
    host - the url of the server hosting the database
    port - which port we are using to connect with the database
    database - the name of the database that we want to connect to
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="eu-west-2")

    try:
        get_secrets_lists_response = client.list_secrets()
        for secret in get_secrets_lists_response["SecretList"]:
            if secret["Name"].startswith(secret_prefix):
                secret_name = secret["Name"]
                break
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(e)
        raise Exception(f"Can't retrieve secret due to {e}")

    return json.loads(secret_value_response["SecretString"])


def connect_to_bucket(client):
    """
    Searches for a raw data bucket within an AWS account and returns bucket name if
    bucket is found or raises exception if bucket is not found.
    """
    buckets = client.list_buckets()
    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            return bucket["Name"]
    logging.error("No raw data bucket found")
    raise Exception("No raw data bucket found")


def connect_to_db(credentials):
    """
    Uses the secret obtained in the get_secret method to establish a
    connection to the database
    """
    return Connection(
        user=credentials["user"],
        password=credentials["password"],
        host=credentials["host"],
        database=credentials["database"],
        port=credentials["port"],
    )


def query_db(dt_name, conn):
    """
    Does two queries to the database:
    1. Name of table's columns --> header of csv format file
    2. All table's content
    Returns data in csv format (header + data rows)
    """
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{dt_name}';"
    column_names = conn.run(query)
    header = []
    for column in column_names:
        header.append(column[0])

    query = f"SELECT * FROM {dt_name};"
    data_rows = conn.run(query)
    return [header] + data_rows


def create_and_upload_csv(data, client, bucket, tablename, time_path, first_call):
    """
    Converts a table from a database into a CSV file and uploads that CSV file to either:
    - first_call == True ? bucket/source as *_new.csv , and history/y/m/d/hh:mm:ss/*_differences.csv
    - first_call == False ? lamba ephemeral storage/tmp as *.csv
    The data argument is a list of lists.
    """
    file_to_save = StringIO()
    csv.writer(file_to_save).writerows(data)
    file_to_save = bytes(file_to_save.getvalue(), encoding="utf-8")

    try:
        if first_call:
            client.put_object(
                Body=file_to_save,
                Bucket=bucket,
                Key=f"{SOURCE_PATH}{tablename}{SOURCE_FILE_SUFFIX}.csv",
            )
            client.put_object(
                Body=file_to_save,
                Bucket=bucket,
                Key=f"{HISTORY_PATH}{time_path}{tablename}{DIFFERENCES_FILE_SUFFIX}.csv",
            )
        else:
            with open(f"/tmp/{tablename}_new.csv", "wb") as csvfile:
                csvfile.write(file_to_save)

    except ClientError as e:
        logging.error(e)
        raise Exception("Failed to upload file")


def compare_csvs(dt_name):
    """
    Takes two csvs (dt_name.csv, dt_name_new.csv) located in /tmp
    and compares the differences between them, returning an
    empty csv if no differences found.

    Arg: datatable name (= prefix of csv file name)

    Returns:
    csv file containing all changes to database (if csv1 and csv2 are not equal)
    None (if dt_name.csv, dt_name_new.csv are equal)
    """
    csv_prev = f"/tmp/{dt_name}.csv"
    csv_new = f"/tmp/{dt_name}_new.csv"

    with open(csv_prev, "r", newline="") as f_prev, open(csv_new, "r", newline="") as f_new:
        reader_prev = list(csv.reader(f_prev))
        reader_new = list(csv.reader(f_new))

        if reader_prev and reader_new:
            #  headers
            header_prev = reader_prev[0]
            header_new = reader_new[0]
            #print(f"\n Headers: {header_prev}, \n{header_new}")

            if header_prev != header_new:
                #print("CSV headers do not match!")
                logging.error("CSV headers do not match")

        else:
            #print("NO HEADERS")
            logging.error("CSV has no header")

        # data rows
        data_prev = reader_prev[1:]
        data_new = reader_new[1:]

    differences = []

    max_len = max(len(data_prev), len(data_new)) 
    for i in range(max_len):
        prev_row = data_prev[i] if i < len(data_prev) else None
        new_row = data_new[i] if i < len(data_new) else None

        if prev_row != new_row:
            if new_row:
                #print(f"NEW ROW: {new_row}")
                differences.append(new_row)
    
    filepath = f"{dt_name}_differences.csv"
    with open(f"/tmp/{filepath}", "w", newline="") as f_diff:    
        csvwriter = csv.writer(f_diff)
        csvwriter.writerow(header_prev) # header
        for diff in differences:
            cleaned_diff = [diff[0].lstrip()] + list(diff[1:])
            #print(f"diff: {cleaned_diff}")
            csvwriter.writerow(cleaned_diff)
 
    return filepath

