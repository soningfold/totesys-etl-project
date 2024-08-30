import boto3
import logging
import polars as pl
import datetime as dt
from botocore.exceptions import ClientError
from io import BytesIO
from pg8000.native import Connection
import json


def find_processed_data_bucket():
    """
    This function finds the processed data bucket on AWS S3.

    Contains error handling for if the processed data bucket is missing.

    Returns:
        processed_data_bucket (string): string containing full name of the processed data bucket
    """
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found_processed = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-processed-data-"):
            processed_data_bucket = bucket["Name"]
            found_processed = True
            break

    if not found_processed:
        logging.error("No processed data bucket found")
        return "No processed data bucket found"

    return processed_data_bucket


def get_secret(secret_prefix="totesys-credentials"):
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
        for secret in get_secrets_lists_response['SecretList']:
            if secret['Name'].startswith(secret_prefix):
                secret_name = secret['Name']
                break
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(e)
        raise Exception(f"Can't retrieve secret due to {e}")

    return json.loads(secret_value_response["SecretString"])


def connect_to_db(credentials):
    """
    Uses the secret obtained in the get_secret method to establish a
    connection to the database
    """
    return Connection(
        user=credentials["user"],
        password=credentials["password"],
        host=credentials["host"],
        database=credentials["name"],
        port=credentials["port"],
    )


def populate_fact_sales(time_prefix):
    '''
    '''
    # read fact_sales_order.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/fact_sales_order.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)

    # reorder columns
    new_order = [
        "sales_order_id", "created_date", "created_time",
        "last_updated_date", "last_updated_time", "staff_id",
        "counterparty_id", "units_sold", "unit_price",
        "currency_id", "design_id", "agreed_payment_date",
        "agreed_delivery_date", "agreed_delivery_location_id"
    ]
    df = df.select(new_order)
    # convert the datetime column to string format
    df = df.with_columns([
        pl.col("created_date").cast(pl.String),
        pl.col("created_time").cast(pl.String),
        pl.col("last_updated_date").cast(pl.String),
        pl.col("last_updated_time").cast(pl.String),
        pl.col("agreed_payment_date").cast(pl.String),
        pl.col("agreed_delivery_date").cast(pl.String)
        ])
    
    
    # insert into fact_sales_order table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            query = f'''INSERT INTO "fact_sales_order"
                ("sales_order_id", "created_date",
                "created_time", "last_updated_date", "last_updated_time",
                "sales_staff_id", "counterparty_id", "units_sold",
                "unit_price", "currency_id", "design_id",
                "agreed_payment_date", "agreed_delivery_date",
                "agreed_delivery_location_id"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL table fact_sales_order successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()


def populate_dim_staff(time_prefix):
    '''
    '''
    # read dim_staff.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/dim_staff.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)
    
    # reorder columns
    new_order = [
        "staff_id", "first_name", "last_name", "department_name",
        "location", "email_address"
    ]
    df = df.select(new_order)

    # insert into dim_staff table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            
            row = [value.replace("'", "") if isinstance(value,str) else value for value in row]
            row = tuple(row)
            query = f'''INSERT INTO "dim_staff"
                ("staff_id", "first_name", "last_name", "department_name",
                "location", "email_address"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL table dim_staff successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()


def populate_dim_date(time_prefix):
    '''
    '''
    # read dim_date.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/dim_date.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)
    
    # reorder columns
    new_order = [
        "date_id", "year", "month", "day", "day_of_week",
        "day_name", "month_name", "quarter"
    ]
    df = df.select(new_order)
    
    df = df.with_columns(pl.col("date_id").cast(pl.String))
    
    # insert into dim_date table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        query_select = """SELECT * FROM dim_date"""
        for row in df.iter_rows():
            row_comparison = list(row)
            row_comparison[0] = dt.date(*map(int, row_comparison[0].split('-')))
            if row_comparison not in db.run(query_select):
                query_insert = f'''INSERT INTO "dim_date"
                    ("date_id", "year", "month", "day",
                    "day_of_week", "day_name", "month_name", "quarter"
                    )
                    VALUES
                    {row}
                    '''
                db.run(query_insert)
        return 'SQL table dim_date successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()


def populate_dim_location(time_prefix):
    '''
    '''
    # read dim_location.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/dim_location.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)
    
    
    # reorder columns
    new_order = [
        "location_id", "address_line_1", "address_line_2", "district", "city",
        "postal_code", "country", "phone"
    ]
    df = df.select(new_order)

    # insert into dim_location table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            row = ["" if value is None else value for value in row]
            row = [value.replace("'", "") if isinstance(value,str) else value for value in row]
            row = tuple(row)
            
            query = f'''INSERT INTO "dim_location"
                ("location_id","address_line_1", "address_line_2", "district", "city",
                "postal_code", "country", "phone"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL table dim_location successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()


def populate_dim_currency(time_prefix):
    '''
    '''
    # read dim_currency.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/dim_currency.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)
    
    # reorder columns
    new_order = [
        "currency_id", "currency_code", "currency_name"
    ]
    df = df.select(new_order)

    # insert into dim_currency table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            query = f'''INSERT INTO "dim_currency"
                ("currency_id", "currency_code", "currency_name"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL table dim_currency successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()


def populate_dim_design(time_prefix):
    '''
    '''
    # read dim_design.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/dim_design.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)

    # reorder columns
    new_order = [
        "design_id", "design_name", "file_location", "file_name"
    ]
    df = df.select(new_order)

    # insert into dim_design table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            query = f'''INSERT INTO "dim_design"
                ("design_id","design_name", "file_location", "file_name"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL table dim_design successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()


def populate_dim_counterparty(time_prefix):
    '''
    '''
    # read dim_counterparty.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket,
            Key=f'/history/{time_prefix}/dim_counterparty.parquet'
        )
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)

    # reorder columns
    new_order = [
        "counterparty_id", "counterparty_legal_name", "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2", "counterparty_legal_district",
        "counterparty_legal_city", "counterparty_legal_postal_code",
        "counterparty_legal_country", "counterparty_legal_phone_number"
    ]
    df = df.select(new_order)
    
    ("dim_counterparty")
    
    # insert into dim_counterparty table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            row = ["" if value is None else value for value in row]
            row = [value.replace("'", "") if isinstance(value,str)  else value for value in row]
            row = tuple(row)
            
            query = f'''INSERT INTO "dim_counterparty"
                ("counterparty_id", "counterparty_legal_name", "counterparty_legal_address_line_1",
                "counterparty_legal_address_line_2", "counterparty_legal_district",
                "counterparty_legal_city", "counterparty_legal_postal_code",
                "counterparty_legal_country", "counterparty_legal_phone_number"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL table dim_counterparty successfully populated'
    except ClientError as e:
        (e)
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()
