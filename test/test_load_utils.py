import pytest
import boto3
import os
from moto import mock_aws
from src.utils.load_utils import (
    find_processed_data_bucket,
    get_secret,
    connect_to_db,
    populate_fact_sales,
    populate_dim_counterparty,
    populate_dim_currency,
    populate_dim_date,
    populate_dim_design,
    populate_dim_location,
    populate_dim_staff
)
from pg8000.native import Connection
from dotenv import load_dotenv, find_dotenv
import json
import polars as pl
from io import BytesIO
from unittest.mock import patch
import numpy as np
from datetime import datetime, timedelta

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
load_dotenv(env_file)

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_DATAWAREHOUSE = os.getenv("PG_DATAWAREHOUSE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
MOCK_TIME_PATH = "2024/01/01/00:00:00"

data_tables = [
    'purchase_order',
    'payment',
    'payment_type',
    'department',
    'sales_order',
    'transaction',
    'address',
    'currency',
    'counterparty',
    'design',
    'staff'
]


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for S3 bucket."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture(scope="function")
def s3_with_parquet(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        path_prefix = f'/history/{MOCK_TIME_PATH}'
        # creating mock fact_sales_order.parquet
        data = {
            "sales_order_id": range(1, 6),  # Mock primary key
            "created_date": [(datetime(2023, 12, 1) - timedelta(days=i)).date() for i in range(5)],  # Mock timestamps
            "created_time": [(datetime(2023, 12, 1) - timedelta(hours=i)).time() for i in range(5)],  # Mock timestamps
            "last_updated_date": [(datetime(2024, 1, 1) - timedelta(days=i)).date() for i in range(5)],
            "last_updated_time":  [(datetime(2024, 1, 1) - timedelta(hours=i)).time() for i in range(5)],  # Mock timestamps
            "design_id": np.random.randint(1, 10, 5),  # Random design IDs
            "staff_id": np.random.randint(1, 10, 5),  # Random staff IDs
            "counterparty_id": [10 - i for i in range(5)],  # NOT Random counterparty IDs
            "units_sold": [1000, 2000, 3000, 4000, 5000],  # Random units sold
            "unit_price": np.random.uniform(2.0, 4.0, 5).round(2),  # Random unit price
            "currency_id": np.random.choice([1, 2, 3], 5),  # Random currency IDs (e.g., 1=USD, 2=EUR, 3=GBP)
            "agreed_delivery_date": [(datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(5)],  # Future dates
            "agreed_payment_date": [(datetime.now() + timedelta(days=i+10)).strftime('%Y-%m-%d') for i in range(5)],  # Future dates
            "agreed_delivery_location_id": np.random.randint(1, 10, 5),  # Random delivery location IDs
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=path_prefix+"/fact_sales_order.parquet"
        )

        # creating mock dim_staff.parquet
        data = {
            "staff_id": range(1, 6),  # Mock primary key
            "first_name": ["firstName_"+str(i) for i in range(5)],
            "last_name": ["lastName_"+str(i) for i in range(5)],
            "department_name": ["department_"+str(i) for i in range(5)],
            "location": ["location_"+str(i) for i in range(5)],
            "email_address": [f'address_{i}@provide{i}.com' for i in range(5)]
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=path_prefix+"/dim_staff.parquet"
        )

        # creating mock dim_location.parquet
        data = {
            "location_id": range(1, 6),  # Mock primary key
            "address_line_1": ["address_line_1_"+str(i) for i in range(5)],
            "address_line_2": ["address_line_2_"+str(i) for i in range(5)],
            "district": ["district_"+str(i) for i in range(5)],
            "city": ["city_"+str(i) for i in range(5)],
            "postal_code": ["1234_" + str(i) for i in range(5)],
            "country": ["country_"+str(i) for i in range(5)],
            "phone": ["0123456789_"+str(i) for i in range(5)]
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=path_prefix+"/dim_location.parquet"
        )
        # creating mock dim_design.parquet
        data = {
            "design_id": range(1, 6),  # Mock primary key
            "design_name": ["design_name_"+str(i) for i in range(5)],
            "file_location": ["file_location_"+str(i) for i in range(5)],
            "file_name": ["file_name_"+str(i) for i in range(5)],
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        key = path_prefix+"/dim_design.parquet"
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=key
        )

        # creating mock dim_counterparty.parquet
        data = {
            "counterparty_id": range(1, 6),  # Mock primary key
            "counterparty_legal_name": ["name_"+str(i) for i in range(5)],
            "counterparty_legal_address_line_1": ["address_02_"+str(i) for i in range(5)],
            "counterparty_legal_address_line_2": ["address_01_"+str(i) for i in range(5)],
            "counterparty_legal_district": ["district_"+str(i) for i in range(5)],
            "counterparty_legal_city": ["city_"+str(i) for i in range(5)],
            "counterparty_legal_postal_code": ["postal_code_"+str(i) for i in range(5)],
            "counterparty_legal_country": ["country_"+str(i) for i in range(5)],
            "counterparty_legal_phone_number": ["0123456789"+str(i) for i in range(5)],
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=path_prefix+"/dim_counterparty.parquet"
        )
        # creating mock dim_date.parquet
        data = {
            "date_id": [(datetime.now() + timedelta(days=i+10)).strftime('%Y-%m-%d') for i in range(5)],  # Mock primary key
            "year": [str(i) for i in range(5)],
            "month": [str(i) for i in range(5)],
            "day": [str(i) for i in range(5)],
            "day_of_week": [str(i) for i in range(5)],
            "day_name": ["day_name_"+str(i) for i in range(5)],
            "month_name": ["month_name_"+str(i) for i in range(5)],
            "quarter": [str(i) for i in range(5)],
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        key = path_prefix+"/dim_date.parquet"
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=key
        )

        # creating mock dim_currency.parquet
        data = {
            "currency_id": range(1, 6),  # Mock primary key
            "currency_code": ["currency_code_"+str(i) for i in range(5)],
            "currency_name": ["currency_name_"+str(i) for i in range(5)],
        }
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        key = path_prefix+"/dim_currency.parquet"
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key=key
        )
        yield s3


@pytest.fixture(scope="function")
def no_s3(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        yield s3


@pytest.fixture(scope="function")
def secretsmanager(aws_credentials):
    with mock_aws():
        database_dict = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "name": PG_DATAWAREHOUSE,
            "port": PG_PORT,
        }
        secretsmanager = boto3.client("secretsmanager")
        secretsmanager.create_secret(
            Name="totesys_data_warehouse_credentials", SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


@pytest.fixture(scope="function")
def db():
    '''
    Connects to test SQL database
    '''
    conn = Connection(
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        database=PG_DATAWAREHOUSE,
        port=PG_PORT
    )
    return conn


@pytest.fixture(scope="function")
def run_seed(db):
    '''Runs seed before starting tests, yields, runs tests,
       then closes connection to db'''
    db.run("TRUNCATE fact_sales_order RESTART IDENTITY CASCADE;")
    db.run("TRUNCATE dim_staff RESTART IDENTITY CASCADE;")
    db.run("TRUNCATE dim_date RESTART IDENTITY CASCADE;")
    db.run("TRUNCATE dim_location RESTART IDENTITY CASCADE;")
    db.run("TRUNCATE dim_currency RESTART IDENTITY CASCADE")
    db.run("TRUNCATE dim_design RESTART IDENTITY CASCADE;")
    db.run("TRUNCATE dim_counterparty RESTART IDENTITY CASCADE;")
    yield
    db.close()


class TestFindBucket:

    def test_find_processed_data_bucket_finds_bucket(self, s3):
        bucket_name = find_processed_data_bucket()
        assert bucket_name == "totesys-processed-data-000000"

    def test_find_processed_data_bucket_raises_exception(self, no_s3):
        assert find_processed_data_bucket() == "No processed data bucket found"


class TestGetSecret:

    def test_get_secret_returns_exception(self, secretsmanager):
        secret_name = "nonexistent_secret"
        with pytest.raises(Exception):
            get_secret(secret_name)

    def test_get_secret_runs_correctly(self, secretsmanager):
        secret_name = "totesys_data_warehouse_credentials"
        credentials = get_secret(secret_name)
        expected = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "name": PG_DATAWAREHOUSE,
            "port": PG_PORT,
        }
        assert credentials == expected


class TestConnectToDB:

    def test_connect_to_db(self, secretsmanager):
        secret_name = "totesys_data_warehouse_credentials"
        credentials = get_secret(secret_name)
        assert not isinstance(connect_to_db(credentials), Exception)


class TestTransformFactTable:

    @patch('src.utils.load_utils.get_secret')
    def test_fact_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "sales_record_id", "sales_order_id", "created_date",
            "created_time", "last_updated_date", "last_updated_time",
            "sales_staff_id", "counterparty_id", "units_sold",
            "unit_price", "currency_id", "design_id",
            "agreed_payment_date", "agreed_delivery_date",
            "agreed_delivery_location_id"
            ]

        ret = populate_fact_sales(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'fact_sales_order';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        assert result_unlisted == expected

        query = "SELECT units_sold FROM fact_sales_order;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == [1000, 2000, 3000, 4000, 5000]

        assert ret == 'SQL table fact_sales_order successfully populated'


class TestTransformDimStaff:

    @patch('src.utils.load_utils.get_secret')
    def test_dim_staff_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "staff_id", "first_name", "last_name",
            "department_name", "location", "email_address"
            ]

        ret = populate_dim_staff(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_staff';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        assert result_unlisted == expected

        query = "SELECT first_name FROM dim_staff;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == ["firstName_"+str(i) for i in range(5)]

        assert ret == 'SQL table dim_staff successfully populated'


class TestTransformDimDate:

    @patch('src.utils.load_utils.get_secret')
    def test_dim_date_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "date_id", "year", "month", "day", "day_of_week",
            "day_name", "month_name", "quarter"
            ]

        ret = populate_dim_date(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_date';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        for column in result_unlisted:
            assert column in expected

        query = "SELECT day FROM dim_date;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == [i for i in range(5)]

        assert ret == 'SQL table dim_date successfully populated'


class TestTransformDimLocation:

    @patch('src.utils.load_utils.get_secret')
    def test_dim_location_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "location_id", "address_line_1", "address_line_2", "district", "city",
            "postal_code", "country", "phone"
        ]

        ret = populate_dim_location(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_location';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        assert result_unlisted == expected

        query = "SELECT city FROM dim_location;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == ["city_"+str(i) for i in range(5)]

        assert ret == 'SQL table dim_location successfully populated'


class TestTransformDimDesign:

    @patch('src.utils.load_utils.get_secret')
    def test_dim_design_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "design_id", "design_name", "file_location", "file_name"
        ]

        ret = populate_dim_design(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_design';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        assert result_unlisted == expected

        query = "SELECT design_name FROM dim_design;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == ["design_name_"+str(i) for i in range(5)]

        assert ret == 'SQL table dim_design successfully populated'


class TestTransformDimCurrency:

    @patch('src.utils.load_utils.get_secret')
    def test_dim_currency_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "currency_id", "currency_code", "currency_name"
        ]

        ret = populate_dim_currency(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_currency';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        assert result_unlisted == expected

        query = "SELECT currency_code FROM dim_currency;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == ["currency_code_"+str(i) for i in range(5)]

        assert ret == 'SQL table dim_currency successfully populated'


class TestTransformDimCounterparty:

    @patch('src.utils.load_utils.get_secret')
    def test_dim_counterparty_table_is_created(self, mock_get_secret, run_seed, db, s3_with_parquet):
        mock_get_secret.return_value = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'name': PG_DATAWAREHOUSE,
            'port': PG_PORT
        }

        expected_columns = [
            "counterparty_id", "counterparty_legal_name",
            "counterparty_legal_address_line_1", "counterparty_legal_address_line_2",
            "counterparty_legal_district", "counterparty_legal_city",
            "counterparty_legal_postal_code", "counterparty_legal_country",
            "counterparty_legal_phone_number"
        ]

        ret = populate_dim_counterparty(MOCK_TIME_PATH)

        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_counterparty';"
        expected = expected_columns
        result = db.run(query)
        result_unlisted = [column[0] for column in result]
        assert result_unlisted == expected

        query = "SELECT counterparty_legal_city FROM dim_counterparty;"
        result = db.run(query)
        result = [row[0] for row in result]
        assert result == ["city_"+str(i) for i in range(5)]

        assert ret == 'SQL table dim_counterparty successfully populated'
