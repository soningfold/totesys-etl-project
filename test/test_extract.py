import pytest
import boto3
import os
import json
from moto import mock_aws
from unittest.mock import patch
from src.lambda_functions.extract import lambda_handler
from datetime import datetime as dt
from dotenv import load_dotenv, find_dotenv

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
if env_file != "":
    load_dotenv(env_file)

    # print(f'\n >>>> ENV: {os.getenv("ENV")}')

# env variables
if os.getenv("ENV") == "testing":
    USER_NAME = os.getenv("PG_USER")
    PASSWORD = os.getenv("PG_PASSWORD")
    DB_NAME = os.getenv("PG_DATABASE")
    HOST = os.getenv("PG_HOST")
    PORT = os.getenv("PG_PORT")
elif os.getenv("ENV") == "development":
    USER_NAME = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")

# print(f'\n >>>> USER_NAME: {USER_NAME}')

# const
SOURCE_PATH = "/source/"
SOURCE_FILE_SUFFIX = "_new"
HISTORY_PATH = "/history/"
HISTORY_FILE_SUFFIX = "_differences"
MOCK_BUCKET_NAME = "totesys-raw-data-000000"

"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
BEFORE RUNNING THE TESTS, PLEASE MAKE SURE TO:
1. Run  database/test_db.sql: psql -f database/test_db.sql
2. Make sure that you have testing and development environment configured
    (.env.testing and .env.development)
3. Switch to testing env (export ENV=testing in the CLI)

Switch back to .env.development to use the online database.
"""


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
    """Mocked S3 client with raw data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=MOCK_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture(scope="function")
def s3_no_buckets(aws_credentials):
    """Mocked S3 client with no buckets."""
    with mock_aws():
        s3_nobuckets = boto3.client("s3")
        yield s3_nobuckets


@pytest.fixture(scope="function")
def s3_wfile_in_source(aws_credentials):
    """Mocked S3 client with a *_new.csv file in /source."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=MOCK_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
                1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
                2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00""",
            Bucket=MOCK_BUCKET_NAME,
            Key=f"{SOURCE_PATH}staff{SOURCE_FILE_SUFFIX}.csv",
        )

        yield s3


@pytest.fixture(scope="function")
def secretsmanager(aws_credentials):
    with mock_aws():
        database_dict = {
            "user": USER_NAME,
            "password": PASSWORD,
            "host": HOST,
            "database": DB_NAME,
            "port": PORT,
        }
        secretsmanager = boto3.client("secretsmanager")
        secretsmanager.create_secret(
            Name="totesys-credentials-0000", SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


@pytest.fixture(scope="function")
def secretsmanager_broken(aws_credentials):
    with mock_aws():
        database_dict = {
            "user": USER_NAME,
            "password": PASSWORD,
            "host": HOST,
            "database": "steve",
            "port": PORT,
        }
        secretsmanager = boto3.client("secretsmanager")
        secretsmanager.create_secret(
            Name="totesys_database_credentials-0000",
            SecretString=json.dumps(database_dict),
        )
        yield secretsmanager


class DummyContext:  # Dummy context class used for testing
    pass


class TestLambdaHandler:

    # @pytest.mark.skip()
    @pytest.mark.it("Raise exception if raw data bucket is not found")
    def test_bucket_does_not_exist(self, s3_no_buckets, secretsmanager):
        event = {}
        context = DummyContext()
        with pytest.raises(Exception):
            lambda_handler(event, context)

    # @pytest.mark.skip()
    @pytest.mark.it("script succesfully connects to database")
    def test_succesfully_connects_to_database(self, s3, secretsmanager):
        event = {}
        context = DummyContext()
        assert not isinstance(lambda_handler(event, context), Exception)

    # @pytest.mark.skip()
    @pytest.mark.it(
        "returns exception when failing to connect to database due to wrong credentials"
    )
    def test_fails_to_connect_to_database(self, s3, secretsmanager_broken):
        event = {}
        context = DummyContext()
        with pytest.raises(Exception):
            lambda_handler(event, context)

    # @pytest.mark.skip()
    @pytest.mark.it(
        "Successfully uploads csv files to /source and /history/timepath when /source is empty"
    )
    @patch("src.utils.extract_utils.dt")
    def test_uploads_csv_to_source_and_history_first_call(
        self, patched_dt, s3, secretsmanager
    ):
        patched_dt.now.return_value = dt(2014, 3, 10)
        patched_dt.side_effect = lambda *args, **kw: dt(*args, **kw)

        path_history = f"{HISTORY_PATH}2014/03/10/00:00:00/"
        event = {}
        context = DummyContext()
        lambda_handler(event, context)

        expected_files_in_source = {
            f"{SOURCE_PATH}sales_order{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}design{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}currency{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}staff{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}counterparty{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}address{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}department{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}purchase_order{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}payment_type{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}payment{SOURCE_FILE_SUFFIX}.csv": 0,
            f"{SOURCE_PATH}transaction{SOURCE_FILE_SUFFIX}.csv": 0,
        }
        expected_files_in_history = {
            f"{path_history}sales_order{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}design{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}currency{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}staff{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}counterparty{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}address{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}department{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}purchase_order{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}payment_type{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}payment{HISTORY_FILE_SUFFIX}.csv": 0,
            f"{path_history}transaction{HISTORY_FILE_SUFFIX}.csv": 0,
        }

        listing = s3.list_objects_v2(Bucket=MOCK_BUCKET_NAME)

        assert len(listing["Contents"]) == 11 * 2

        for i in range(len(listing["Contents"])):
            assert (
                f"{listing['Contents'][i]['Key']}" in expected_files_in_source
                or f"{listing['Contents'][i]['Key']}" in expected_files_in_history
            )

    # @pytest.mark.skip()
    @pytest.mark.it(
        """Overwrite csv file to in /source after files
        have been compared and differences are stored in /history"""
    )
    def test_overwrites_new_csv_in_source_dir(self, s3_wfile_in_source, secretsmanager):
        prev_file = s3_wfile_in_source.get_object(
            Bucket=MOCK_BUCKET_NAME, Key=f"/source/staff{SOURCE_FILE_SUFFIX}.csv"
        )

        event = {}
        context = DummyContext()
        lambda_handler(event, context)

        new_file = s3_wfile_in_source.get_object(
            Bucket=MOCK_BUCKET_NAME, Key="/source/staff_new.csv"
        )
        assert new_file["ContentLength"] > prev_file["ContentLength"]

    @pytest.mark.it("Mase sure that files in /tmp folder have been deleted")
    def test_deletes_files_in_tmp_folder(self, s3_wfile_in_source, secretsmanager):
        event = {}
        context = DummyContext()
        lambda_handler(event, context)

        tmp_content = [filename for filename in os.listdir("/tmp")]
        assert "staff.csv" not in tmp_content
        assert f"staff{SOURCE_FILE_SUFFIX}.csv" not in tmp_content
