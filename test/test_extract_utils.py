import pytest
import boto3
import os
import json
import csv
from moto import mock_aws
from unittest.mock import patch
from datetime import datetime as dt
from pg8000.native import Connection
from src.utils.extract_utils import create_time_based_path, get_secret, connect_to_bucket, connect_to_db, query_db, create_and_upload_csv, compare_csvs
from dotenv import load_dotenv, find_dotenv

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
if env_file != "":
    load_dotenv(env_file)
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

    print(f"\n >>>> USER_NAME: {USER_NAME}")


SOURCE_PATH = "/source/"
SOURCE_FILE_SUFFIX = "_new"
HISTORY_PATH = "/history/"
HISTORY_FILE_SUFFIX = "_differences"

MOCK_BUCKET_NAME = "totesys-raw-data-000000"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for S3 bucket."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_empty_bucket(aws_credentials):
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
def s3_extra_rows(aws_credentials):
    """Mocked S3 client with raw data bucket containing
    two csv files, the _new version having the same content of
    the other csv file, plus extra two rows."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=MOCK_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        """,
            Bucket=MOCK_BUCKET_NAME,
            Key="test_csv_extra_rows.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@nc.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket=MOCK_BUCKET_NAME,
            Key="test_csv_extra_rows_new.csv",
        )

        yield s3


@pytest.fixture(scope="function")
def s3_rows_edited(aws_credentials):
    """Mocked S3 client with raw data bucket containing
    two csv files sharing the same content apart from two
    rows whose content has been modified"""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=MOCK_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@nc.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket=MOCK_BUCKET_NAME,
            Key="test_csv_edited_rows.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@this_has_been_edited.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@kastriot.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket=MOCK_BUCKET_NAME,
            Key="test_csv_edited_rows_new.csv",
        )

        yield s3


@pytest.fixture(scope="function")
def s3_no_dt_changes(aws_credentials):
    """Mocked S3 client with raw data bucket containing
    two csv files sharing the very same content"""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=MOCK_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        """,
            Bucket=MOCK_BUCKET_NAME,
            Key="test_csv_same_content.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        """,
            Bucket=MOCK_BUCKET_NAME,
            Key="test_csv_same_content_new.csv",
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
            Name="totesys-credentials-0000", SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


class TestTimePath:

    @patch("src.utils.extract_utils.dt")
    @pytest.mark.it("Create time prefix function returns formatted datetime")
    def test_correct_time_is_returned(self, patched_dt):
        patched_dt.now.return_value = dt(2024, 1, 1)
        patched_dt.side_effect = lambda *args, **kw: dt(*args, **kw)
        result = create_time_based_path()
        patched_dt.now.assert_called_once()
        assert result == "2024/01/01/00:00:00/"


class TestGetSecret:

    @pytest.mark.it("get secret returns the correct credentials data")
    def test_get_secret_returns_correct_credentials(self, secretsmanager):
        assert get_secret()["user"] == USER_NAME
        assert get_secret()["password"] == PASSWORD
        assert get_secret()["host"] == HOST

    @pytest.mark.it(
        "get secret raises an error if secret_name is not in secretsmanager"
    )
    def test_get_secret_failed(self, secretsmanager):
        with pytest.raises(Exception):
            get_secret("imposter_steve")


class TestConnectToBucket:

    @pytest.mark.it("Connect to bucket function returns bucket name")
    def test_connect_to_bucket_returns_correct_reponse(self, s3_empty_bucket):
        result = connect_to_bucket(s3_empty_bucket)
        assert result == MOCK_BUCKET_NAME

    @pytest.mark.it(
        "Connect to bucket function returns exception when no bucket is found"
    )
    def test_connect_to_bucket_returns_exception(self, s3_no_buckets):
        with pytest.raises(Exception):
            connect_to_bucket(s3_no_buckets)


class TestConnectToDB:

    @pytest.mark.it("Connect to DB returns credentials")
    def test_connect_to_db_returns_valid_response(self, secretsmanager):
        inp = get_secret()
        result = connect_to_db(inp)
        assert type(result) is Connection


class TestQueryDB:

    @pytest.mark.it("Return valid csv formatted data (header + table rows)")
    def test_db_query_returns_valid_csv_format_data(self, secretsmanager):
        dt = "currency"
        conn = connect_to_db(get_secret())

        result = query_db(dt, conn)

        assert len(result) >= 1


class TestCreateAndUploadCsv:

    @pytest.mark.it(
        "Succesfully saves csv file to both history and source if csv file with same name is not found inside source dir"
    )
    @patch("src.utils.extract_utils.dt")
    def test_function_saves_csv_in_both_history_and_source_first_call(
        self, patched_dt, s3_empty_bucket
    ):
        patched_dt.now.return_value = dt(2024, 1, 1)
        patched_dt.side_effect = lambda *args, **kw: dt(*args, **kw)
        time_path = create_time_based_path()
        bucket_name = MOCK_BUCKET_NAME
        file_name = "test_file"
        data = [["A", "B", "C"], [1, 2, 3], [4, 5, 6]]

        create_and_upload_csv(
            data, s3_empty_bucket, bucket_name, file_name, time_path, True
        )

        list_objects = s3_empty_bucket.list_objects_v2(Bucket=MOCK_BUCKET_NAME)[
            "Contents"
        ]

        assert (
            list_objects[1]["Key"]
            == f"{SOURCE_PATH}{file_name}{SOURCE_FILE_SUFFIX}.csv"
        )
        assert (
            list_objects[0]["Key"]
            == f"{HISTORY_PATH}2024/01/01/00:00:00/{file_name}{HISTORY_FILE_SUFFIX}.csv"
        )

    @pytest.mark.it(
        "Saves csv file to /tmp if csv file with same name already exists in /source"
    )
    def test_function_saves_csv_in_temp_after_first_call(self, s3_empty_bucket):
        bucket_name = MOCK_BUCKET_NAME
        dt_name = "test_dt"
        data = [["A", "B", "C"], [1, 2, 3], [4, 5, 6]]
        time_path = create_time_based_path()
        create_and_upload_csv(
            data, s3_empty_bucket, bucket_name, dt_name, time_path, False
        )

        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith(f"test_dt{SOURCE_FILE_SUFFIX}")
        ]
        assert len(csv_path_list) > 0


class TestCompareCsvs:
    # @pytest.mark.skip()
    @pytest.mark.it("""Creates a csv file after comparing two csv files in /tmp/""")
    def test_file_exists(self, s3_extra_rows, secretsmanager):
        s3_extra_rows.download_file(
            MOCK_BUCKET_NAME,
            "test_csv_extra_rows.csv",
            "/tmp/test_csv_extra_rows.csv",
        )
        s3_extra_rows.download_file(
            MOCK_BUCKET_NAME,
            "test_csv_extra_rows_new.csv",
            "/tmp/test_csv_extra_rows_new.csv",
        )

        compare_csvs("test_csv_extra_rows")

        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_extra_rows_differences")
        ]
        assert len(csv_path_list) > 0

    # @pytest.mark.skip()
    @pytest.mark.it(
        """Creates a csv file containing changes between the two csvs
        (most recent dt has extra rows)"""
    )
    def test_change_in_datatable_extra_rows(self, s3_extra_rows, secretsmanager):
        s3_extra_rows.download_file(
            MOCK_BUCKET_NAME,
            "test_csv_extra_rows.csv",
            "/tmp/test_csv_extra_rows.csv",
        )
        s3_extra_rows.download_file(
            MOCK_BUCKET_NAME,
            "test_csv_extra_rows_new.csv",
            "/tmp/test_csv_extra_rows_new.csv",
        )

        compare_csvs("test_csv_extra_rows")

        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_extra_rows_differences")
        ]

        with open(f"/tmp/{csv_path_list[0]}", "r") as reader:
            next(reader)
            differences = csv.reader(reader)
            assert list(differences) == [
                [
                    "10",
                    "Steve",
                    "Imposter",
                    "1",
                    "steve_imposter@nc.com",
                    "2023-08-12 10:30:00",
                    "2023-08-12 10:30:00",
                ],
                [
                    "11",
                    "Stevie",
                    "Impostah",
                    "1",
                    "stevie_impostah@nc.com",
                    "2024-08-12 10:30:00",
                    "2024-08-12 10:30:00",
                ],
            ]

    # @pytest.mark.skip()
    @pytest.mark.it(
        """Writes empty csv file when both csvs share the very same content"""
    )
    def test_no_change_in_database(self, s3_no_dt_changes, secretsmanager):
        file_name = "test_csv_same_content"
        s3_no_dt_changes.download_file(
            MOCK_BUCKET_NAME,
            f"{file_name}.csv",
            f"/tmp/{file_name}.csv",
        )
        s3_no_dt_changes.download_file(
            MOCK_BUCKET_NAME,
            f"{file_name}{SOURCE_FILE_SUFFIX}.csv",
            f"/tmp/{file_name}{SOURCE_FILE_SUFFIX}.csv",
        )

        compare_csvs("test_csv_same_content")

        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_same_content_differences")
        ]
        with open(f"/tmp/{csv_path_list[0]}", "r") as reader:
            next(reader)
            differences = csv.reader(reader)
            assert list(differences) == []

    # @pytest.mark.skip()
    @pytest.mark.it(
        """Creates a csv file containing changes between
        the two csvs (new dt has edited rows)"""
    )
    def test_change_in_datatable_edited_rows(self, s3_rows_edited, secretsmanager):
        s3_rows_edited.download_file(
            MOCK_BUCKET_NAME,
            "test_csv_edited_rows.csv",
            "/tmp/test_csv_edited_rows.csv",
        )
        s3_rows_edited.download_file(
            MOCK_BUCKET_NAME,
            "test_csv_edited_rows_new.csv",
            "/tmp/test_csv_edited_rows_new.csv",
        )

        compare_csvs("test_csv_edited_rows")

        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_edited_rows_differences")
        ]

        with open(f"/tmp/{csv_path_list[0]}", "r") as reader:
            next(reader)
            differences = csv.reader(reader)

            assert list(differences) == [
                [
                    "1",
                    "John",
                    "Doe",
                    "1",
                    "john.doe@this_has_been_edited.com",
                    "2023-08-10 08:00:00",
                    "2023-08-10 08:00:00",
                ],
                [
                    "11",
                    "Stevie",
                    "Impostah",
                    "1",
                    "stevie_impostah@kastriot.com",
                    "2024-08-12 10:30:00",
                    "2024-08-12 10:30:00",
                ],
            ]
