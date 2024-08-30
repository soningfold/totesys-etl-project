import pytest
import boto3
import os
from moto import mock_aws
from src.lambda_functions.transform import lambda_handler as transform


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for S3 bucket."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class DummyContext:  # Dummy context class used for testing
    pass


event = {"time_prefix": "YYYY/MM/DD/HH:MM:SS/"}
prefix = event["time_prefix"]
context = DummyContext()


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mocked S3 client with objects"""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3.put_object(
            Body="""sales_order_id,created_at,last_updated,design_id,staff_id,counterparty_id,units_sold,unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id
2,2022-11-03 14:20:52.186000,2022-11-03 14:20:52.186000,3,19,8,42972,3.94,2,2022-11-07,2022-11-08,8
3,2022-11-03 14:20:52.188000,2022-11-03 14:20:52.188000,4,10,4,65839,2.91,3,2022-11-06,2022-11-07,19
4,2022-11-03 14:20:52.188000,2022-11-03 14:20:52.188000,4,10,16,32069,3.89,2,2022-11-05,2022-11-07,15
5,2022-11-03 14:20:52.186000,2022-11-03 14:20:52.186000,7,18,4,49659,2.41,3,2022-11-05,2022-11-08,25
6,2022-11-04 11:37:10.341000,2022-11-04 11:37:10.341000,3,13,18,83908,3.99,3,2022-11-04,2022-11-07,17
7,2022-11-04 12:57:09.926000,2022-11-04 12:57:09.926000,7,11,10,65453,2.89,2,2022-11-04,2022-11-09,28
8,2022-11-04 13:45:10.306000,2022-11-04 13:45:10.306000,2,11,20,20381,2.22,2,2022-11-06,2022-11-07,8
10,2022-11-07 09:07:10.485000,2022-11-07 09:07:10.485000,3,16,12,61620,3.86,2,2022-11-09,2022-11-10,20
11,2022-11-07 15:53:10.153000,2022-11-07 15:53:10.153000,9,14,12,35227,3.41,2,2022-11-08,2022-11-13,13""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}sales_order_differences.csv",
        )

        s3.put_object(
            Body="""department_id,department_name,location,manager,created_at,last_updated
1,Sales,Manchester,Richard Roma,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
2,Purchasing,Manchester,Naomi Lapaglia,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
3,Production,Leeds,Chester Ming,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
4,Dispatch,Leds,Mark Hanna,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
5,Finance,Manchester,Jordan Belfort,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
6,Facilities,Manchester,Shelley Levene,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
7,Communications,Leeds,Ann Blake,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
8,HR,Leeds,James Link,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}department_differences.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
1,Jeremie,Franey,2,jeremie.franey@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
2,Deron,Beier,6,deron.beier@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
3,Jeanette,Erdman,6,jeanette.erdman@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
4,Ana,Glover,3,ana.glover@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
5,Magdalena,Zieme,8,magdalena.zieme@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
6,Korey,Kreiger,3,korey.kreiger@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}staff_differences.csv",
        )

        s3.put_object(
            Body="""counterparty_id,counterparty_legal_name,legal_address_id,commercial_contact,delivery_contact,created_at,last_updated
1,Fahey and Sons,15,Micheal Toy,Mrs. Lucy Runolfsdottir,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
2,"Leannon, Predovic and Morar",28,Melba Sanford,Jean Hane III,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
3,Armstrong Inc,2,Jane Wiza,Myra Kovacek,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
4,Kohler Inc,29,Taylor Haag,Alfredo Cassin II,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
5,"Frami, Yundt and Macejkovic",22,Homer Mitchell,Ivan Balistreri,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
6,Mraz LLC,23,Florence Casper,Eva Upton,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
7,"Padberg, Lueilwitz and Johnson",16,Ms. Wilma Witting,Christy O'Hara,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
8,Grant - Lakin,16,Emily Orn,Veronica Fay,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
9,Price LLC,5,Sheryl Langworth,Simon Schoen,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000
10,Bosco - Grant,10,Ed Halvorson,Dewey Kuhic,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}counterparty_differences.csv",
        )

        s3.put_object(
            Body="""currency_id,currency_code,created_at,last_updated
1,GBP,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
2,USD,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
3,EUR,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}currency_differences.csv",
        )

        s3.put_object(
            Body="""design_id,created_at,design_name,file_location,file_name,last_updated
8,2022-11-03 14:20:49.962000,Wooden,/usr,wooden-20220717-npgz.json,2022-11-03 14:20:49.962000
51,2023-01-12 18:50:09.935000,Bronze,/private,bronze-20221024-4dds.json,2023-01-12 18:50:09.935000
69,2023-02-07 17:31:10.093000,Bronze,/lost+found,bronze-20230102-r904.json,2023-02-07 17:31:10.093000
16,2022-11-22 15:02:10.226000,Soft,/System,soft-20211001-cjaz.json,2022-11-22 15:02:10.226000
54,2023-01-16 09:14:09.775000,Plastic,/usr/ports,plastic-20221206-bw3l.json,2023-01-16 09:14:09.775000
55,2023-01-19 08:10:10.138000,Concrete,/opt/include,concrete-20210614-04nd.json,2023-01-19 08:10:10.138000
10,2022-11-03 14:20:49.962000,Soft,/usr/share,soft-20220201-hzz1.json,2022-11-03 14:20:49.962000
57,2023-01-19 10:37:09.965000,Cotton,/etc/periodic,cotton-20220527-vn4b.json,2023-01-19 10:37:09.965000
41,2022-12-30 10:06:10.081000,Granite,/usr/X11R6,granite-20220125-ifwa.json,2022-12-30 10:06:10.081000
45,2023-01-04 15:24:10.123000,Frozen,/Users,frozen-20221021-bjqs.json,2023-01-04 15:24:10.123000""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}design_differences.csv",
        )

        s3.put_object(
            Body="""address_id,address_line_1,address_line_2,district,city,postal_code,country,phone,created_at,last_updated
1,6826 Herzog Via,,Avon,New Patienceburgh,28441,Turkey,1803 637401,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
2,179 Alexie Cliffs,,,Aliso Viejo,99305-7380,San Marino,9621 880720,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
3,148 Sincere Fort,,,Lake Charles,89360,Samoa,0730 783349,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
4,6102 Rogahn Skyway,,Bedfordshire,Olsonside,47518,Republic of Korea,1239 706295,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
5,34177 Upton Track,,,Fort Shadburgh,55993-8850,Bosnia and Herzegovina,0081 009772,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
6,846 Kailey Island,,,Kendraburgh,08841,Zimbabwe,0447 798320,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
7,75653 Ernestine Ways,,Buckinghamshire,North Deshaun,02813,Faroe Islands,1373 796260,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
8,0579 Durgan Common,,,Suffolk,56693-0660,United Kingdom,8935 157571,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
9,644 Edward Garden,,Borders,New Tyra,30825-5672,Australia,0768 748652,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000
10,49967 Kaylah Flat,,,Beaulahcester,89470,Democratic People's Republic of Korea,4949 998070,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000""",
            Bucket="totesys-raw-data-000000",
            Key=f"/history/{prefix}address_differences.csv",
        )

        yield s3


class TestTransform:

    @pytest.mark.it("parquet data lands in the processed bucket")
    def test_transform_lands_data_in_processed_data_bucket(self, s3):

        res = transform(event, context)

        assert res == {"time_prefix": "YYYY/MM/DD/HH:MM:SS/"}
