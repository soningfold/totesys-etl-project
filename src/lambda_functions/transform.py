from src.utils.transform_utils import create_star_schema_from_sales_order_csv_file

def lambda_handler(event, context):
    """
    This function finds data buckets, converts the csvs to parquet in a star schema format, then uploads this
    to the processed data bucket.

    Args:
        event (dict): time prefix provided by extract function
        context (dict): AWS provided context

    Returns:
        dict: dictionary with time prefix to be used in the load function
    """

    prefix = event["time_prefix"]

    create_star_schema_from_sales_order_csv_file(prefix)

    return {"time_prefix": prefix}