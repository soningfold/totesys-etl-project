from src.utils.load_utils import (
    populate_fact_sales,
    populate_dim_counterparty,
    populate_dim_currency,
    populate_dim_date,
    populate_dim_design,
    populate_dim_location,
    populate_dim_staff
)
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    time_prefix = event['time_prefix']
    try:
        populate_dim_location(time_prefix)
        populate_dim_staff(time_prefix)
        populate_dim_counterparty(time_prefix)
        populate_dim_currency(time_prefix)
        populate_dim_date(time_prefix)
        populate_dim_design(time_prefix)
        populate_fact_sales(time_prefix)

        logging.info('All tables successfully loaded')
    except Exception as e:
        print(e)
        logging.error('Issue with loading data into warehouse')
