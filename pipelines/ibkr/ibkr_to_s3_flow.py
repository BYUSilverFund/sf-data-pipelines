import tools
import datetime as dt
import dateutil.relativedelta as du
import aws
import os
import dotenv
import config
import tqdm

dotenv.load_dotenv(override=True)

def execute_ibkr_to_s3_daily_flow(config: dict) -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)
    
    for query in tqdm.tqdm(
        iterable=['delta_nav', 'dividends', 'nav', 'positions', 'trades'],
        desc=f"Processing {config['fund']}",
    ):
        # 1. Pull data from IBKR
        df = tools.ibkr_query(
            token=config['token'],
            query_id=config['queries'][query],
            from_date=last_market_date,
            to_date=last_market_date
        )

        # 2. Save to S3
        s3 = aws.S3(
            aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
            region_name=os.getenv('COGNITO_REGION'),
        )

        file_name = f"daily-files/{last_market_date}/{config['fund']}/{last_market_date}-{config['fund']}-{query}.csv"
        s3.drop_file(file_name=file_name, bucket_name='ibkr-flex-query-files', file_data=df)
        
def execute_ibkr_to_s3_backfill_flow(config: dict, start_date: dt.date, end_date: dt.date) -> None:
    for query in tqdm.tqdm(
        iterable=['delta_nav', 'dividends', 'nav', 'positions', 'trades'],
        desc=f"Processing {config['fund']}",
    ):
        # 1. Pull data from IBKR
        df = tools.ibkr_query_batches(
            token=config['token'],
            query_id=config['queries'][query],
            start_date=start_date,
            end_date=end_date
        )

        # 2. Save to S3
        s3 = aws.S3(
            aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
            region_name=os.getenv('COGNITO_REGION'),
        )

        file_name = f"backfill-files/{start_date}_{end_date}/{config['fund']}/{start_date}_{end_date}-{config['fund']}-{query}.csv"
        s3.drop_file(file_name=file_name, bucket_name='ibkr-flex-query-files', file_data=df)

def ibkr_to_s3_daily_flow() -> None:
    for fund_config in config.configs:
        execute_ibkr_to_s3_daily_flow(fund_config)

def ibkr_to_s3_backfill_flow(start_date: dt.date | None = None, end_date: dt.date | None = None):
    min_start_date = dt.date.today() - du.relativedelta(years=1)
    min_start_date = min_start_date.replace(day=1) + du.relativedelta(months=1)

    max_end_date = dt.date.today() - du.relativedelta(days=1)

    start_date = start_date or min_start_date
    end_date = end_date or max_end_date

    for fund_config in config.configs:
        execute_ibkr_to_s3_backfill_flow(fund_config, start_date, end_date)