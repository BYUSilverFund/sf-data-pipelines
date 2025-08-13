import tools
import datetime as dt
import dateutil.relativedelta as du
import aws
import os
import dotenv
import config
import tqdm
import polars as pl
import fsspec

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

def execute_s3_to_rds_positions():
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Process raw positions data
    source_pattern = f"s3://ibkr-flex-query-files/daily-files/{last_market_date}/*/*-positions.csv"

    storage_options = {
        "key": os.getenv('COGNITO_ACCESS_KEY_ID'),
        "secret": os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    }

    schema_overrides = {
        'ReportDate': pl.String
    }

    positions_column_mapping = {
        'ReportDate': 'report_date',
        'ClientAccountID': 'client_account_id',
        'AssetClass': 'asset_class',
        'SubCategory': 'sub_category',
        'Description': 'description',
        'CUSIP': 'cusip',
        'ISIN': 'isin',
        'Symbol': 'symbol',
        'MarkPrice': 'mark_price',
        'Quantity': 'quantity',
        'FXRateToBase': 'fx_rate_to_base'
    }

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = (
            pl.read_csv(f"s3://{file}", storage_options=storage_options, schema_overrides=schema_overrides)
            .filter(pl.col('ClientAccountID').ne('ClientAccountID'))
            .select(positions_column_mapping.keys())
            .rename(positions_column_mapping)
            .with_columns(
                pl.col('cusip').cast(pl.String),
                pl.col('report_date').str.strptime(pl.Date, "%Y%m%d"),
                pl.col('mark_price', 'fx_rate_to_base').cast(pl.Float64),
                pl.col('quantity').cast(pl.Int32)
            )
            .with_columns(
                pl.col('mark_price').mul('quantity').alias('value')
            )
        )
        dfs.append(df)

    df = pl.concat(dfs)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/positions_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_POSITIONS"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/positions_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f"DROP TABLE {stage_table};")

def s3_to_rds_daily_flow():
    execute_s3_to_rds_positions()

def s3_to_rds_reload_flow():
    pass

# if __name__ == '__main__':
#     s3_to_rds_daily_flow()

#     db = aws.RDS(
#         db_endpoint=os.getenv("DB_ENDPOINT"),
#         db_name=os.getenv("DB_NAME"),
#         db_user=os.getenv("DB_USER"),
#         db_password=os.getenv("DB_PASSWORD"),
#         db_port=os.getenv("DB_PORT"),
#     )

#     print(
#         # db.execute('SELECT * FROM "2025-08-11_POSITIONS";')
#         db.execute("SELECT COUNT(*) FROM positions_new;")
#         # db.execute("DROP TABLE positions_new;")
#     )

