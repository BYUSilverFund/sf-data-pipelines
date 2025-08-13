import tools
import datetime as dt
import dateutil.relativedelta as du
import aws
import os
import polars as pl
import fsspec
import dotenv

dotenv.load_dotenv(override=True)

def clean_positions_data(df: pl.DataFrame) -> pl.DataFrame:
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

    return (
        df
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

def execute_s3_to_rds_positions_daily():
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

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = pl.read_csv(f"s3://{file}", storage_options=storage_options, schema_overrides=schema_overrides)
        df_clean = clean_positions_data(df)
        dfs.append(df_clean)

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
    db.execute(f'DROP TABLE "{stage_table}";')