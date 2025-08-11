import tools
import datetime as dt
import dateutil.relativedelta as du
import aws
import os
import dotenv

dotenv.load_dotenv(override=True)

config = {
    "fund": 'grad',
    "token": 547414482431868624428004,
    "nav": 993010,
    "delta_nav": 993013,
    "positions": 993015,
    "dividends": 993011,
    "trades": 993012,
}


def ibkr_daily_flow(config: dict):
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    for query in ['delta_nav', 'dividends', 'nav', 'positions', 'trades']:
        # 1. Pull data from IBKR
        df = tools.ibkr_query(
            token=config['token'],
            query_id=config[query],
            from_date=last_market_date,
            to_date=last_market_date
        )

        # 2. Save to S3
        s3 = aws.S3(
            aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
            region_name=os.getenv('COGNITO_REGION'),
        )

        file_name = f"{last_market_date}/{query}/{last_market_date}-{query}-{config['fund']}.csv"
        s3.drop_file(file_name=file_name, bucket_name='ibkr-flex-query-files', file_data=df)
        
def ibkr_backfill_flow():
    pass

def ibkr_reload_flow():
    pass


if __name__ == '__main__':
    ibkr_daily_flow(config)
