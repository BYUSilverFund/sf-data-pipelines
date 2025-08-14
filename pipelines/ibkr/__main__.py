from ibkr_to_s3_flow import ibkr_to_s3_daily_flow, ibkr_to_s3_backfill_flow
from s3_to_rds_flow import s3_to_rds_daily_flow, s3_to_rds_backfill_flow
from calendar_flow import calendar_daily_flow, calendar_backfill_flow
from benchmark_flow import benchmark_daily_flow, benchmark_backfill_flow
from risk_free_rate_flow import risk_free_rate_daily_flow, risk_free_rate_backfill_flow
from returns_flow import returns_daily_flow, returns_backfill_flow
import datetime as dt
import dateutil.relativedelta as du

def dashboard_daily_flow() -> None:
    ibkr_to_s3_daily_flow()
    s3_to_rds_daily_flow()
    calendar_daily_flow()
    benchmark_daily_flow()
    risk_free_rate_daily_flow()
    returns_daily_flow()

def dashboard_backill_flow(start_date: dt.date | None = None, end_date: dt.date | None = None) -> None:
    min_start_date = dt.date.today() - du.relativedelta(years=1)
    min_start_date = min_start_date.replace(day=1) + du.relativedelta(months=1)

    max_end_date = dt.date.today() - du.relativedelta(days=1)

    start_date = start_date or min_start_date
    end_date = end_date or max_end_date

    ibkr_to_s3_backfill_flow(start_date, end_date)
    s3_to_rds_backfill_flow(start_date, end_date)
    calendar_backfill_flow(start_date, end_date)
    benchmark_backfill_flow(start_date, end_date)
    risk_free_rate_backfill_flow(start_date, end_date)
    returns_backfill_flow(start_date, end_date)

if __name__ == '__main__':
    import os
    import aws
    import dotenv

    dotenv.load_dotenv(override=True)

    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )

    # dashboard_daily_flow()
    # dashboard_backill_flow()

    print(
        db.execute_to_df("SELECT * FROM returns;")
    )
