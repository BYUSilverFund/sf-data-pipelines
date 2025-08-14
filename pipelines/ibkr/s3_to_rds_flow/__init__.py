from .positions_flow import execute_s3_to_rds_positions_daily, execute_s3_to_rds_positions_backfill
from .trades_flow import execute_s3_to_rds_trades_daily, execute_s3_to_rds_trades_backfill
from .dividends_flow import execute_s3_to_rds_dividends_daily, execute_s3_to_rds_dividends_backfill
from .delta_nav_flow import execute_s3_to_rds_delta_nav_daily, execute_s3_to_rds_delta_nav_backfill
import datetime as dt

def s3_to_rds_daily_flow():
    execute_s3_to_rds_positions_daily()
    execute_s3_to_rds_trades_daily()
    execute_s3_to_rds_dividends_daily()
    execute_s3_to_rds_delta_nav_daily()

def s3_to_rds_backfill_flow(start_date: dt.date, end_date: dt.date):
    execute_s3_to_rds_positions_backfill(start_date, end_date)
    execute_s3_to_rds_trades_backfill(start_date, end_date)
    execute_s3_to_rds_dividends_backfill(start_date, end_date)
    execute_s3_to_rds_delta_nav_backfill(start_date, end_date)