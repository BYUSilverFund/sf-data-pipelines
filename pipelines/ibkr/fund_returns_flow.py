from pipelines.ibkr import tools, aws
import datetime as dt
import dateutil.relativedelta as du
import os

def fund_returns_daily_flow() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday) 
    
    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/fund_returns_create.sql')

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name='pipelines/ibkr/sql/fund_returns_materialize.sql',
        params={'start_date': last_market_date, 'end_date': last_market_date}
    )

def fund_returns_backfill_flow(start_date: dt.date, end_date: dt.date) -> None:
    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/fund_returns_create.sql')

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name='pipelines/ibkr/sql/fund_returns_materialize.sql',
        params={'start_date': start_date, 'end_date': end_date}
    )