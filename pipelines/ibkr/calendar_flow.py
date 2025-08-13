from pipelines.ibkr import tools, aws
import datetime as dt
import dateutil.relativedelta as du
import os

def calendar_daily_flow() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Pull calendar data
    df = tools.get_market_calendar(last_market_date, last_market_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/calendar_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_CALENDAR"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/calendar_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')

def calendar_backfill_flow(start_date: dt.date, end_date: dt.date) -> None:
    # 1. Pull calendar data
    df = tools.get_market_calendar(start_date, end_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/calendar_create.sql')

    # 3. Load into stage table
    stage_table = f"{start_date}_{end_date}_CALENDAR"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/calendar_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')