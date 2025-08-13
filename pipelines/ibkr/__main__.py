from ibkr_to_s3_flow import ibkr_to_s3_daily_flow, ibkr_to_s3_backfill_flow
from s3_to_rds_flow import s3_to_rds_daily_flow, s3_to_rds_backfill_flow
import datetime as dt

def ibkr_daily_flow() -> None:
    ibkr_to_s3_daily_flow()
    s3_to_rds_daily_flow()

def ibkr_backfill_flow(start_date: dt.date, end_date: dt.date) -> None:
    ibkr_to_s3_backfill_flow(start_date, end_date)
    s3_to_rds_backfill_flow(start_date, end_date)


if __name__ == '__main__':
    import os
    import aws
    import dotenv

    dotenv.load_dotenv(override=True)

    # ibkr_daily_flow()

    # db = aws.RDS(
    #     db_endpoint=os.getenv("DB_ENDPOINT"),
    #     db_name=os.getenv("DB_NAME"),
    #     db_user=os.getenv("DB_USER"),
    #     db_password=os.getenv("DB_PASSWORD"),
    #     db_port=os.getenv("DB_PORT"),
    # )

    # print(
    #     # db.execute('SELECT * FROM "2025-08-11_POSITIONS";')
    #     db.execute_to_df("SELECT * FROM positions_new WHERE report_date = '2025-08-11';")
    #     # db.execute("DROP TABLE positions_new;")
    # )

