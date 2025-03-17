from datetime import date
from prefect import task, flow
from pipelines.utils.database import Database
from utils import render_sql_file, get_last_market_date
import polars as pl
import wrds


def load_ftse_russell_df(start_date: date, end_date: date) -> None:
    """Task for loading a dataframe of FTSE Russell data into duckdb."""
    print(f"Loading ftse file")
    date_string = end_date.strftime("%Y%m%d")
    stage_table = f"ftse_russell_{date_string}_stage"

    with Database() as db:
        wrds_db = wrds.Connection(wrds_username="amh1124")

        schema = {
            "date": pl.String,
            "cusip": pl.String,
            "ticker": pl.String,
            "russell_2000": pl.Boolean,
            "russell_1000": pl.Boolean,
            "russell_3000_weight": pl.Float64,
            "russell_2000_weight": pl.Float64,
            "russell_1000_weight": pl.Float64,
        }
        df = wrds_db.raw_sql(
            f"""
                SELECT 
                    date, 
                    cusip, 
                    ticker, 
                    CASE WHEN russell2000 = 'Y' THEN true ELSE false END AS russell_2000,
                    CASE WHEN russell1000 = 'Y' THEN true ELSE false END AS russell_1000,
                    r3000_wt AS russell_3000_weight,
                    r2000_wt AS russell_2000_weight,
                    r1000_wt AS russell_1000_weight
                FROM ftse_russell_us.idx_holdings_us
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY cusip, date
                ;
                """
        )
        df = pl.from_pandas(df, schema_overrides=schema)

        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM df;"
        )
        db.execute(stage_query)

        merge_query = render_sql_file(
            "sql/ftse_russell_merge.sql",
            source_table=stage_table,
        )
        db.execute(merge_query)


def ftse_russell_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra ids backfill."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    if start_date.year == end_date.year:
        end_date = date(start_date.year + 1, 1, 1)

    years = list(range(start_date.year, end_date.year + 1))

    for i in range(0, len(years) - 1):
        start_year = years[i]
        end_year = years[i + 1]

        load_ftse_russell_df(
            start_date=date(start_year, 1, 1), end_date=date(end_year, 12, 31)
        )


def ftse_russell_daily_flow() -> None:
    """Flow for orchestrating Russell constituents each day."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    current_year = date.today().year

    load_ftse_russell_df(
        start_date=date(current_year, 1, 1), end_date=date(current_year, 12, 31)
    )
