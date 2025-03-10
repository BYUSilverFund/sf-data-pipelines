from datetime import date, timedelta
from prefect import task, flow
from pipelines.database import Database
from pipelines.barra_file import (
    BarraFile,
    Folder,
    ZipFolder,
    File,
)
from utils import render_sql_file
import exchange_calendars as xcals
import polars as pl


@task(task_run_name="barra-file-pipeline_{barra_file.date_}")
def load_barra_file(barra_file: BarraFile) -> None:
    """Task for loading a BarraFile into duckdb."""
    date_string = barra_file.date_.strftime("%Y%m%d")
    stage_table = f"factors_{date_string}_stage"
    transform_table = f"factors_{date_string}_transform"
    pivot_table = f"factors_{date_string}_pivot"

    with Database() as db:
        _ = barra_file.df
        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM _;"
        )
        db.execute(stage_query)

        transform_query = render_sql_file(
            "sql/factors_transform.sql",
            stage_table=stage_table,
            transform_table=transform_table,
        )
        db.execute(transform_query)

        merge_query = render_sql_file(
            "sql/factors_merge.sql",
            source_table=transform_table,
        )
        db.execute(merge_query)

        pivot_query = render_sql_file(
            "sql/factors_pivot.sql",
            long_table=transform_table,
            wide_table=pivot_table
        )
        db.execute(pivot_query)

        merge_pivot_query = render_sql_file(
            "sql/factors_wide_merge.sql",
            source_table=pivot_table
        )
        db.execute(merge_pivot_query)

@task 
def get_last_market_date(current_date: date) -> date:
    """
    Get the last trading day before the given date.

    This function retrieves the previous market date based on the
    New York Stock Exchange (XNYS) trading calendar.

    Args:
        current_date (date): The reference date to find the previous trading day.

    Returns:
        date: The last trading day before the given date.
    """
    # Load market calendar
    market_calendar = (
        pl.DataFrame(xcals.get_calendar("XNYS").schedule)
        .with_columns(pl.col("close").dt.date())
        .select(pl.col("close").alias("date"))
        .with_columns(pl.col("date").shift(1).alias("prev_date"))
    )

    # Get previous date
    prev_date = market_calendar.filter(pl.col("date").eq(current_date))["prev_date"].max()

    return prev_date



@flow(name="barra-factors-backfill-flow")
def barra_factors_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra factors backfill."""

    last_market_date = get_last_market_date(end_date)

    with Database() as db:
        create_query = render_sql_file("sql/factors_create.sql")
        db.execute(create_query)
        create_wide_query = render_sql_file("sql/factors_wide_create.sql")
        db.execute(create_wide_query)

    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOWL_100,
        file=File.USSLOWL_100_DlyFacRet,
        date_=last_market_date,
    )

    print(barra_file.zip_folder_path)

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


@flow(name="barra-factors-daily-flow")
def barra_factors_daily_flow() -> None:
    """Flow for orchestrating barra factors each day."""
    last_market_date = get_last_market_date(date.today())

    with Database() as db:
        create_query = render_sql_file("sql/factors_create.sql")
        db.execute(create_query)
        create_wide_query = render_sql_file("sql/factors_wide_create.sql")
        db.execute(create_wide_query)

    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOWL_100,
        file=File.USSLOWL_100_DlyFacRet,
        date_=last_market_date,
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


if __name__ == "__main__":
    barra_factors_backfill_flow(start_date=date(2025, 2, 21), end_date=date.today())

    with Database() as db:
        print(db.execute("SELECT * FROM factors;").pl())
        print(db.execute("SELECT * FROM factors_wide;").pl())
