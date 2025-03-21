from datetime import date
from pipelines.utils.database import Database
from pipelines.utils.barra_file import (
    BarraFile,
    Folder,
    ZipFolder,
    File,
)
from utils import render_sql_file, get_last_market_date


def load_barra_file(barra_file: BarraFile) -> None:
    """Task for loading a BarraFile into duckdb."""
    print(f"Loading barra file: {barra_file.file_name}")
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
            "sql/factors_pivot.sql", long_table=transform_table, wide_table=pivot_table
        )
        db.execute(pivot_query)

        merge_pivot_query = render_sql_file(
            "sql/factors_wide_merge.sql", source_table=pivot_table
        )
        db.execute(merge_pivot_query)


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

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


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
