from datetime import date, timedelta
from prefect import task, flow
from pipelines.database import Database
from pipelines.barra_file import (
    BarraFile,
    Folder,
    Model,
    ModelFolder,
    Frequency,
    ZipFolder,
    File,
)
from utils import render_sql_file


@task(task_run_name="barra-file-pipeline_{barra_file.date_}")
def load_barra_file(barra_file: BarraFile) -> None:
    """Task for loading a BarraFile into duckdb."""
    date_string = barra_file.date_.strftime("%Y%m%d")
    stage_table = f"covariances_{date_string}_stage"
    transform_table = f"covariances_{date_string}_transform"
    pivot_table = f"covariances_{date_string}_pivot"

    with Database() as db:
        _ = barra_file.df
        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM _;"
        )
        db.execute(stage_query)

        transform_query = render_sql_file(
            "sql/covariances_transform.sql",
            stage_table=stage_table,
            transform_table=transform_table,
        )
        db.execute(transform_query)

        merge_query = render_sql_file(
            "sql/covariances_merge.sql",
            source_table=transform_table,
        )
        db.execute(merge_query)

        pivot_query = render_sql_file(
            "sql/covariances_pivot.sql",
            long_table=transform_table,
            wide_table=pivot_table
        )
        db.execute(pivot_query)

        merge_pivot_query = render_sql_file(
            "sql/covariances_wide_merge.sql",
            source_table=pivot_table
        )
        db.execute(merge_pivot_query)


@flow(name="barra-covariances-backfill-flow")
def barra_covariances_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra covariances backfill."""

    with Database() as db:
        create_query = render_sql_file("sql/covariances_create.sql")
        db.execute(create_query)
        create_wide_query = render_sql_file("sql/covariances_wide_create.sql")
        db.execute(create_wide_query)

    current_date = start_date
    while current_date <= end_date:
        barra_file = BarraFile(
            folder=Folder.HISTORY,
            model=Model.USSLOW,
            model_folder=ModelFolder.SM,
            frequency=Frequency.DAILY,
            zip_folder=ZipFolder.SMD_USSLOWL_100_D,
            file=File.USSLOWL_100_Covariance,
            date_=current_date,
        )

        if barra_file.exists:
            load_barra_file(barra_file=barra_file)

        current_date += timedelta(days=1)


@flow(name="barra-covariances-daily-flow")
def barra_covariances_daily_flow() -> None:
    """Flow for orchestrating barra covariances each day."""

    with Database() as db:
        create_query = render_sql_file("sql/covariances_create.sql")
        db.execute(create_query)
        create_wide_query = render_sql_file("sql/covariances_wide_create.sql")
        db.execute(create_wide_query)

    barra_file = BarraFile(
        folder=Folder.HISTORY,
        model=Model.USSLOW,
        model_folder=ModelFolder.SM,
        frequency=Frequency.DAILY,
        zip_folder=ZipFolder.SMD_USSLOWL_100_D,
        file=File.USSLOWL_100_Covariance,
        date_=date.today(),
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


if __name__ == "__main__":
    barra_covariances_backfill_flow(start_date=date(2025, 1, 1), end_date=date.today())

    with Database() as db:
        print(db.execute("SELECT * FROM covariances ORDER BY factor1, factor2, date;").pl())
        print(db.execute("SELECT * FROM covariances_wide ORDER BY factor1, date;").pl())
