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
    stage_table = f"pricing_{date_string}_stage"
    transform_table = f"pricing_{date_string}_transform"

    with Database() as db:
        _ = barra_file.df
        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM _;"
        )
        db.execute(stage_query)

        transform_query = render_sql_file(
            "sql/pricing_transform.sql",
            stage_table=stage_table,
            transform_table=transform_table,
        )
        db.execute(transform_query)

        merge_query = render_sql_file(
            "sql/pricing_merge.sql",
            source_table=transform_table,
        )
        db.execute(merge_query)


@flow(name="barra-returns-backfill-flow")
def barra_returns_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra reutrns backfill."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    current_date = start_date
    while current_date <= end_date:
        barra_file = BarraFile(
            folder=Folder.HISTORY,
            model=Model.USSLOW,
            model_folder=ModelFolder.SM,
            frequency=Frequency.DAILY,
            zip_folder=ZipFolder.SMD_USSLOW_100_D,
            file=File.USSLOW_Daily_Asset_Price,
            date_=current_date,
        )

        if barra_file.exists:
            load_barra_file(barra_file=barra_file)

        current_date += timedelta(days=1)


@flow(name="barra-returns-daily-flow")
def barra_returns_daily_flow() -> None:
    """Flow for orchestrating barra reutrns each day."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    barra_file = BarraFile(
        folder=Folder.HISTORY,
        model=Model.USSLOW,
        model_folder=ModelFolder.SM,
        frequency=Frequency.DAILY,
        zip_folder=ZipFolder.SMD_USSLOW_100_D,
        file=File.USSLOW_Daily_Asset_Price,
        date_=date.today(),
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


if __name__ == "__main__":
    barra_returns_backfill_flow(start_date=date(2025, 1, 1), end_date=date(2025, 2, 21))
    # barra_daidly_flow()

    with Database() as db:
        print(db.execute("SELECT * FROM assets ORDER BY barrid, date;").pl())
