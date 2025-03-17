from datetime import date, timedelta
from prefect import task, flow
from pipelines.utils.database import Database
from pipelines.utils.barra_file import (
    BarraFile,
    Folder,
    Model,
    ModelFolder,
    Frequency,
    ZipFolder,
    File,
)
from utils import render_sql_file


def load_barra_file(barra_file: BarraFile) -> None:
    """Task for loading a BarraFile into duckdb."""
    print(f"Loading barra file: {barra_file.file_name}")
    date_string = barra_file.date_.strftime("%Y%m%d")
    stage_table = f"specific_returns_{date_string}_stage"
    transform_table = f"specific_returns_{date_string}_transform"

    with Database() as db:
        _ = barra_file.df
        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM _;"
        )
        db.execute(stage_query)

        transform_query = render_sql_file(
            "sql/specific_returns_transform.sql",
            stage_table=stage_table,
            transform_table=transform_table,
        )
        db.execute(transform_query)

        merge_query = render_sql_file(
            "sql/specific_returns_merge.sql",
            source_table=transform_table,
        )
        db.execute(merge_query)


def barra_speicifc_returns_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra specific reutrns backfill."""

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
            file=File.USSLOW_100_Asset_DlySpecRet,
            date_=current_date,
        )

        if barra_file.exists:
            load_barra_file(barra_file=barra_file)

        current_date += timedelta(days=1)


def barra_specific_returns_daily_flow() -> None:
    """Flow for orchestrating barra specific reutrns each day."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    barra_file = BarraFile(
        folder=Folder.HISTORY,
        model=Model.USSLOW,
        model_folder=ModelFolder.SM,
        frequency=Frequency.DAILY,
        zip_folder=ZipFolder.SMD_USSLOW_100_D,
        file=File.USSLOW_100_Asset_DlySpecRet,
        date_=date.today(),
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)
