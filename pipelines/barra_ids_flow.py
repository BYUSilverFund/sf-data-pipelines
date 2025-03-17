from datetime import date
from prefect import task, flow
from pipelines.utils.database import Database
from pipelines.utils.barra_file import (
    BarraFile,
    Folder,
    ZipFolder,
    File,
)
from utils import render_sql_file, get_last_market_date
import polars as pl


def load_barra_file(barra_file: BarraFile, start_date: date, end_date: date) -> None:
    """Task for loading a BarraFile into duckdb."""
    print(f"Loading barra file: {barra_file.file_name}")
    date_string = barra_file.date_.strftime("%Y%m%d")
    stage_table = f"barra_ids_{date_string}_stage"

    with Database() as db:
        df = (
            barra_file.df
            # Rename columns
            .rename(
                {
                    "!Barrid": "barrid",
                    "AssetIDType": "asset_id_type",
                    "AssetID": "asset_id",
                    "StartDate": "start_date",
                    "EndDate": "end_date",
                }
            )
            # Cast date columns
            .with_columns(
                pl.col(["start_date", "end_date"])
                .cast(pl.String)
                .str.strptime(dtype=pl.Date, format="%Y%m%d")
            )
            # Remove end of file line
            .filter(pl.col("barrid").ne("[End of File]"))
            # Clip dates
            .with_columns(pl.col("end_date").clip(upper_bound=date.today()))
            # Pivot out asset id types
            .pivot(
                index=["start_date", "end_date", "barrid"],
                on="asset_id_type",
                values="asset_id",
            )
        )

        df = (
            df
            # Create date range column
            .with_columns(pl.date_ranges("start_date", "end_date").alias("date"))
            # Explode date range to rows
            .explode("date")
            # Filter to date range
            .filter(pl.col("date").is_between(start_date, end_date))
            # Rename asset id columns
            .rename({col: col.lower() for col in df.columns})
            # Aggregate rows
            .group_by(["date", "barrid"])
            .agg(
                pl.col("cins").max(),
                pl.col("cusip").max(),
                pl.col("isin").max(),
                pl.col("localid").max(),
            )
        )

        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM df;"
        )
        db.execute(stage_query)

        merge_query = render_sql_file(
            "sql/ids_merge.sql",
            source_table=stage_table,
        )
        db.execute(merge_query)


def barra_ids_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra ids backfill."""
    last_market_date = get_last_market_date(end_date)

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOW_XSEDOL_ID,
        file=File.USA_XSEDOL_Asset_ID,
        date_=last_market_date,
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file, start_date=start_date, end_date=end_date)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


def barra_ids_daily_flow() -> None:
    """Flow for orchestrating barra ids each day."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOW_XSEDOL_ID,
        file=File.USA_XSEDOL_Asset_ID,
        date_=date.today(),
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)
