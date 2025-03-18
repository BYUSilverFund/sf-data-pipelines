from datetime import date
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
    stage_table = f"barra_assets_{date_string}_stage"

    with Database() as db:
        _ = (
            barra_file.df
            # Rename columns
            .rename({
                '!Barrid': 'barrid',
                'Name': 'name',
                'Instrument': 'instrument',
                'IssuerID': 'issuerid',
                'ISOCountryCode': 'iso_country_code',
                'ISOCurrencyCode': 'iso_currency_code',
                'RootID': 'rootid',
                'StartDate': 'start_date',
                'EndDate': 'end_date'
            })
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
            # Create date range column
            .with_columns(pl.date_ranges("start_date", "end_date").alias("date"))
            # Explode date range to rows
            .explode("date")
            .filter(pl.col("date").is_between(start_date, end_date))
        )

        stage_query = (
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} AS SELECT * FROM _;"
        )
        db.execute(stage_query)

        merge_query = render_sql_file(
            "sql/assets_merge.sql",
            source_table=stage_table,
        )
        db.execute(merge_query)


def barra_assets_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra assets backfill."""
    last_market_date = get_last_market_date(end_date)

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOW_XSEDOL_ID,
        file=File.USA_Asset_Identity,
        date_=last_market_date,
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file, start_date=start_date, end_date=end_date)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)


def barra_assets_daily_flow() -> None:
    """Flow for orchestrating barra assets each day."""

    with Database() as db:
        create_query = render_sql_file("sql/assets_create.sql")
        db.execute(create_query)

    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOW_XSEDOL_ID,
        file=File.USA_Asset_Identity,
        date_=date.today(),
    )

    if barra_file.exists:
        load_barra_file(barra_file=barra_file)
    else:
        msg = f"BarraFile '{barra_file.file_name}' does not exist!"
        raise RuntimeError(msg)
