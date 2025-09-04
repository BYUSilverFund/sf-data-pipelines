from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from sf_data_pipelines.utils import barra_schema, barra_columns
import os
from tqdm import tqdm
from sf_data_pipelines.utils import get_last_market_date
from utils.tables import Database
from sf_data_pipelines.utils.barra_datasets import barra_assets


def load_current_barra_files() -> pl.DataFrame:
    dates = get_last_market_date(n_days=60)

    for date_ in reversed(dates):
        zip_folder_path = barra_assets.daily_zip_folder_path(date_)
        file_path = barra_assets.file_name(date_)

        if os.path.exists(zip_folder_path):
            with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
                return pl.read_csv(
                    BytesIO(zip_folder.read(file_path)),
                    skip_rows=1,
                    separator="|",
                    schema_overrides=barra_schema,
                    try_parse_dates=True,
                )

    return pl.DataFrame()


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.rename(barra_columns, strict=False)
        .with_columns(pl.col("start_date", "end_date").str.strptime(pl.Date, "%Y%m%d"))
        .filter(pl.col("barrid").ne("[End of File]"))
    )

    return (
        df.with_columns(pl.col("end_date").clip(upper_bound=date.today()))
        .with_columns(pl.date_ranges("start_date", "end_date").alias("date"))
        .explode("date")
        .drop("start_date", "end_date")
        .sort(["barrid", "date"])
    )


def barra_assets_daily_flow(database: Database) -> None:
    raw_df = load_current_barra_files()
    clean_df = clean_barra_df(raw_df)

    years = clean_df.select(pl.col("date").dt.year().unique().sort().alias("year"))[
        "year"
    ]

    for year in tqdm(years, desc="Barra Assets"):
        if database.assets_table.exists(year):
            year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

            dates = (
                database.assets_table.read(year).select("date").unique().sort("date").collect()
            )
            year_df = year_df.filter(pl.col("date").is_in(dates))

            database.assets_table.update(year, year_df)
