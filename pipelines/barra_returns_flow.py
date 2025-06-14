from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from pipelines.utils import barra_schema, barra_columns
from utils.barra_datasets import barra_returns
import os
from tqdm import tqdm
from utils import get_last_market_date
from utils.tables import Database


def load_barra_history_files(year: int) -> pl.DataFrame:
    zip_folder_path = barra_returns.history_zip_folder_path(year)
    file_name = barra_returns.file_name()

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        dfs = [
            pl.read_csv(
                BytesIO(zip_folder.read(file)),
                skip_rows=1,
                separator="|",
                schema_overrides=barra_schema,
                try_parse_dates=True,
            )
            for file in zip_folder.namelist()
            if file.startswith(file_name)
        ]

    return pl.concat(dfs, how="vertical") if dfs else pl.DataFrame()


def load_current_barra_files() -> pl.DataFrame:
    dfs = []

    dates = get_last_market_date(n_days=20)

    for date_ in tqdm(dates, desc="Searching Files"):
        zip_folder_path = barra_returns.daily_zip_folder_path(date_)
        file_name = barra_returns.file_name(date_)

        if os.path.exists(zip_folder_path):
            with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
                dfs.append(
                    pl.read_csv(
                        BytesIO(zip_folder.read(file_name)),
                        skip_rows=1,
                        separator="|",
                        schema_overrides=barra_schema,
                        try_parse_dates=True,
                    )
                )

    df = pl.concat(dfs)

    return df


def clean_barra_returns(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(barra_columns, strict=False)
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y%m%d"))
        .filter(pl.col("barrid").ne("[End of File]"))
        .sort(["barrid", "date"])
    )


def barra_returns_history_flow(start_date: date, end_date: date, database: Database) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Barra Returns"):
        raw_df = load_barra_history_files(year)
        clean_df = clean_barra_returns(raw_df)

        database.assets_table.create_if_not_exists(year)
        database.assets_table.upsert(year, clean_df)


# def barra_returns_daily_flow() -> None:
#     raw_df = load_current_barra_files()
#     clean_df = clean_barra_returns(raw_df)

#     years = clean_df.select(pl.col("date").dt.year().unique().sort().alias("year"))[
#         "year"
#     ]

#     for year in tqdm(years, desc="Daily Barra Returns"):
#         year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

#         assets_table.create_if_not_exists(year)
#         assets_table.upsert(year, year_df)
