from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from pipelines.utils import barra_schema, barra_columns, get_last_market_date
from pipelines.utils.barra_datasets import barra_risk
from utils.tables import Database
import os
from tqdm import tqdm


def load_barra_history_files(year: int) -> pl.DataFrame:
    file_name = barra_risk.file_name()

    dfs = []
    for zip_folder_name in sorted(os.listdir(barra_risk.history_zip_folder())):
        if barra_risk.history_zip_file(year) in zip_folder_name:
            with zipfile.ZipFile(f"{barra_risk.history_zip_folder()}/{zip_folder_name}", "r") as zip_folder:
                folder_dfs = [
                    pl.read_csv(
                        BytesIO(zip_folder.read(file)),
                        skip_rows=2,
                        separator="|",
                        schema_overrides=barra_schema,
                        try_parse_dates=True,
                    )
                    for file in zip_folder.namelist()
                    if file.startswith(file_name)
                ]

                folder_df = pl.concat(folder_dfs, how="vertical") if folder_dfs else pl.DataFrame()
                dfs.append(folder_df)
    
    return pl.concat(dfs, how="vertical") if dfs else pl.DataFrame()


def load_current_barra_files() -> pl.DataFrame:
    dfs = []

    dates = get_last_market_date(n_days=60)

    for date_ in dates:
        zip_folder_path = barra_risk.daily_zip_folder_path(date_)
        file_name = barra_risk.file_name(date_)

        if os.path.exists(zip_folder_path):
            with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
                dfs.append(
                    pl.read_csv(
                        BytesIO(zip_folder.read(file_name)),
                        skip_rows=2,
                        separator="|",
                        schema_overrides=barra_schema,
                        try_parse_dates=True,
                    )
                )

    df = pl.concat(dfs)

    return df


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(barra_columns, strict=False)
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y%m%d"))
        .filter(pl.col("barrid").ne("[End of File]"))
        .sort(["barrid", "date"])
    )


def barra_risk_history_flow(start_date: date, end_date: date, database: Database) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Barra Risk"):
        raw_df = load_barra_history_files(year)
        clean_df = clean_barra_df(raw_df)

        database.assets_table.create_if_not_exists(year)
        database.assets_table.update(year, clean_df)


def barra_risk_daily_flow(database: Database) -> None:
    raw_df = load_current_barra_files()
    clean_df = clean_barra_df(raw_df)

    years = clean_df.select(pl.col("date").dt.year().unique().sort().alias("year"))[
        "year"
    ]

    for year in tqdm(years, desc="Daily Barra Risk"):
        year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

        database.assets_table.create_if_not_exists(year)
        database.assets_table.update(year, year_df)
