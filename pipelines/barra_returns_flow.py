from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from pipelines.utils import barra_schema, barra_columns
import os
from tqdm import tqdm
from utils import get_last_market_date


def load_barra_history_files(year: int) -> pl.DataFrame:
    zip_folder_path = f"/home/amh1124/groups/grp_msci_barra/nobackup/archive/history/usslow/sm/daily/SMD_USSLOW_100_D_{year}.zip"

    # Open zip folder
    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        # Read each file
        dfs = [
            pl.read_csv(
                BytesIO(zip_folder.read(file_name)),
                skip_rows=1,
                separator="|",
                schema_overrides=barra_schema,
                try_parse_dates=True,
            )
            for file_name in zip_folder.namelist()
            if file_name.startswith("USSLOW_Daily_Asset_Price")
        ]

    # Concat
    return pl.concat(dfs, how="vertical") if dfs else pl.DataFrame()


def load_current_barra_files() -> pl.DataFrame:
    usslow_dir = "/home/amh1124/groups/grp_msci_barra/nobackup/archive/us/usslow/"

    dfs = []

    dates = get_last_market_date(n_days=20)

    for date_ in tqdm(dates, desc="Searching Files"):
        date_long = date_.strftime("%Y%m%d")
        date_short = date_.strftime("%y%m%d")
        zip_path = f"SMD_USSLOWL_100_{date_short}.zip"
        file_path = f"USSLOW_Daily_Asset_Price.{date_long}"

        # Check zip folder exists
        if os.path.exists(usslow_dir + zip_path):
            # Open zip folder
            with zipfile.ZipFile(usslow_dir + zip_path, "r") as zip_folder:
                dfs.append(
                    # Read each file
                    pl.read_csv(
                        BytesIO(zip_folder.read(file_path)),
                        skip_rows=1,
                        separator="|",
                        schema_overrides=barra_schema,
                        try_parse_dates=True,
                    )
                )

    df = pl.concat(dfs)

    return df


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # Rename columns
        .rename(barra_columns, strict=False)
        # Clean date column
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y%m%d"))
        # Filter out End of File lines
        .filter(pl.col("barrid").ne("[End of File]"))
        # Sort
        .sort(["barrid", "date"])
    )


def merge_into_master(master_file: str, df: pl.DataFrame) -> None:
    (
        # Scan master parquet file
        pl.scan_parquet(master_file)
        # Update
        .update(df.lazy(), on=["date", "barrid"], how="full")
        .collect()
        # Write
        .write_parquet(master_file)
    )


def barra_returns_history_flow(start_date: date, end_date: date) -> None:
    os.makedirs("data/assets", exist_ok=True)

    # Get years
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Barra Returns"):
        master_file = f"data/assets/assets_{year}.parquet"

        # Load raw df
        raw_df = load_barra_history_files(year)

        # Clean
        clean_df = clean_barra_df(raw_df)

        # Merge
        if os.path.exists(master_file):
            merge_into_master(master_file, clean_df)

        # or Create
        else:
            clean_df.write_parquet(master_file)


def barra_returns_daily_flow() -> None:
    os.makedirs("data/assets", exist_ok=True)
    
    # Load raw df
    raw_df = load_current_barra_files()

    # Clean
    clean_df = clean_barra_df(raw_df)

    # Get all years
    years = (
        clean_df
        # Select unique year columns
        .select(pl.col("date").dt.year().unique().sort().alias("year"))["year"]
    )

    # Update master files by year
    for year in tqdm(years, desc="Daily Barra Returns"):
        # Subset df to year
        year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

        # Merge into master
        master_file = f"data/assets/assets_{year}.parquet"

        # Merge
        if os.path.exists(master_file):
            merge_into_master(master_file, year_df)

        # or Create
        else:
            year_df.write_parquet(master_file)


if __name__ == "__main__":
    # ----- History Flow -----
    barra_returns_history_flow(start_date=date(2024, 1, 1), end_date=date.today())

    # ----- Current Flow -----
    barra_returns_daily_flow()

    # ----- Print -----
    print(pl.read_parquet("data/assets/assets_*.parquet"))
