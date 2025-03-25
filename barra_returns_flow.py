from datetime import date, datetime
import zipfile
import polars as pl
from io import BytesIO
from tools import raw_schema, barra_columns, clean_schema
import os
import exchange_calendars as xcals


def get_last_market_date() -> date:
    df = (
        pl.from_pandas(xcals.get_calendar("XNYS").schedule)
        # Cast date types
        .with_columns(pl.col("close").cast(pl.Date).alias("date"))
        # Get previous date
        .with_columns(pl.col("date").shift(1).alias("previous_date"))
        # Filter
        .filter(pl.col("date").le(date.today()))
        # Sort
        .sort("date")["previous_date"]
        # Get last previous date
        .last()
    )

    return df


def load_barra_history_files(year: int) -> pl.DataFrame:
    zip_folder_path = f"/Users/andrew/groups/grp_msci_barra/nobackup/archive/history/usslow/sm/daily/SMD_USSLOW_100_D_{year}.zip"

    # Open zip folder
    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        # Read each file
        dfs = [
            pl.read_csv(
                BytesIO(zip_folder.read(file_name)),
                skip_rows=1,
                separator="|",
                schema_overrides=raw_schema,
                try_parse_dates=True,
            )
            for file_name in zip_folder.namelist()
            if file_name.startswith("USSLOW_Daily_Asset_Price")
        ]

    # Concat
    return pl.concat(dfs, how="vertical") if dfs else pl.DataFrame()


def load_current_barra_files() -> pl.DataFrame:
    current_dir = "/Users/andrew/groups/grp_msci_barra/nobackup/archive/us/usslow/"

    dfs = []
    for zip_path in os.listdir(current_dir):
        if zip_path.startswith("SMD_USSLOWL_100_") and zip_path.endswith(".zip"):

            # Date strings
            date_short = zip_path.split(".")[0].split("_")[-1]
            date_long = datetime.strptime(date_short, "%y%m%d").strftime("%Y%m%d")

            # File path
            file_path = f"USSLOW_Daily_Asset_Price.{date_long}"

            # Open zip folder
            with zipfile.ZipFile(current_dir + zip_path, "r") as zip_folder:
                dfs.append(
                    # Read each file
                    pl.read_csv(
                        BytesIO(zip_folder.read(file_path)),
                        skip_rows=1,
                        separator="|",
                        schema_overrides=raw_schema,
                        try_parse_dates=True,
                    )
                )

    df = pl.concat(dfs)

    return df


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # Rename columns
        .rename(barra_columns)
        # Clean date column
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y%m%d"))
        .cast(clean_schema)
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
    # Get years
    years = list(range(start_date.year, end_date.year + 1))

    for year in years:
        master_file = f"assets_{year}.parquet"

        # Create master file if not exists
        if not os.path.exists(master_file):
            pl.DataFrame([], schema=clean_schema).write_parquet(master_file)

        # Load raw df
        raw_df = load_barra_history_files(year)

        # Clean 
        clean_df = clean_barra_df(raw_df)

        # Merge into master
        merge_into_master(master_file, clean_df)


def barra_returns_current_flow() -> None:
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
    for year in years:
        # Subset df to year
        year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

        # Merge into master
        master_file = f"assets_{year}.parquet"
        merge_into_master(master_file, year_df)


if __name__ == "__main__":
    # ----- History Flow -----
    barra_returns_history_flow(start_date=date(2025, 1, 1), end_date=date(2025, 3, 7))

    # ----- Current Flow -----
    barra_returns_current_flow()

    # ----- Print -----
    print(pl.read_parquet("assets_2025.parquet"))
