from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from pipelines.utils import barra_schema, barra_columns
import os
from tqdm import tqdm
from pipelines.utils import get_last_market_date


def load_current_barra_files() -> pl.DataFrame:
    bime_dir = "/home/amh1124/groups/grp_msci_barra/nobackup/archive/bime/"

    dates = get_last_market_date(n_days=40)

    for date_ in reversed(dates):
        date_long = date_.strftime("%Y%m%d")
        date_short = date_.strftime("%y%m%d")
        zip_path = f"SMD_USSLOW_XSEDOL_ID_{date_short}.zip"
        file_path = f"USA_Asset_Identity.{date_long}"

        # Check zip folder exists
        if os.path.exists(bime_dir + zip_path):
            # Open zip folder
            with zipfile.ZipFile(bime_dir + zip_path, "r") as zip_folder:
                return (
                    # Read each file
                    pl.read_csv(
                        BytesIO(zip_folder.read(file_path)),
                        skip_rows=1,
                        separator="|",
                        schema_overrides=barra_schema,
                        try_parse_dates=True,
                    )
                )

    return pl.DataFrame()


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df
        # Rename columns
        .rename(barra_columns, strict=False)
        # Clean date column
        .with_columns(pl.col("start_date", "end_date").str.strptime(pl.Date, "%Y%m%d"))
        # Filter out End of File lines
        .filter(pl.col("barrid").ne("[End of File]"))
    )

    return (
        df.with_columns(pl.col("end_date").clip(upper_bound=date.today()))
        .with_columns(pl.date_ranges("start_date", "end_date").alias("date"))
        .explode("date")
        .drop("start_date", "end_date")
        # Sort
        .sort(["barrid", "date"])
    )


def merge_into_master(master_file: str, df: pl.DataFrame) -> None:
    # Get master columns lazily
    master_columns = pl.scan_parquet(master_file).collect_schema().names()

    # Add missing columns
    missing_columns = set(df.columns) - set(master_columns)
    for col in missing_columns:
        dtype = df.schema[col]
        (
            pl.scan_parquet(master_file)
            .with_columns(pl.lit(None, dtype=dtype).alias(col))
            .collect()
            .write_parquet(master_file)
        )

    # Update rows
    (
        # Scan master parquet file
        pl.scan_parquet(master_file)
        # Update
        .update(df.lazy(), on=["date", "barrid"], how="left")
        .collect()
        # Write
        .write_parquet(master_file)
    )


def barra_assets_daily_flow() -> None:
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
    for year in tqdm(years, desc="Barra Assets"):
        master_file = f"data/assets/assets_{year}.parquet"

        if os.path.exists(master_file):
            # Subset df to year
            year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

            # Filter to market dates
            dates = (
                pl.scan_parquet(master_file)
                .select("date")
                .unique()
                .sort("date")
                .collect()
            )
            year_df = year_df.filter(pl.col("date").is_in(dates))

            # Merge
            merge_into_master(master_file, year_df)

