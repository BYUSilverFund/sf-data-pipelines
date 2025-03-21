from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from tools import raw_schema, barra_columns, clean_schema
import duckdb


def load_barra_file_by_year(year: int, start_date: date, end_date: date) -> pl.DataFrame:
    zip_folder_path = f"/Users/andrew/groups/grp_msci_barra/nobackup/archive/history/usslow/sm/daily/SMD_USSLOW_100_D_{year}.zip"
    start_int = int(start_date.strftime("%Y%m%d"))
    end_int = int(end_date.strftime("%Y%m%d"))

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        dfs = [
            pl.read_csv(
                BytesIO(zip_folder.read(file_name)),
                skip_rows=1,
                separator="|",
                schema_overrides=raw_schema,
                try_parse_dates=True,
            )
            for file_name in zip_folder.namelist()
            if file_name.startswith("USSLOW_Daily_Asset_Price") and
               file_name.rsplit(".", 1)[-1].isdigit() and
               start_int <= int(file_name.rsplit(".", 1)[-1]) <= end_int
        ]

    return pl.concat(dfs, how="vertical") if dfs else pl.DataFrame()

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

def merge_into_master(df: pl.DataFrame) -> None:
   (
       pl.scan_parquet('master.parquet')
       .update(
            df.lazy(),
            on=['date', 'barrid'],
            how='full'
        )
        .collect()
        .write_parquet("master.parquet")
   )


def barra_returns_backfill_flow(start_date: date, end_date: date) -> None:

    years = list(range(start_date.year, end_date.year + 1))

    for year in years:
        raw_df = load_barra_file_by_year(year, start_date, end_date)
        clean_df = clean_barra_df(raw_df)
        merge_into_master(clean_df)

def barra_returns_daily_flow() -> None:
    pass


if __name__ == "__main__":
    import time
    start = time.time()
    barra_returns_backfill_flow(start_date=date(2025, 1, 1), end_date=date(2025, 3, 7))
    print(f"{time.time() - start} seconds")
