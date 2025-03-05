# Let's see if I can write a pipeline for Barra returns
from datetime import date
import polars as pl
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
import duckdb
from prefect import task, flow


@task
def get_barra_file_as_pl() -> pl.DataFrame:
    BARRA_FOLDER = Path("/Users/andrew/groups/grp_barra/barra")
    DAILY_FOLDER = BARRA_FOLDER / "history/usslow/sm/daily"

    zip_file = "SMD_USSLOW_100_D_2025.zip"
    file_date = date(2025, 2, 21).strftime("%Y%m%d")
    file_name = "USSLOW_Daily_Asset_Price." + file_date

    zip_file_path = DAILY_FOLDER / zip_file
    print(zip_file_path)

    # Open zipped folder as a ref
    with ZipFile(zip_file_path, "r") as zip_ref:
        # Open a specific file in the zipped folder
        with zip_ref.open(file_name) as file:
            return pl.read_csv(BytesIO(file.read()), skip_rows=1, separator="|")


@task
def clean_barra_pl(raw_df: pl.DataFrame) -> pl.DataFrame:
    df = (
        raw_df.head(-1)  # Drop last row
        # Ranme columns
        .rename(
            {
                "!Barrid": "barrid",
                "Price": "price",
                "Capt": "market_cap",
                "PriceSource": "price_source",
                "Currency": "currency",
                "DlyReturn%": "return",
                "DataDate": "date",
            }
        )
        # Cast date type
        .with_columns(pl.col("date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d"))
    )

    return df

@task
def load_pl_into_duckdb(df: pl.DataFrame) -> None:
    with duckdb.connect("barra.duckdb") as con:
        con.execute("CREATE TABLE IF NOT EXISTS barra_returns AS SELECT * FROM df;")

@flow(name="barra-backfill-flow")
def barra_backfill_flow() -> None:
    raw_df = get_barra_file_as_pl()
    clean_df = clean_barra_pl(raw_df)
    load_pl_into_duckdb(clean_df)


if __name__ == "__main__":
    barra_backfill_flow()

    # Verify run
    with duckdb.connect("barra.duckdb") as con:
        result = con.execute("SELECT * FROM barra_returns;").pl()
        print(result)
