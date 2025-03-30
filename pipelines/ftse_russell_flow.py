from datetime import date
from pipelines.utils import merge_into_master, russell_schema, russell_columns
import polars as pl
import wrds
import os


def load_ftse_russell_df(start_date: date, end_date: date) -> None:
    wrds_db = wrds.Connection(wrds_username="amh1124")

    df = wrds_db.raw_sql(
        f"""
            SELECT 
                date, 
                cusip, 
                ticker, 
                russell2000,
                russell1000,
                r3000_wt,
                r2000_wt,
                r1000_wt
            FROM ftse_russell_us.idx_holdings_us
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY cusip, date
            ;
            """
    )
    df = pl.from_pandas(df, schema_overrides=russell_schema)

    return df


def clean(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # Rename columns
        .rename(russell_columns, strict=False).with_columns(
            pl.col("russell_2000", "russell_1000").eq("Y")
        )
    )


def ftse_russell_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra ids backfill."""
    os.makedirs("data/assets", exist_ok=True)

    years = list(range(start_date.year, end_date.year + 1))

    for year in years:
        raw_df = load_ftse_russell_df(
            start_date=date(year, 1, 1), end_date=date(year, 12, 31)
        )

        clean_df = clean(raw_df)

        # Merge into master
        master_file = f"data/assets/assets_{year}.parquet"

        # Merge
        if os.path.exists(master_file):
            merge_into_master(master_file, clean_df, on=["cusip", "date"], how="left")


if __name__ == "__main__":
    os.makedirs("data/assets", exist_ok=True)

    # ----- History Flow -----
    # ftse_russell_backfill_flow(start_date=date(2024, 1, 1), end_date=date(2025, 12, 31))

    # ----- Print -----
    print(
        pl.scan_parquet("data/assets/assets_*.parquet")
        .filter(pl.col("rootid").eq(pl.col("barrid")))
        .with_columns(
            pl.col('ticker', 'russell_1000', 'russell_2000').fill_null(strategy='forward').over('barrid')
        )
        .filter(pl.col("russell_1000") | pl.col("russell_2000"))
        .filter(pl.col('date').eq(date(2025, 3, 25)))
        .sort(['barrid', 'date'])
        .select("date", "barrid", 'rootid', "cusip", "ticker", "russell_1000", "russell_2000", 'price', "total_risk")
        .collect()
    )
