from datetime import date
from pipelines.utils import merge_into_master, russell_schema, russell_columns
import polars as pl
import wrds
import os
from tqdm import tqdm
from pipelines.utils.tables import assets_table

def load_ftse_russell_df(start_date: date, end_date: date) -> None:
    wrds_db = wrds.Connection(wrds_username="amh1124")

    df = wrds_db.raw_sql(
        f"""
            SELECT 
                date, 
                cusip, 
                ticker, 
                russell2000,
                russell1000
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
        .rename(russell_columns, strict=False)
        .with_columns(
            pl.col("russell_2000", "russell_1000").eq("Y")
        )
    )


def ftse_russell_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra ids backfill."""
    os.makedirs("data/assets", exist_ok=True)

    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="FTSE Russell"):
        raw_df = load_ftse_russell_df(
            start_date=date(year, 1, 1), end_date=date(year, 12, 31)
        )

        clean_df = clean(raw_df)

        if assets_table.exists(year):
            assets_table.update(year, clean_df)
