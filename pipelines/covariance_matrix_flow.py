import polars as pl
from datetime import date
from pipelines.system.covariance_matrix import construct_covariance_matrix
from pipelines.utils.views import in_universe_assets
import pipelines.utils.s3 as s3
from pipelines.utils import get_last_market_date

def get_covariance_matrix(date_: date) -> pl.DataFrame:
    # Assets lazyframe
    assets = (
        in_universe_assets
        # Filter by date
        .filter(pl.col("date").eq(date_)).select("date", "barrid", "ticker")
    )

    # Barrids list
    barrids = assets.select("barrid").collect()["barrid"].sort().to_list()

    # Mapping dictionary
    mapping_df = assets.select("barrid", "ticker").collect().to_dicts()
    mapping = {row["barrid"]: row["ticker"] for row in mapping_df}
    tickers = sorted(mapping.values())

    # Barrid covariance matrix
    cov_mat = construct_covariance_matrix(date_, barrids)

    # Ticker covariance matrix
    cov_mat_rekeyed = (
        cov_mat
        # Rekey columns
        .rename(mapping)
        # Rekey barrid column
        .with_columns(pl.col("barrid").replace(mapping))
        .rename({"barrid": "ticker"})
        # Resort
        .sort("ticker")
        .select("ticker", *tickers)
    )

    return cov_mat_rekeyed

def upload_to_s3(df: pl.DataFrame, date_: date) -> None:
    bucket_name = 'barra-covariance-matrices'
    model = "USSLOW"
    file_name = f"{model}/{model}_{date_}.parquet"

    s3.upload_df_to_s3(
        df=df,
        bucket_name=bucket_name,
        file_name=file_name
    )


def covariance_daily_flow() -> None:
    date_ = get_last_market_date()[0]
    print(date_)
    df = get_covariance_matrix(date_)
    upload_to_s3(df, date_)

if __name__ == "__main__":
    covariance_daily_flow()
