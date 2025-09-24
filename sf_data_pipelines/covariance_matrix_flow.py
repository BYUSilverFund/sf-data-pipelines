import datetime as dt
import sf_quant.data as sfd
import utils.s3
import polars as pl

def covariance_matrix_daily_flow() -> None:
    yesterday = dt.date.today() - dt.timedelta(days=1)

    barrids = (
        sfd.load_assets_by_date(
            date_=yesterday,
            in_universe=True,
            columns='barrid'
        )
        .sort('barrid')
        ['barrid']
        .to_list()
    )

    covariance_matrix = (
        sfd.construct_covariance_matrix(
            date_=yesterday,
            barrids=barrids
        )
        .with_columns(
            pl.lit(yesterday).alias('date')
        )
    )

    utils.s3.write_parquet(
        bucket_name='barra-covariance-matrices',
        file_name='latest.parquet',
        file_data=covariance_matrix,
    )