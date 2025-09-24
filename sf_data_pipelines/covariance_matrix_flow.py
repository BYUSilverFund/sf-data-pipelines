import datetime as dt
import sf_quant.data as sfd
import utils.s3
import polars as pl

def covariance_matrix_daily_flow() -> None:
    yesterday = dt.date.today() - dt.timedelta(days=1)

    assets = (
        sfd.load_assets_by_date(
            date_=yesterday,
            in_universe=True,
            columns=['barrid', 'ticker']
        )
        .sort('barrid')
    )

    barrids = assets['barrid'].to_list()
    tickers = assets['ticker'].to_list()

    mapping = {barrid: ticker for barrid, ticker in zip(barrids, tickers)}

    covariance_matrix = (
        sfd.construct_covariance_matrix(
            date_=yesterday,
            barrids=barrids
        )
        .with_columns(
            pl.lit(yesterday).alias('date')
        )
        .rename({'barrid': 'ticker', **mapping})
        .with_columns(
            pl.col('ticker').replace(mapping)
        )
        .select('date', 'ticker', *sorted(tickers))
        .sort('ticker')
    )

    utils.s3.write_parquet(
        bucket_name='barra-covariance-matrices',
        file_name='latest.parquet',
        file_data=covariance_matrix,
    )