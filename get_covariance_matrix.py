import sf_quant.data as sfd
import datetime as dt
from rich import print
import polars as pl

grad_fund_tickers = sorted([
    "AAPL",
    "CMG",
    "COKE",
    "KLAC",
    # "MELI",
    "NFLX",
    "RMD",
    "RSG",
    "CAKE",
    "TXRH",
    "LDOS",
    "LEN",
    "PR",
    "TKO",
    "DELL",
    "OC",
    "MA",
    "CELH",
    "APH",
    "CCL",
    "LII",
    "MANH",
    "ODC",
    "WFRD",
    # "IWV"
])

date_ = dt.date(2025, 9, 15)

assets = (
    sfd.load_assets_by_date(
        date_=date_,
        in_universe=True,
        columns=['barrid', 'ticker', 'cusip']
    )
    .sort('ticker')
)

print(assets)

barrids = assets['barrid'].to_list()
tickers = assets['ticker'].to_list()

mapping = {barrid: ticker for barrid, ticker in zip(barrids, tickers)}

cov_mat = sfd.construct_covariance_matrix(
    date_=date_,
    barrids=barrids
)

print(cov_mat)

cov_mat_with_tickers = (
    cov_mat
    .rename({'barrid': 'ticker', **mapping})
    .with_columns(
        pl.col('ticker').replace(mapping)
    )
    .filter(
        pl.col('ticker').is_in(grad_fund_tickers)
    )
    .select('ticker', *sorted(grad_fund_tickers))
    .sort('ticker')
)

print(cov_mat_with_tickers)

# cov_mat_with_tickers.write_csv(f"grad_fund_cov_mat_{date_}.csv")
