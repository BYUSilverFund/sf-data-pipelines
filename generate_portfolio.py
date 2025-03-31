import polars as pl
from pipelines.utils.tables import assets_clean, benchmark_weights
from pipelines.system.portfolios import mean_variance_efficient
from pipelines.system.records import Alpha
from pipelines.system.constraints import full_investment, no_buying_on_margin, long_only, unit_beta
from pipelines.utils import get_last_market_date
from pipelines.system.covariance_matrix import construct_covariance_matrix
import numpy as np

date_ = get_last_market_date()[0]
filter = True

composite_alphas = (
    pl.scan_parquet("data/composite_alphas/composite_alphas_*.parquet")
    .filter(pl.col('date').eq(date_))
    .filter(pl.col('name').eq('risk_parity'))
    .select(['date', 'barrid', 'alpha'])
    .sort('barrid')
    .collect()
)

print(composite_alphas)

if filter:
    filtered_barrids = pl.read_csv("barrid_back.csv")['barrid'].unique().sort().to_list()
    composite_alphas = composite_alphas.filter(pl.col('barrid').is_in(filtered_barrids)).sort('barrid')

print(composite_alphas)

barrids = composite_alphas['barrid'].unique().sort().to_list()

constraints = [
    full_investment,
    no_buying_on_margin,
    unit_beta,
    long_only,
]

weights = mean_variance_efficient(date_, barrids, Alpha(composite_alphas), constraints, gamma=.09)  # 0.0006

portfolio = (
    assets_clean
    .filter(pl.col('date').eq(date_))
    .select('date', 'barrid', 'ticker')
    .collect()
    .join(
        weights,
        on=['date', 'barrid'],
        how='left'
    )
    .filter(pl.col('weight').gt(0))
    .sort('weight', descending=True)
    .select('barrid', 'ticker', 'weight')
)

benchmark = (
    benchmark_weights
    .filter(pl.col('date').eq(date_))
    .select(
        # 'date',
        'barrid',
        pl.col('weight').alias('benchmark_weight')
    )
    .collect()
)

active_portfolio = (
    portfolio
    .join(
        benchmark,
        on=['barrid'],
        how='left'
    )
    .with_columns(
        pl.col('weight').sub(pl.col('benchmark_weight')).alias('active_weight')
    )
)

barrids = active_portfolio['barrid'].unique().sort().to_list()

covariance_matrix = construct_covariance_matrix(date_, barrids).drop('barrid').to_numpy()

active_weights = active_portfolio['active_weight'].to_numpy()

active_risk = np.sqrt(active_weights.T @ covariance_matrix @ active_weights)

print("Active Portfolio", active_portfolio.sort('active_weight', descending=True))
print(f"Active Risk: {active_risk:.2f}%")

# portfolio.write_csv('filtered_portfolio.csv')
# active_portfolio.write_csv('filtered_portfolio.csv')

