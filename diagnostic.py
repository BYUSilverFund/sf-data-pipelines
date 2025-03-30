import polars as pl
from pipelines.system.covariance_matrix import construct_covariance_matrix
import numpy as np

weights_file = 'optimal_portfolio_weights_gamma_100.csv'

df = (
    pl.read_csv(weights_file, try_parse_dates=True)
    .join(
        pl.read_csv('ticker_map.csv'),
        on='barrid',
        how='left'
    )
    .select(['date', 'barrid', 'ticker', 'weight'])
    .sort('weight', descending=True)
)
print("RAW PORTFOLIO")
print(df)

df = (
    pl.read_csv(weights_file, try_parse_dates=True)
    .with_columns(pl.col('weight').round(5))
    .filter(pl.col('weight').gt(0))
    .join(
        pl.read_csv('ticker_map.csv'),
        on='barrid',
        how='left'
    )
    .select(['date', 'barrid', 'ticker', 'weight'])
    .sort('weight', descending=True)
)
print("NON ZERO ROUND(5) WEIGHTS")
print(df)

sum_df = (
    pl.read_csv(weights_file, try_parse_dates=True)
    .filter(pl.col('weight').gt(0))
    .join(
        pl.read_csv('ticker_map.csv'),
        on='barrid',
        how='left'
    )
    .select(pl.col('weight').sum())
)
print("NON ZERO WEIGHTS SUM")
print(sum_df)

date_ = df['date'].max()
barrids = df['barrid'].unique().sort().to_list()
cov_mat = construct_covariance_matrix(date_, barrids)

print("COVARIANCE MATRIX")
print(cov_mat)

weights = df.select(['barrid', 'ticker', 'weight']).sort('barrid')
print("WEIGHTS")
display_weights = weights.sort('weight', descending=True)
display_weights.write_csv("pre_filter_portfolio_gamma_100.csv")
print(display_weights)

cov_mat_np = cov_mat.drop('barrid').to_numpy()
weights_np = weights['weight'].to_numpy()
active_risk = np.sqrt(weights_np.T @ cov_mat_np @ weights_np)

print("ACTIVE RISK")
print(f"{active_risk:.2f}%")
