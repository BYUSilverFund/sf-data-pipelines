import polars as pl

weights_file = 'optimal_portfolio_weights.csv'

df = (
    pl.read_csv(weights_file)
    .join(
        pl.read_csv('ticker_map.csv'),
        on='barrid',
        how='left'
    )
    .select(['date', 'barrid', 'ticker', 'weight'])
)
print("RAW PORTFOLIO")
print(df)

df = (
    pl.read_csv(weights_file)
    .filter(pl.col('weight').gt(0))
    .join(
        pl.read_csv('ticker_map.csv'),
        on='barrid',
        how='left'
    )
    .select(['date', 'barrid', 'ticker', 'weight'])
)
print("NON ZERO WEIGHTS")
print(df)

df = (
    pl.read_csv(weights_file)
    .filter(pl.col('weight').gt(0))
    .join(
        pl.read_csv('ticker_map.csv'),
        on='barrid',
        how='left'
    )
    .select(pl.col('weight').sum())
)
print("NON ZERO WEIGHTS SUM")
print(df)
