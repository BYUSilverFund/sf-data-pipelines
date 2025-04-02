import polars as pl


assets_clean = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    .filter(pl.col("rootid").eq(pl.col("barrid")))
    .with_columns(
        pl.col('ticker', 'russell_1000', 'russell_2000').fill_null(strategy='forward').over('barrid')
    )
    .filter(pl.col("russell_1000") | pl.col("russell_2000"))
)

universe = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    .filter(pl.col("rootid").eq(pl.col("barrid")))
    .with_columns(
        pl.col('ticker', 'russell_1000', 'russell_2000').fill_null(strategy='forward').over('barrid')
    )
    .filter(pl.col("russell_1000") | pl.col("russell_2000"))
    .select('date', 'barrid')
)

benchmark_weights = (
    assets_clean
    .select(
        'date', 
        'barrid', 
        pl.col('market_cap').truediv(pl.col('market_cap').sum()).over('date').alias('weight')
    )
    .sort(['barrid', 'date'])
)

market_calendar = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    .select('date')
    .unique()
    .sort('date')
)

if __name__ == '__main__':
    print(
        pl.scan_parquet("data/signals/signals_*.parquet")
        .sort(['date', 'barrid'])
        .collect()
    )