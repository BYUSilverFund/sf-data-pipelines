import polars as pl
from pipelines.utils.tables import assets_table, signals_table

russell_rebalance_dates = (
    assets_table.read()
    # Standard filters
    .filter(pl.col('barrid').eq(pl.col('rootid')))
    .filter(pl.col('iso_country_code').eq("USA"))
    # Russell constituency filter
    .filter(pl.col('russell_1000') | pl.col('russell_2000'))
    # Create rebalance column
    .select('date', pl.lit(True).alias('russell_rebalance'))
    .unique()
)

in_universe_assets = (
    assets_table.read()
    # Standard filters
    .filter(pl.col('barrid').eq(pl.col('rootid')))
    .filter(pl.col('iso_country_code').eq("USA"))
    # Join rebalance dates
    .join(
        russell_rebalance_dates,
        on='date',
        how='left'
    )
    # Fill nulls with false on rebalance dates
    .with_columns(
        pl.when(pl.col('russell_rebalance'))
        .then(pl.col('russell_1000', 'russell_2000').fill_null(False))
    )
    # Sort before forward fill
    .sort(['barrid', 'date'])
    # Forward fill 
    .with_columns(
        pl.col("ticker", "russell_1000", "russell_2000").fill_null(strategy='forward').over('barrid')
    )
    # Russell constituency filter
    .filter(pl.col('russell_1000') | pl.col('russell_2000'))
    # Drop russell_rebalance column
    .drop("russell_rebalance")
    # Sort
    .sort(['barrid', 'date'])
)

universe = (
    in_universe_assets
    .select('date', 'barrid')
)

in_universe_signals = (
    universe.join(
        signals_table.read(),
        on=['date', 'barrid'],
        how='left'
    )

    .sort(['barrid', 'date'])
)

benchmark_weights = (
    in_universe_assets
    .select(
        'date', 
        'barrid', 
        pl.col('market_cap').truediv(pl.col('market_cap').sum()).over('date').alias('weight')
    )
    .sort(['barrid', 'date'])
)

market_calendar = (
    assets_table.read()
    .select('date')
    .unique()
    .sort('date')
)
