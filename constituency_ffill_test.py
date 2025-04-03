import polars as pl
from datetime import date
import matplotlib.pyplot as plt
import seaborn as sns

# Check that we are wrongly forward filling constituents
# print(
#     pl.scan_parquet("data/assets/assets_*.parquet")
#     .sort(['barrid', 'date'])
#     .with_columns(
#         pl.col("ticker", "russell_1000", "russell_2000").fill_null(strategy='forward').over('barrid')
#     )
#     .filter(pl.col('date').eq(date.today()))
#     .filter(pl.col('barrid').eq(pl.col('rootid')))
#     .filter(pl.col('russell_1000') | pl.col('russell_2000'))
#     .sort(['barrid', 'date'])
#     .select('date', 'barrid', "return", "cusip", "ticker", "russell_1000", "russell_2000", 'rebalance')
#     .collect()
# )

russell = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    # Filter
    .filter(pl.col('barrid').eq(pl.col('rootid')))
    .filter(pl.col('iso_country_code').eq("USA"))
    .filter(pl.col('russell_1000') | pl.col('russell_2000'))
    # Sort
    .sort(['barrid', 'date'])
    # Select
    .select('date', 'barrid', "ticker", "russell_1000", "russell_2000")
    # Collect
    .group_by('date')
    .agg(pl.col('barrid').count())
    .sort('date')
    .collect()
)

rebalance = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    # Filter
    .filter(pl.col('barrid').eq(pl.col('rootid')))
    .filter(pl.col('iso_country_code').eq("USA"))
    .filter(pl.col('russell_1000') | pl.col('russell_2000'))
    .select('date', pl.lit(True).alias('russell_rebalance'))
    .unique()
)

daily = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    .filter(pl.col('barrid').eq(pl.col('rootid')))
    .filter(pl.col('iso_country_code').eq("USA"))
    .join(
        rebalance,
        on='date',
        how='left'
    )
    .with_columns(
        pl.when(pl.col('russell_rebalance'))
        .then(pl.col('russell_1000', 'russell_2000').fill_null(False))
    )
    .sort(['barrid', 'date'])
    .with_columns(
        pl.col("ticker", "russell_1000", "russell_2000").fill_null(strategy='forward').over('barrid')
    )
    .filter(pl.col('russell_1000') | pl.col('russell_2000'))
    .sort(['barrid', 'date'])
    .select('date', 'barrid', 'cusip', 'ticker', 'price', 'return', 'russell_1000', 'russell_2000') # 'russell_rebalance'
    .group_by('date')
    .agg(pl.col('barrid').count())
    .sort('date')
    .collect()
)

# Note that russell dates are end of month

print(russell)
print(daily)

sns.lineplot(russell, x='date', y='barrid')
plt.savefig("russell.png", dpi=300)
plt.clf()

sns.lineplot(daily, x='date', y='barrid')
plt.savefig('forward_filled.png', dpi=300)