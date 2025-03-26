import polars as pl


assets_clean = (
    pl.scan_parquet("data/assets/assets_*.parquet")
    .filter(pl.col("rootid").eq(pl.col("barrid")))
    .with_columns(
        pl.col('ticker', 'russell_1000', 'russell_2000').fill_null(strategy='forward').over('barrid')
    )
    .filter(pl.col("russell_1000") | pl.col("russell_2000"))
)