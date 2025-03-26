import polars as pl 


assets = pl.read_parquet("data/assets/assets_*.parquet")

print(assets)