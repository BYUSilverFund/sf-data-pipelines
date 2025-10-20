import sf_quant.data as sfd
import polars as pl
import datetime as dt

print(sfd.get_assets_columns())

assets = sfd.load_assets(
    start=dt.date(2000, 1, 1),
    end=dt.date(2025, 12, 31),
    columns=['date', 'barrid', 'rootid', 'instrument', 'return', 'ticker', 'cusip', 'russell_1000', 'russell_2000', 'in_universe'],
    in_universe=False
)

print(assets)

print(
    assets
    .filter(
        pl.col('in_universe'),
        pl.col('date').eq(pl.col('date').max())
    )
    .sort('barrid', 'date')
)