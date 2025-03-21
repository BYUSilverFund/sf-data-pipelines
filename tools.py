import polars as pl

barra_columns = {
    '!Barrid': 'barrid',
    'Price': 'price',
    'Capt': 'market_cap',
    'PriceSource': 'price_source',
    'Currency': 'currency',
    'DlyReturn%': 'return',
    'DataDate': 'date'
}

raw_schema = {
    'DataDate': pl.String,
    '!Barrid': pl.String,
    'Price': pl.Float64,
    'Capt': pl.Float64,
    'PriceSource': pl.String,
    'Currency': pl.String,
    'DlyReturn%': pl.Float64,
}

clean_schema = {
    'date': pl.Date,
    'barrid': pl.String,
    'price': pl.Float64,
    'market_cap': pl.Float64,
    'price_source': pl.String,
    'currency': pl.String,
    'return': pl.Float64,
}