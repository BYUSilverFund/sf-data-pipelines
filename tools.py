import polars as pl
import exchange_calendars as xcals
from datetime import date

barra_columns = {
    '!Barrid': 'barrid',
    'Price': 'price',
    'Capt': 'market_cap',
    'PriceSource': 'price_source',
    'Currency': 'currency',
    'DlyReturn%': 'return',
    'DataDate': 'date',
    'AssetIDType': 'asset_id_type',
    'AssetID': 'asset_id',
    'StartDate': 'start_date',
    'EndDate': 'end_date'
}

barra_schema = {
    'DataDate': pl.String,
    '!Barrid': pl.String,
    'Price': pl.Float64,
    'Capt': pl.Float64,
    'PriceSource': pl.String,
    'Currency': pl.String,
    'DlyReturn%': pl.Float64,
    'AssetIDType': pl.String,
    'AssetID': pl.String,
    'StartDate': pl.String,
    'EndDate': pl.String
}


def get_last_market_date(n_days: int = 1) -> list[date]:
    df = (
        pl.from_pandas(xcals.get_calendar("XNYS").schedule)
        # Cast date types
        .with_columns(pl.col("close").cast(pl.Date).alias("date"))
        # Get previous date
        .with_columns(pl.col("date").shift(1).alias("previous_date"))
        # Filter
        .filter(pl.col("date").le(date.today()))
        # Sort
        .sort("date")["previous_date"]
        # Get last previous date
        .tail(n_days)
        .to_list()
    )

    return df