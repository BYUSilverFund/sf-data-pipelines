import polars as pl

BARRA_COLUMN_NAMES = {
    "!Barrid": "barrid",
    "Price": "price",
    "Capt": "market_cap",
    "PriceSource": "price_source",
    "Currency": "currency",
    "DlyReturn%": "return",
    "DataDate": "date",
}

BARRA_SCHEMA = {
    "barrid": pl.Categorical,
    "price": pl.Float64,
    "market_cap": pl.Float64,
    "price_source": pl.String,
    "currency": pl.Categorical,
    "return": pl.Float64,
    "date": pl.Date,
}


def rename_barra_columns(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Utility function for renaming all barra columns."""
    return raw_df.rename(BARRA_COLUMN_NAMES, strict=False)


def cast_barra_columns(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Utility function for casting dataframe to barra schema."""
    return raw_df.with_columns(
        pl.col("date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d")
    ).cast(BARRA_SCHEMA, strict=False)
