import polars as pl
import exchange_calendars as xcals
from datetime import date

barra_columns = {
    "!Barrid": "barrid",
    "Price": "price",
    "Capt": "market_cap",
    "PriceSource": "price_source",
    "Currency": "currency",
    "DlyReturn%": "return",
    "DataDate": "date",
    "AssetIDType": "asset_id_type",
    "AssetID": "assetid",
    "StartDate": "start_date",
    "EndDate": "end_date",
    "Name": "name",
    "Instrument": "instrument",
    "IssuerID": "issuerid",
    "ISOCountryCode": "iso_country_code",
    "ISOCurrencyCode": "iso_currency_code",
    "RootID": "rootid",
    "Yield%": "yield",
    "TotalRisk%": "total_risk",
    "SpecRisk%": "specific_risk",
    "HistBeta": "historical_beta",
    "PredBeta": "predicted_beta",
    "Factor": "factor",
    "Exposure": "exposures",
    "!Factor1": "factor_1",
    "Factor2": "factor_2",
    'VarCovar': 'covariance',
}

barra_schema = {
    "DataDate": pl.String,
    "!Barrid": pl.String,
    "Price": pl.Float64,
    "Capt": pl.Float64,
    "PriceSource": pl.String,
    "Currency": pl.String,
    "DlyReturn%": pl.Float64,
    "AssetIDType": pl.String,
    "AssetID": pl.String,
    "StartDate": pl.String,
    "EndDate": pl.String,
    "Name": pl.String,
    "Instrument": pl.String,
    "IssuerID": pl.String,
    "ISOCountryCode": pl.String,
    "ISOCurrencyCode": pl.String,
    "RootID": pl.String,
    "Yield%": pl.Float64,
    "TotalRisk%": pl.Float64,
    "SpecRisk%": pl.Float64,
    "HistBeta": pl.Float64,
    "PredBeta": pl.Float64,
    "Factor": pl.String,
    "Exposures": pl.Float64,
    "!Factor1": pl.String,
    "Factor2": pl.String,
    'VarCovar': pl.Float64
}

russell_columns = {
    "russell2000": "russell_2000",
    "russell1000": "russell_1000",
    "r3000_wt": "russell_3000_weight",
    "r2000_wt": "russell_2000_weight",
    "r1000_wt": "russell_1000_weight",
}

russell_schema = {
    "date": pl.Date,
    "ticker": pl.String,
    "cusip": pl.String,
    "russell2000": pl.String,
    "russell1000": pl.String,
    "r3000_wt": pl.Float64,
    "r2000_wt": pl.Float64,
    "r1000_wt": pl.Float64,
}


def get_last_market_date(current_date: date | None = None, n_days: int = 1) -> list[date]:
    current_date = current_date or date.today()

    df = (
        pl.from_pandas(xcals.get_calendar("XNYS").schedule)
        # Cast date types
        .with_columns(pl.col("close").cast(pl.Date).alias("date"))
        # Get previous date
        .with_columns(pl.col("date").shift(1).alias("previous_date"))
    
    )

    market_dates = df["date"].to_list()
    
    # If today is not a market date, use the next closest.
    if current_date not in market_dates:
        current_date = next(d for d in market_dates if d > current_date)

    df = (
        df
        # Filter
        .filter(pl.col("date").le(current_date))
        # Sort
        .sort("date")
        ["previous_date"]
        # Get last previous date
        .tail(n_days)
        .to_list()
    )

    return df


def merge_into_master(
    master_file: str, df: pl.DataFrame, on: list[str], how: str
) -> None:
    # Get master columns lazily
    master_columns = pl.scan_parquet(master_file).collect_schema().names()

    # Add missing columns
    missing_columns = set(df.columns) - set(master_columns)
    for col in missing_columns:
        dtype = df.schema[col]
        (
            pl.scan_parquet(master_file)
            .with_columns(pl.lit(None, dtype=dtype).alias(col))
            .collect()
            .write_parquet(master_file)
        )

    # Update rows
    (
        # Scan master parquet file
        pl.scan_parquet(master_file)
        # Update
        .update(df.lazy(), on=on, how=how)
        .collect()
        # Write
        .write_parquet(master_file)
    )


if __name__ == '__main__':
    print(get_last_market_date(n_days=5))
