from datetime import date
import polars as pl
from tqdm import tqdm
from pipelines.utils.views import in_universe_assets, in_universe_signals
from pipelines.utils.tables import Database

def compute_composite_alphas(start_date: date, end_date: date, database: Database) -> pl.DataFrame:
    assets = in_universe_assets(database).filter(
        pl.col("date").is_between(start_date, end_date)
    ).select("date", "barrid", pl.col("return"))
    print(assets.collect())

    signals = in_universe_signals(database).filter(
        pl.col("date").is_between(start_date, end_date)
    ).select("date", "barrid", pl.col("name").alias("signal"), "alpha")
    print(signals.collect())

    weights = database.active_weights_table.read().filter(
        pl.col("date").is_between(start_date, end_date)
    )
    print(weights.collect())

    signal_portfolios = (
        assets.join(weights, on=["date", "barrid"], how="left")
        # Constrain the leverage of each signal portfolio to be one.
        .with_columns(
            pl.col("weight")
            .truediv(pl.col("weight").abs().sum())
            .over(["barrid", "signal"])
        )
        # Lag weights
        .with_columns(pl.col("weight").shift(1).over(["barrid", "signal"]))
        .group_by(["date", "signal"])
        # Compute the return of each signal portfolio
        .agg(pl.col("return").mul(pl.col("weight")).sum())
    )
    print(signal_portfolios.collect())

    risk_parity_weights = (
        signal_portfolios.sort(["signal", "date"])
        # Find the 22 day volatility of each signal portfolio
        .with_columns(
            pl.col("return")
            .rolling_std(window_size=22)
            .over("signal")
            .alias("volatility")
        )
        # Use the inverse of the volatility as the weight
        .with_columns(
            pl.lit(1).truediv(pl.col("volatility")).over("date").alias("weight")
        )
        # Constraint weights to sum to one
        .with_columns(pl.col("weight").truediv(pl.col("weight").sum()).over("date"))
    )
    print(risk_parity_weights.collect())

    composite_alphas = (
        signals.join(risk_parity_weights, on=["date", "signal"], how="left")
        .drop_nulls("weight")
        .group_by(["date", "barrid"])
        # Compute risk parity weighted alphas
        .agg(pl.col("weight").mul(pl.col("alpha")).sum().alias("alpha"))
        .sort(["barrid", "date"])
        .select("date", "barrid", pl.lit("risk_parity").alias("name"), "alpha")
        .collect()
    )

    print(composite_alphas)

    return composite_alphas


def risk_parity_history_flow(start_date: date, end_date: date, database: Database) -> None:
    composite_alphas = compute_composite_alphas(start_date, end_date, database)

    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Composite Alphas"):
        year_df = composite_alphas.filter(pl.col("date").dt.year().eq(year))

        database.composite_alphas_table.create_if_not_exists(year)
        database.composite_alphas_table.upsert(year, year_df)
