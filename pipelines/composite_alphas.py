from datetime import date
import polars as pl
from tqdm import tqdm
from pipelines.utils.views import in_universe_assets, in_universe_signals
from pipelines.utils.tables import composite_alphas_table, active_weights_table


def compute_composite_alphas(start_date: date, end_date: date) -> pl.DataFrame:
    assets = in_universe_assets.filter(
        pl.col("date").is_between(start_date, end_date)
    ).select("date", "barrid", pl.col("return"))

    signals = in_universe_signals.filter(
        pl.col("date").is_between(start_date, end_date)
    ).select("date", "barrid", pl.col("name").alias("signal"), "alpha")

    weights = active_weights_table.read().filter(
        pl.col("date").is_between(start_date, end_date)
    )

    signal_portfolios = (
        assets.join(weights, on=["date", "barrid"], how="left")
        .with_columns(
            pl.col("weight")
            .truediv(pl.col("weight").abs().sum())
            .over(["barrid", "signal"])
        )
        .with_columns(pl.col("weight").shift(1).over(["barrid", "signal"]))
        .group_by(["date", "signal"])
        .agg(pl.col("return").mul(pl.col("weight")).sum())
    )

    risk_parity_weights = (
        signal_portfolios.sort(["signal", "date"])
        .with_columns(
            pl.col("return")
            .rolling_std(window_size=22)
            .over("signal")
            .alias("volatility")
        )
        .with_columns(
            pl.lit(1).truediv(pl.col("volatility")).over("date").alias("weight")
        )
        .with_columns(pl.col("weight").truediv(pl.col("weight").sum()).over("date"))
    )

    composite_alphas = (
        signals.join(risk_parity_weights, on=["date", "signal"], how="left")
        .drop_nulls("weight")
        .group_by(["date", "barrid"])
        .agg(pl.col("weight").mul(pl.col("alpha")).sum().alias("alpha"))
        .sort(["barrid", "date"])
        .select("date", "barrid", pl.lit("risk_parity").alias("name"), "alpha")
        .collect()
    )

    return composite_alphas


def risk_parity_history_flow(start_date: date, end_date: date) -> None:
    composite_alphas = compute_composite_alphas(start_date, end_date)

    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Composite Alphas"):
        year_df = composite_alphas.filter(pl.col("date").dt.year().eq(year))

        composite_alphas_table.create_if_not_exists(year)
        composite_alphas_table.upsert(year, year_df)
