from datetime import date
import polars as pl
from tqdm import tqdm
from pipelines.utils.views import in_universe_assets
from pipelines.system.signals import momentum, beta, reversal
from pipelines.utils.tables import Database
from pipelines.utils import get_last_market_date
from pipelines.utils.tables import Database


def compute_signals(start_date: date, end_date: date, database: Database) -> pl.DataFrame:
    data_start_date = min(get_last_market_date(start_date, 252)) or date(
        1995, 6, 1
    )  # momentum signal lookback

    signals = (
        in_universe_assets(database).filter(pl.col("date").is_between(data_start_date, end_date))
        .select(
            "date",
            "barrid",
            "return",
            "predicted_beta",
        )
        .sort(["barrid", "date"])
        .collect()
    )

    signal_fns = [momentum, beta, reversal]
    signal_names = sorted([fn.__name__ for fn in signal_fns])

    # ----- Compute signals -----
    for signal_fn in signal_fns:
        signals = signal_fn(signals)

    signals = (
        signals.select(["date", "barrid"] + signal_names)
        .filter(pl.col("date").is_between(start_date, end_date))
        .unpivot(
            index=["date", "barrid"],
            on=signal_names,
            variable_name="name",
            value_name="signal",
        )
        .sort(["barrid", "date", "name"])
    )

    # ----- Compute Scores -----
    signals = signals.with_columns(
        pl.col("signal")
        .sub(pl.col("signal").mean())
        .truediv(pl.col("signal").std())
        .over(["date", "name"])
        .alias("score")
    ).select(["date", "barrid", "name", "signal", "score"])

    # ----- Compute Alphas -----
    specific_risk = (
        in_universe_assets(database).filter(pl.col("date").is_between(start_date, end_date))
        .select("date", "barrid", "specific_risk")
        .sort(["barrid", "date"])
        .collect()
    )

    signals = (
        signals.join(specific_risk, on=["date", "barrid"], how="left")
        .with_columns(pl.col("score").mul(0.05).mul("specific_risk").alias("alpha"))
        .select(["date", "barrid", "name", "signal", "score", "alpha"])
    )

    return signals


def signals_history_flow(start_date: date, end_date: date, database: Database) -> None:
    signals = compute_signals(start_date, end_date, database)

    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Signals"):
        year_df = signals.filter(pl.col("date").dt.year().eq(year))

        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, year_df)
