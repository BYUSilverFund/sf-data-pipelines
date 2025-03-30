from datetime import date
import polars as pl
import os
from tqdm import tqdm
from utils.tables import assets_clean
from system.signals import momentum, beta, reversal
from utils import get_last_market_date, merge_into_master

def compute_signals(start_date: date, end_date: date) -> pl.DataFrame:
    data_start_date = min(get_last_market_date(start_date, 252)) # momentum signal lookback

    signals = (
        assets_clean
        .filter(
            pl.col('date').is_between(data_start_date, end_date)
        )
        .select(
            'date', 
            'barrid',
            'return', 
            'predicted_beta', 
        )
        .sort(['barrid', 'date'])
        .collect()
    )

    signal_fns = [momentum, beta, reversal]
    signal_names = sorted([fn.__name__ for fn in signal_fns])

    # ----- Compute signals -----
    for signal_fn in signal_fns:
        signals = signal_fn(signals)

    signals = (
        signals
        # Select columns
        .select(
            ["date", "barrid"] + signal_names
        )
        # Filter to start_date, end_Date
        .filter(pl.col('date').is_between(start_date, end_date))
        # Put in signals long format
        .unpivot(index=['date', 'barrid'], on=signal_names, variable_name='name', value_name='signal')
        # Sort
        .sort(['barrid', 'date', 'name'])
    )

    # ----- Compute Scores -----
    signals = (
        signals
        .with_columns(
            pl.col('signal')
            .sub(pl.col('signal').mean())
            .truediv(pl.col('signal').std())
            .over(['date', 'name'])
            .alias('score')
        )
        .select(['date', 'barrid', 'name', 'signal', 'score'])
    )

    # ----- Compute Alphas -----

    specific_risk = (
        assets_clean
        # Get specific risk data
        .filter(pl.col('date').is_between(start_date, end_date))
        .select('date', 'barrid', 'specific_risk')
        .sort(['barrid', 'date'])
        .collect()
    )

    signals = (
        signals
        .join(
            specific_risk,
            on=['date', 'barrid'],
            how='left'
        )
        .with_columns(
            pl.col('score').mul(.05).mul('specific_risk').alias('alpha')
        )
        .select(['date', 'barrid', 'name', 'signal', 'score', 'alpha'])
    )

    return signals


def signals_history_flow(start_date: date, end_date: date) -> None:
    os.makedirs("data/signals", exist_ok=True)
    signals = compute_signals(start_date, end_date)

    # Get years
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Backfilling"):
        master_file = f"data/signals/signals_{year}.parquet"

        year_df = signals.filter(pl.col("date").dt.year().eq(year))

        # Merge
        if os.path.exists(master_file):
            merge_into_master(master_file, year_df,  on=['date', 'barrid', 'name'], how='full')

        # or Create
        else:
            year_df.write_parquet(master_file)


if __name__ == "__main__":
    # ----- History Flow -----
    signals_history_flow(start_date=date(2025, 1, 1), end_date=date.today())

    # ----- Print -----
    print(pl.read_parquet("data/signals/signals_*.parquet"))
