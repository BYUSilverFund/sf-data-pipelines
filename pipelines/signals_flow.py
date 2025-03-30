from datetime import date
import polars as pl
import os
from tqdm import tqdm
from utils.tables import assets_clean
from system.signals import momentum, beta, reversal
from utils import get_last_market_date


def merge_into_master(master_file: str, df: pl.DataFrame) -> None:
    (
        # Scan master parquet file
        pl.scan_parquet(master_file)
        # Update
        .update(df.lazy(), on=["date", "barrid"], how="full")
        .collect()
        # Write
        .write_parquet(master_file)
    )


def signals_history_flow(start_date: date, end_date: date) -> None:
    os.makedirs("data/signals", exist_ok=True)

    data_start_date = min(get_last_market_date(start_date, 252)) # momentum signal

    signals = (
        assets_clean
        .filter(
            pl.col('date').is_between(data_start_date, end_date)
        )
        .select(
            'date', 
            'barrid',
            'ticker',
            'price', 
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

    # Filter to start_date, end_date
    signals = signals.filter(pl.col('date').is_between(start_date, end_date))

    # Get years
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Backfilling"):
        master_file = f"data/signals/signals_{year}.parquet"

        year_df = signals.filter(pl.col("date").dt.year().eq(year))

        # Merge
        if os.path.exists(master_file):
            merge_into_master(master_file, year_df)

        # or Create
        else:
            year_df.write_parquet(master_file)


if __name__ == "__main__":
    os.makedirs("data/signals", exist_ok=True)

    # ----- History Flow -----
    signals_history_flow(start_date=date(2025, 1, 1), end_date=date.today())

    # ----- Print -----
    print(pl.read_parquet("data/signals/signals_*.parquet"))
