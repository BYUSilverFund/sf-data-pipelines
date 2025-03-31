from datetime import date
import polars as pl
import os
from tqdm import tqdm
from utils.tables import signals_clean, assets_clean
from utils import  merge_into_master

def compute_composite_alphas(start_date: date, end_date: date) -> pl.DataFrame:
    assets = (
        assets_clean
        # In date range
        .filter(pl.col('date').is_between(start_date, end_date))
        .select(
            'date', 
            'barrid', 
            pl.col('return')
        )
    )

    signals = (
        signals_clean
        # In date range
        .filter(pl.col('date').is_between(start_date, end_date))
        .select('date', 'barrid', pl.col('name').alias('signal'), 'alpha')
    )

    weights = (
        pl.scan_parquet("data/active_weights/active_weights_*.parquet")
        # In date range
        .filter(pl.col('date').is_between(start_date, end_date))
    )

    signal_portfolios = (
        assets
        # Join weights
        .join(
            weights,
            on=['date', 'barrid'],
            how='left'
        )
        # Leverage scaling
        .with_columns(
            pl.col('weight').truediv(pl.col('weight').abs().sum()).over(['barrid', 'signal'])
        )
        # Lag weights
        .with_columns(
            pl.col('weight').shift(1).over(['barrid', 'signal'])
        )
        # Aggregate to date/signal level
        .group_by(['date', 'signal'])
        .agg(
            pl.col('return').mul(pl.col('weight')).sum()
        )
    )

    risk_parity_weights = (
        signal_portfolios
        # Sort before rolling function
        .sort(['signal', 'date'])
        # 22 day volatility
        .with_columns(
            pl.col('return')
            .rolling_std(window_size=22)
            .over('signal')
            .alias('volatility')
        )
        # Inverse volatility weighting scheme
        .with_columns(
            pl.lit(1).truediv(pl.col('volatility')).over('date').alias('weight')
        )
    )
    
    composite_alphas = (
        signals
        # Join risk parity weights (each signal is unit volatility)
        .join(
            risk_parity_weights,
            on=['date', 'signal'],
            how='left'
        )
        # Drop null rows
        .drop_nulls('weight')
        # Aggregate to date/barrid level
        .group_by(['date', 'barrid'])
        .agg(
            pl.col('weight').mul(pl.col('alpha')).sum().alias('alpha')
        )
        .sort(['barrid', 'date'])
        .select(
            'date', 'barrid', pl.lit('risk_parity').alias('name'), 'alpha'
        )
        .collect()
    )

    return composite_alphas

def risk_parity_history_flow(start_date: date, end_date: date) -> None:
    os.makedirs("data/composite_alphas", exist_ok=True)

    composite_alphas = compute_composite_alphas(start_date, end_date)

    # Get years
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="Backfilling"):
        master_file = f"data/composite_alphas/composite_alphas_{year}.parquet"

        year_df = composite_alphas.filter(pl.col("date").dt.year().eq(year))

        # Merge
        if os.path.exists(master_file):
            merge_into_master(master_file, year_df,  on=['date', 'barrid'], how='full')

        # or Create
        else:
            year_df.write_parquet(master_file)



if __name__ == "__main__":
    # ----- History Flow -----
    risk_parity_history_flow(start_date=date(2025, 2, 1), end_date=date.today())

    # ----- Print -----
    print(pl.read_parquet("data/composite_alphas/composite_alphas_*.parquet"))
