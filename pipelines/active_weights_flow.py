from datetime import date
import polars as pl
import os
from tqdm import tqdm
from utils.tables import assets_clean
from system.signals import momentum, beta, reversal
from utils import get_last_market_date, merge_into_master
from system.records import Alpha
from functools import partial
from system.portfolios import mean_variance_efficient
from system.constraints import zero_beta

def get_alphas(signal: str, start_date: date, end_date: date) -> pl.DataFrame:
    return (
        pl.scan_parquet("data/signals/signals_*.parquet")
        .filter(pl.col('date').is_between(start_date, end_date))
        .filter(pl.col('name').eq(signal))
        .with_columns(
            pl.col('alpha').fill_null(0) # TODO: Check this...
        )
        .select('date', 'barrid', 'alpha')
        .sort(['barrid', 'date'])
        .collect()
    )


def active_weights_history_flow(start_date: date, end_date: date) -> None:
    os.makedirs("data/active_weights", exist_ok=True)

    signal_fns = [momentum, beta, reversal]
    signal_names = sorted([fn.__name__ for fn in signal_fns])

    for signal_name in tqdm(signal_names, desc="Computing active weights"):
        alphas = get_alphas(signal_name, start_date, end_date)

        periods = alphas['date'].unique().sort().to_list()

        for period in tqdm(periods, desc=f"{signal_name.replace('_', ' ').title()}"):

            period_alphas = (
                alphas
                # Filter to period
                .filter(pl.col('date').eq(period))
                .sort('barrid')
            )

            period_barrids = period_alphas['barrid'].unique().sort().to_list()

            period_portfolio = mean_variance_efficient(
                period=period,
                barrids=period_barrids,
                alphas=Alpha(period_alphas),
                constraints=[zero_beta],
                gamma=2
            )

            period_portfolio = (
                period_portfolio
                .with_columns(
                    pl.lit(signal_name).alias('signal')
                )
                .select('date', 'barrid', 'signal', 'weight')
            )
            print(period_portfolio)

            master_file = f"data/active_weights/active_weights_{period.year}.parquet"

            # Merge
            if os.path.exists(master_file):
                merge_into_master(master_file, period_portfolio, on=['date', 'barrid', 'signal'], how='full')

            # or Create
            else:
                period_portfolio.write_parquet(master_file)
    


if __name__ == "__main__":
    # ----- History Flow -----
    active_weights_history_flow(start_date=date(2025, 2, 1), end_date=date.today())

    # ----- Print -----
    print(pl.read_parquet("data/active_weights/active_weights_*.parquet"))
