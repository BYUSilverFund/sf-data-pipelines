from datetime import date
import polars as pl
from tqdm import tqdm
from pipelines.system.signals import momentum, beta, reversal
from pipelines.system.records import Alpha
from pipelines.system.portfolios import mean_variance_efficient
from pipelines.system.constraints import zero_beta
from pipelines.utils.tables import active_weights_table
from pipelines.utils.views import in_universe_signals

def get_alphas(signal: str, start_date: date, end_date: date) -> pl.DataFrame:
    return (
        in_universe_signals
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
    signal_fns = [momentum, beta, reversal]
    signal_names = sorted([fn.__name__ for fn in signal_fns])

    for signal_name in tqdm(signal_names, desc="Computing active weights"):
        alphas = get_alphas(signal_name, start_date, end_date)

        periods = alphas['date'].unique().sort().to_list()

        for period in tqdm(periods, desc=f"{signal_name.replace('_', ' ').title()}"):

            period_alphas = (
                alphas
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

            active_weights_table.create_if_not_exists(period.year)
            active_weights_table.upsert(period.year, period_portfolio)
    
