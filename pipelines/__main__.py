from barra_assets_flow import barra_assets_daily_flow
from barra_covariances_flow import barra_covariances_daily_flow, barra_covariances_history_flow
from barra_exposures_flow import barra_exposures_daily_flow, barra_exposures_history_flow
from barra_ids_flow import barra_ids_daily_flow
from barra_returns_flow import barra_returns_daily_flow, barra_returns_history_flow
from barra_risk_flow import barra_risk_daily_flow, barra_risk_history_flow
from ftse_russell_flow import ftse_russell_backfill_flow
from datetime import date
import polars as pl
from pipelines.utils import get_last_market_date

def daily_flow(look_back: int = 5) -> None:
    current_date = date.today()
    look_back_date = min(get_last_market_date(n_days=look_back))
    
    # Assets table
    barra_returns_daily_flow()
    barra_ids_daily_flow()
    barra_assets_daily_flow()
    barra_risk_daily_flow()
    ftse_russell_backfill_flow(
        start_date=look_back_date,
        end_date=current_date,
    )

    # Covariance Matrix Components
    barra_exposures_daily_flow()
    barra_covariances_daily_flow()

def history_flow(start_date: date, end_date: date) -> None:
    # Assets table
    barra_returns_history_flow(start_date, end_date)
    barra_ids_daily_flow()
    barra_assets_daily_flow()
    barra_risk_history_flow(start_date, end_date)
    ftse_russell_backfill_flow(start_date, end_date)

    # Covariance Matrix Components
    barra_exposures_history_flow(start_date, end_date)
    barra_covariances_history_flow(start_date, end_date)


if __name__ == '__main__':
    history_flow(
        start_date=date(2024, 1, 1),
        end_date=date.today()
    )

    daily_flow()

    df = pl.read_parquet("data/assets/assets_*.parquet")

    print(df)
