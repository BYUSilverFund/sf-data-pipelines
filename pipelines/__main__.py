from barra_assets_flow import barra_assets_daily_flow
from barra_covariances_flow import barra_covariances_daily_flow, barra_covariances_history_flow
from barra_exposures_flow import barra_exposures_daily_flow, barra_exposures_history_flow
from barra_ids_flow import barra_ids_daily_flow
from barra_returns_flow import barra_returns_daily_flow, barra_returns_history_flow
from barra_risk_flow import barra_risk_daily_flow, barra_risk_history_flow
from ftse_russell_flow import ftse_russell_backfill_flow
from signals_flow import signals_history_flow
from active_weights_flow import active_weights_history_flow
from pipelines.composite_alphas import risk_parity_history_flow
from datetime import date

def daily_flow() -> None:
    # Assets table
    barra_returns_daily_flow()
    barra_ids_daily_flow()
    barra_assets_daily_flow()
    barra_risk_daily_flow()

    # Covariance Matrix Components
    barra_exposures_daily_flow()
    barra_covariances_daily_flow()

def history_flow(start_date: date, end_date: date) -> None:
    # Assets table
    # barra_returns_history_flow(start_date, end_date)
    # barra_ids_daily_flow()
    # barra_assets_daily_flow()
    # barra_risk_history_flow(start_date, end_date)

    # Covariance Matrix Components
    barra_exposures_history_flow(start_date, end_date)
    # barra_covariances_history_flow(start_date, end_date)

def wrds_history_flow(start_date: date, end_date: date) -> None:
    ftse_russell_backfill_flow(
        start_date=start_date,
        end_date=end_date,
    )

def strategy_backfill_flow(start_date: date, end_date: date) -> None:
    signals_history_flow(start_date, end_date)
    active_weights_history_flow(start_date, end_date)
    risk_parity_history_flow(start_date, end_date)
    
if __name__ == '__main__':
    start_date = date(2025, 2, 1)
    end_date = date.today()

    history_flow(start_date, end_date)

    # daily_flow()

    # wrds_history_flow(start_date, end_date)

    # strategy_backfill_flow(start_date, end_date)

