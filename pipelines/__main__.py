from barra_assets_flow import barra_assets_daily_flow
from barra_covariances_flow import barra_covariances_daily_flow, barra_covariances_history_flow
from barra_exposures_flow import barra_exposures_daily_flow, barra_exposures_history_flow
from barra_ids_flow import barra_ids_daily_flow
from barra_returns_flow import barra_returns_daily_flow, barra_returns_history_flow
from barra_risk_flow import barra_risk_daily_flow, barra_risk_history_flow
from ftse_russell_flow import ftse_russell_backfill_flow
from signals_flow import signals_history_flow
from active_weights_flow import active_weights_history_flow
from barra_specific_returns import barra_specific_returns_daily_flow, barra_specific_returns_history_flow
from pipelines.composite_alphas import risk_parity_history_flow
from barra_volume_flow import barra_volume_history_flow, barra_volume_daily_flow
from crsp_daily_flow import crsp_daily_backfill_flow
from crsp_monthly_flow import crsp_monthly_backfill_flow
from crsp_events_flow import crsp_events_backfill_flow
from barra_factors_flow import barra_factors_daily_flow
from covariance_matrix_flow import covariance_daily_flow
from datetime import date

def barra_daily_flow() -> None:
    # Assets table
    barra_returns_daily_flow()
    barra_specific_returns_daily_flow()
    barra_risk_daily_flow()
    barra_volume_daily_flow()

    # Covariance Matrix Components
    barra_exposures_daily_flow()
    barra_covariances_daily_flow()

    # Factors
    barra_factors_daily_flow()

def barra_history_flow(start_date: date, end_date: date) -> None:
    # Assets table
    barra_returns_history_flow(start_date, end_date)
    barra_specific_returns_history_flow(start_date, end_date)
    barra_risk_history_flow(start_date, end_date)
    barra_volume_history_flow(start_date, end_date)

    # Covariance Matrix Components
    barra_exposures_history_flow(start_date, end_date)
    barra_covariances_history_flow(start_date, end_date)

def id_mappings_flow() -> None:
    barra_ids_daily_flow()
    barra_assets_daily_flow()

def ftse_history_flow(start_date: date, end_date: date) -> None:
    ftse_russell_backfill_flow(
        start_date=start_date,
        end_date=end_date,
    )

def crsp_history_flow(start_date: date, end_date: date) -> None:
    crsp_events_backfill_flow(start_date, end_date)
    crsp_monthly_backfill_flow(start_date, end_date)
    crsp_daily_backfill_flow(start_date, end_date)

def strategy_backfill_flow(start_date: date, end_date: date) -> None:
    signals_history_flow(start_date, end_date)
    active_weights_history_flow(start_date, end_date)
    risk_parity_history_flow(start_date, end_date)

def daily_etl_pipeline() -> None:
    # date_ = date.today() - timedelta(days=1)
    barra_daily_flow()
    id_mappings_flow()
    covariance_daily_flow()
    # strategy_backfill_flow(date_, date_)
    
if __name__ == '__main__':
    # start_date = date(1995, 1, 1)
    # end_date = date.today()

    # barra_history_flow(start_date, end_date)
    daily_etl_pipeline()
    # ftse_history_flow(start_date, end_date)

