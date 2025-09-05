from barra_assets_flow import barra_assets_daily_flow
from barra_covariances_flow import barra_covariances_daily_flow, barra_covariances_history_flow
from barra_exposures_flow import barra_exposures_daily_flow, barra_exposures_history_flow
from barra_ids_flow import barra_ids_daily_flow
from barra_returns_flow import barra_returns_daily_flow, barra_returns_history_flow
from barra_risk_flow import barra_risk_daily_flow, barra_risk_history_flow
from ftse_russell_flow import ftse_russell_backfill_flow
from barra_specific_returns import barra_specific_returns_daily_flow, barra_specific_returns_history_flow
from barra_volume_flow import barra_volume_history_flow, barra_volume_daily_flow
from crsp_daily_flow import crsp_daily_backfill_flow
from crsp_monthly_flow import crsp_monthly_backfill_flow
from crsp_events_flow import crsp_events_backfill_flow
from barra_factors_flow import barra_factors_daily_flow
import datetime as dt
from utils.tables import Database

def barra_daily_flow(database: Database) -> None:
    # Assets table
    barra_returns_daily_flow(database)
    barra_specific_returns_daily_flow(database)
    barra_risk_daily_flow(database)
    barra_volume_daily_flow(database)

    # Covariance Matrix Components
    barra_exposures_daily_flow(database)
    barra_covariances_daily_flow(database)

    # Factors
    barra_factors_daily_flow(database)

def barra_history_flow(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    # Assets table
    barra_returns_history_flow(start_date, end_date, database)
    barra_specific_returns_history_flow(start_date, end_date, database)
    barra_risk_history_flow(start_date, end_date, database)
    barra_volume_history_flow(start_date, end_date, database)

    # Covariance Matrix Components
    barra_exposures_history_flow(start_date, end_date, database)
    barra_covariances_history_flow(start_date, end_date, database)

def id_mappings_flow(database: Database) -> None:
    barra_ids_daily_flow(database)
    barra_assets_daily_flow(database)

def ftse_history_flow(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    """Note: requires logging in to WRDS when running."""
    ftse_russell_backfill_flow(start_date, end_date, database)

def crsp_history_flow(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    """Note: requires logging in to WRDS when running."""
    crsp_events_backfill_flow(start_date, end_date, database)
    crsp_monthly_backfill_flow(start_date, end_date, database)
    crsp_daily_backfill_flow(start_date, end_date, database)

def barra_daily_pipeline(database: Database) -> None:
    barra_daily_flow(database)
    id_mappings_flow(database)

def barra_backfill_pipeline(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    barra_history_flow(start_date, end_date, database)
    id_mappings_flow(database)

def ftse_backfill_pipeline(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    ftse_history_flow(start_date, end_date, database)

def crsp_backfill_pipeline(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    crsp_history_flow(start_date, end_date, database)
