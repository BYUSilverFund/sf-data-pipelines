from barra_assets_flow import barra_assets_daily_flow, barra_assets_backfill_flow
from barra_covariances_flow import (
    barra_covariances_daily_flow,
    barra_covariances_backfill_flow,
)
from barra_exposures_flow import (
    barra_exposures_daily_flow,
    barra_exposures_backfill_flow,
)
from barra_factors_flow import barra_factors_daily_flow, barra_factors_backfill_flow
from barra_ids_flow import barra_ids_daily_flow, barra_ids_backfill_flow
from barra_returns_flow import barra_returns_daily_flow, barra_returns_backfill_flow
from barra_risk_flow import barra_risk_daily_flow, barra_risk_backfill_flow
from barra_specific_returns_flow import (
    barra_specific_returns_daily_flow,
    barra_speicifc_returns_backfill_flow,
)
from ftse_russell_flow import ftse_russell_daily_flow, ftse_russell_backfill_flow

from datetime import date


def backfill_orchestration_flow(start_date: date, end_date: date) -> None:
    # Backfill complete ID space
    barra_returns_backfill_flow(start_date, end_date)
    barra_ids_backfill_flow(start_date, end_date)
    ftse_russell_backfill_flow(start_date, end_date)

    # Assets table
    barra_assets_backfill_flow(start_date, end_date)
    barra_speicifc_returns_backfill_flow(start_date, end_date)
    barra_risk_backfill_flow(start_date, end_date)

    # Covariances Matrix Tables
    barra_covariances_backfill_flow(start_date, end_date)
    barra_exposures_backfill_flow(start_date, end_date)

    # Factors Table
    barra_factors_backfill_flow(start_date, end_date)


def daily_orchestration_flow() -> None:
    # IDs
    barra_returns_daily_flow()
    barra_ids_daily_flow()
    ftse_russell_daily_flow()

    # Assets table
    barra_assets_daily_flow()
    barra_specific_returns_daily_flow()
    barra_risk_daily_flow()

    # Covariances Matrix Tables
    barra_covariances_daily_flow()
    barra_exposures_daily_flow()

    # Factors Table
    barra_factors_daily_flow()


if __name__ == "__main__":
    backfill_orchestration_flow(start_date=date(2025, 1, 1), end_date=date(2025, 3, 8))
