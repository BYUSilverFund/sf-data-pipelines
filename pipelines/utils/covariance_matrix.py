from database import Database
from datetime import date
import polars as pl
import numpy as np
from factors import factors

def get_barrids_by_date(date_: date) -> list[str]:
    with Database() as db:
        # Get barrids for date
        barrids = db.execute(
            f"""
            SELECT distinct barrid FROM assets_clean WHERE date = '{date_}';
            """
        ).pl()

        # Sort
        barrids = barrids['barrid'].sort().to_list()

        return barrids
    
def construct_covariance_matrix(date_: date, barrids: list[str] | None = None) -> pl.DataFrame:
    """
    Constructs the covariance matrix based on exposures, factor covariances, and specific risks.

    Args:
        date_ (date): The date for which the covariance matrix is computed.
        barrids (List[str]): List of Barrid identifiers for the assets (optional).

    Returns:
        CovarianceMatrix: The computed covariance matrix wrapped in a CovarianceMatrix object.
    """
    barrids = barrids or get_barrids_by_date(date_)

    # Construct covariance matrix components
    exposures_matrix = construct_exposure_matrix(barrids, date_).drop('barrid').to_numpy()
    factor_covariance_matrix = construct_factor_covariance_matrix(date_).drop('factor1').to_numpy()
    specific_risk_matrix = construct_specific_risk_matrix(barrids, date_).drop('barrid').to_numpy()

    # Construct asset covariance matrix
    covariance_matrix = exposures_matrix @ factor_covariance_matrix @ exposures_matrix.T + specific_risk_matrix

    # Put in decimal space
    covariance_matrix = covariance_matrix / (100**2)

    # Package
    covariance_matrix = pl.DataFrame(
        {
            "barrid": barrids,
            **{id: covariance_matrix[:, i] for i, id in enumerate(barrids)},
        }
    )

    return covariance_matrix


def construct_exposure_matrix(barrids: list[str], date_: date) -> pl.DataFrame:
    """
    Constructs the factor exposure matrix for the given date and Barrids.

    Args:
        date_ (date): The date for which the factor exposure matrix is computed.
        barrids (List[str]): List of Barrid identifiers for the assets.

    Returns:
        pl.DataFrame: The factor exposure matrix.
    """
    factors_str = ", ".join(sorted(factors))
    barrids_str = ", ".join([f"'{barrid}'" for barrid in barrids])
    with Database() as db:
        df = db.execute(
            f"""
            SELECT barrid, {factors_str}
            FROM exposures_wide
            WHERE date = '{date_}'
                AND barrid IN ({barrids_str})
            ORDER By barrid, date
            ;
            """
        ).pl()

        df = df.fill_null(0)

        return df
    
def construct_factor_covariance_matrix(date_: date) -> pl.DataFrame:
    """
    Constructs the factor covariance matrix for the given date.

    Args:
        date_ (date): The date for which the factor covariance matrix is computed.

    Returns:
        pl.DataFrame: The factor covariance matrix.
    """
    factors_str = ", ".join(sorted(factors))
    with Database() as db:
        df = db.execute(
            f"""
            SELECT factor1, {factors_str}
            FROM covariances_wide
            WHERE date = '{date_}'
            ORDER By factor1, date
            ;
            """
        ).pl()

        # Convert from upper triangular to symetric
        upper_triangular_matrix = df.drop("factor1").to_numpy()
        covariance_matrix = np.where(np.isnan(upper_triangular_matrix), upper_triangular_matrix.T, upper_triangular_matrix)

        # Package
        covariance_matrix = pl.DataFrame(
            {
                "factor1": sorted(factors),
                **{col: covariance_matrix[:, idx] for idx, col in enumerate(sorted(factors))},
            }
        )

        # Fill NaN (from Barra)
        covariance_matrix = covariance_matrix.fill_nan(0)

        return covariance_matrix
    
def construct_specific_risk_matrix(barrids: list[str], date_: date) -> pl.DataFrame:
    """
    Constructs the specific risk matrix for the given date and Barrids.

    Args:
        barrids (List[str]): List of Barrid identifiers for the assets.
        date_ (date): The date for which the specific risk matrix is computed.

    Returns:
        pl.DataFrame: The specific risk matrix.
    """
    barrids_str = ", ".join([f"'{barrid}'" for barrid in barrids])
    with Database() as db:
        df = db.execute(
            f"""
            SELECT barrid, specific_risk
            FROM assets_clean
            WHERE date = '{date_}'
                AND barrid IN ({barrids_str})
            ORDER By barrid
            ;
            """
        ).pl()

        # Fill null values with 0
        df = df.fill_null(0)

        # Convert vector to diagonal matrix
        diagonal_matrix = np.power(np.diag(df["specific_risk"]), 2)

        # Package
        specific_risk_matrix = pl.DataFrame(
            {
                "barrid": barrids,
                **{id: diagonal_matrix[:, i] for i, id in enumerate(barrids)},
            }
        )

        return specific_risk_matrix
    

if __name__ == '__main__':
    # date_ = date(2025, 3, 7)
    # cov_mat = construct_covariance_matrix(date_)
    # print(cov_mat)
    from database import Database

    with Database() as db:
        df = db.execute(
"""
    SELECT 
        barrid,
        ticker,
        price
    FROM assets_clean 
    WHERE date >= '2025-01-31' 
        AND (russell_1000 OR russell_2000) 
    ORDER BY date, barrid;
"""
        ).pl()

        print(df)

    
