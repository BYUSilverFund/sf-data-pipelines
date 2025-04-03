from datetime import date
import polars as pl
import numpy as np
from pipelines.utils.factors import factors
import polars as pl
from pipelines.utils.tables import in_universe_assets

def get_barrids_by_date(date_: date) -> list[str]:

    barrids = (
        in_universe_assets
        .filter(pl.col('date').eq(date_))
        .select('barrid')
        .unique()
        .sort('barrid')
        .collect()
        ['barrid']
        .to_list()
    )

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
    factor_covariance_matrix = construct_factor_covariance_matrix(date_).drop('factor_1').to_numpy()
    specific_risk_matrix = construct_specific_risk_matrix(barrids, date_).drop('barrid').to_numpy()

    # Construct asset covariance matrix
    covariance_matrix = exposures_matrix @ factor_covariance_matrix @ exposures_matrix.T + specific_risk_matrix

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
    df = (
        pl.scan_parquet("data/exposures/exposures_*.parquet")
        .filter(pl.col('date').eq(date_))
        .filter(pl.col('barrid').is_in(barrids))
        .select(['barrid', *factors])
        .fill_null(0)
        .sort('barrid')
        .collect()
    )

    return df
    
def construct_factor_covariance_matrix(date_: date) -> pl.DataFrame:
    """
    Constructs the factor covariance matrix for the given date.

    Args:
        date_ (date): The date for which the factor covariance matrix is computed.

    Returns:
        pl.DataFrame: The factor covariance matrix.
    """

    df = (
        pl.scan_parquet("data/covariances/covariances_*.parquet")
        .filter(pl.col('date').eq(date_))
        .filter(pl.col('factor_1').is_in(factors))
        .select(['factor_1', *factors])
        .sort('factor_1')
        .collect()
    )

    # Convert from upper triangular to symetric
    upper_triangular_matrix = df.drop("factor_1").to_numpy()
    covariance_matrix = np.where(np.isnan(upper_triangular_matrix), upper_triangular_matrix.T, upper_triangular_matrix)

    # Package
    covariance_matrix = pl.DataFrame(
        {
            "factor_1": sorted(factors),
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

    df = (
        pl.scan_parquet("data/assets/assets_*.parquet")
        .filter(pl.col('date').eq(date_))
        .filter(pl.col('barrid').is_in(barrids))
        .select('barrid', 'specific_risk')
        .fill_null(0)
        .sort('barrid')
        .collect()
    )

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
    date_ = date(2025, 3, 25)
    barrids = get_barrids_by_date(date_)

    cov_mat = construct_covariance_matrix(date_, barrids)

    print(cov_mat)

    
