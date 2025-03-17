from database import Database
from datetime import date
import polars as pl
import numpy as np

FACTORS = sorted([
    "USSLOWL_BETA",
    "USSLOWL_COUNTRY",
    "USSLOWL_DIVYILD",
    "USSLOWL_EARNQLTY",
    "USSLOWL_EARNYILD",
    "USSLOWL_GROWTH",
    "USSLOWL_LEVERAGE",
    "USSLOWL_LIQUIDTY",
    "USSLOWL_LTREVRSL",
    "USSLOWL_MGMTQLTY",
    "USSLOWL_MIDCAP",
    "USSLOWL_MOMENTUM",
    "USSLOWL_PROFIT",
    "USSLOWL_PROSPECT",
    "USSLOWL_SIZE",
    "USSLOWL_VALUE",
    "USSLOWL_AERODEF",
    "USSLOWL_AIRLINES",
    "USSLOWL_ALUMSTEL",
    "USSLOWL_APPAREL",
    "USSLOWL_AUTO",
    "USSLOWL_BANKS",
    "USSLOWL_BEVTOB",
    "USSLOWL_BIOLIFE",
    "USSLOWL_BLDGPROD",
    "USSLOWL_CHEM",
    "USSLOWL_CNSTENG",
    "USSLOWL_CNSTMACH",
    "USSLOWL_CNSTMATL",
    "USSLOWL_COMMEQP",
    "USSLOWL_COMPELEC",
    "USSLOWL_COMSVCS",
    "USSLOWL_CONGLOM",
    "USSLOWL_CONTAINR",
    "USSLOWL_DISTRIB",
    "USSLOWL_DIVFIN",
    "USSLOWL_ELECEQP",
    "USSLOWL_ELECUTIL",
    "USSLOWL_FOODPROD",
    "USSLOWL_FOODRET",
    "USSLOWL_GASUTIL",
    "USSLOWL_HLTHEQP",
    "USSLOWL_HLTHSVCS",
    "USSLOWL_HOMEBLDG",
    "USSLOWL_HOUSEDUR",
    "USSLOWL_INDMACH",
    "USSLOWL_INSURNCE",
    "USSLOWL_INTERNET",
    "USSLOWL_LEISPROD",
    "USSLOWL_LEISSVCS",
    "USSLOWL_LIFEINS",
    "USSLOWL_MEDIA",
    "USSLOWL_MGDHLTH",
    "USSLOWL_MULTUTIL",
    "USSLOWL_OILGSCON",
    "USSLOWL_OILGSDRL",
    "USSLOWL_OILGSEQP",
    "USSLOWL_OILGSEXP",
    "USSLOWL_PAPER",
    "USSLOWL_PHARMA",
    "USSLOWL_PRECMTLS",
    "USSLOWL_PSNLPROD",
    "USSLOWL_REALEST",
    "USSLOWL_RESTAUR",
    "USSLOWL_RESVOL",
    "USSLOWL_ROADRAIL",
    "USSLOWL_SEMICOND",
    "USSLOWL_SEMIEQP",
    "USSLOWL_SOFTWARE",
    "USSLOWL_SPLTYRET",
    "USSLOWL_SPTYCHEM",
    "USSLOWL_SPTYSTOR",
    "USSLOWL_TELECOM",
    "USSLOWL_TRADECO",
    "USSLOWL_TRANSPRT",
    "USSLOWL_WIRELESS",
])

def get_barrids_by_date(date_: date) -> list[str]:
    with Database() as db:
        # Get barrids for date
        barrids = db.execute(
            f"""
            SELECT distinct barrid FROM assets WHERE barrid = rootid AND date = '{date_}';
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
    factors_str = ", ".join(FACTORS)
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
    factors_str = ", ".join(FACTORS)
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
                "factor1": FACTORS,
                **{col: covariance_matrix[:, idx] for idx, col in enumerate(FACTORS)},
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
            FROM assets
            WHERE date = '{date_}'
                AND barrid IN ({barrids_str})
            ORDER By barrid
            ;
            """
        ).pl()

        # Fill null values with 0
        df = df.fill_null(0)

        return df
    

if __name__ == '__main__':
    date_ = date(2025, 3, 7)
    cov_mat = construct_covariance_matrix(date_)
    print(cov_mat)

    
