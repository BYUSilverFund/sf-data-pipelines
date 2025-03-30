import os
from datetime import date
from functools import partial
from typing import Protocol

import polars as pl
from tqdm import tqdm

from pipelines.system.alphas import Alpha
from pipelines.system.constraints import ConstraintConstructor
from pipelines.system.covariance_matrix import construct_covariance_matrix
from pipelines.system.optimizers import quadratic_program
from pipelines.system.records import Portfolio, CovarianceMatrix


def mean_variance_efficient(
    period: date,
    barrids: list[str],
    alphas: Alpha,
    constraints: list[ConstraintConstructor],
    gamma: float = 2.0,
) -> Portfolio:
    """Constructs a mean-variance efficient portfolio using quadratic optimization.

    This function builds an optimal portfolio by solving a quadratic programming
    problem that balances expected returns (alphas) and risk (covariance matrix)
    subject to given constraints.

    Args:
        period (date): The date for which the portfolio is constructed.
        barrids (list[str]): List of asset identifiers (barrids) included in the portfolio.
        alphas (Alpha): Expected returns for the assets.
        constraints (list[ConstraintConstructor]): List of constraints applied to the optimization.
        gamma (float, optional): Risk aversion parameter (default is 2.0).
                                 Higher values penalize risk more heavily.

    Returns:
        Portfolio: A Polars DataFrame wrapped in the Portfolio class,
                   containing 'date', 'barrid', and 'weight' columns.
    """

    # Get covariance matrix
    cov_mat = CovarianceMatrix(construct_covariance_matrix(period, barrids), barrids)

    # Cast to numpy arrays
    alphas = alphas.to_vector()
    cov_mat = cov_mat.to_matrix()

    # Construct constraints
    constraints = [partial(constraint, date_=period, barrids=barrids) for constraint in constraints]

    # Find optimal weights
    weights = quadratic_program(alphas, cov_mat, constraints, gamma)

    portfolio = pl.DataFrame({"date": period, "barrid": barrids, "weight": weights})
    portfolio = portfolio.sort(["barrid", "date"])

    return Portfolio(portfolio)