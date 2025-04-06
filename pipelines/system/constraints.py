from datetime import date
from typing import Protocol

import cvxpy as cp
import polars as pl
from pipelines.utils.tables import assets_table

class ConstraintConstructor(Protocol):
    """
    A protocol that defines a callable interface for constraint constructors.
    This interface is used to define various portfolio constraints in the optimization process.

    Methods:
        __call__(weights, date_, barrids) -> cp.Constraint:
            Given portfolio weights, a specific date, and a list of asset identifiers (barrids),
            returns a constraint to be applied in the portfolio optimization problem.
    """

    def __call__(self, weights: cp.Variable, date_: date, barrids: list[str]) -> cp.Constraint: ...


def full_investment(weights: cp.Variable, date_: date, barrids: list[str]) -> cp.Constraint:
    """
    Enforces the full investment constraint, where the sum of portfolio weights must equal 1.

    Args:
        weights (cp.Variable): The decision variable representing portfolio weights.
        date_ (date): The date for which the constraint is applied.
        barrids (list[str]): A list of asset identifiers (barrids) in the portfolio.

    Returns:
        cp.Constraint: The full investment constraint that ensures the sum of weights equals 1.
    """
    return cp.sum(weights) == 1


def no_buying_on_margin(weights: cp.Variable, date_: date, barrids: list[str]) -> cp.Constraint:
    """
    Enforces the constraint that no asset can be purchased on margin, i.e., portfolio weights must be less than or equal to 1.

    Args:
        weights (cp.Variable): The decision variable representing portfolio weights.
        date_ (date): The date for which the constraint is applied.
        barrids (list[str]): A list of asset identifiers (barrids) in the portfolio.

    Returns:
        cp.Constraint: The no-buying-on-margin constraint that ensures weights are less than or equal to 1.
    """
    return weights <= 1


def long_only(weights: cp.Variable, date_: date, barrids: list[str]) -> cp.Constraint:
    """
    Enforces the long-only constraint, where portfolio weights must be non-negative.

    Args:
        weights (cp.Variable): The decision variable representing portfolio weights.
        date_ (date): The date for which the constraint is applied.
        barrids (list[str]): A list of asset identifiers (barrids) in the portfolio.

    Returns:
        cp.Constraint: The long-only constraint that ensures weights are greater than or equal to 0.
    """
    return weights >= 0


def zero_beta(
    weights: cp.Variable, date_: date, barrids: list[str]
) -> cp.Constraint:
    betas = (
        assets_table.read(date_.year)
        .filter(pl.col('date').eq(date_))
        .filter(pl.col('barrid').is_in(barrids))
        .select('barrid', 'predicted_beta')
        .sort('barrid')
        .collect()
        ['predicted_beta']
        .to_numpy()
    )

    return cp.sum(cp.multiply(weights, betas)) == 0


def unit_beta(
    weights: cp.Variable, date_: date, barrids: list[str]
) -> cp.Constraint:
    betas = (
        assets_table.read(date_.year)
        .filter(pl.col('date').eq(date_))
        .filter(pl.col('barrid').is_in(barrids))
        .select('barrid', 'predicted_beta')
        .sort('barrid')
        .collect()
        ['predicted_beta']
        .to_numpy()
    )

    return cp.sum(cp.multiply(weights, betas)) == 1
