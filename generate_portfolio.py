import polars as pl
from pipelines.utils.views import in_universe_assets, benchmark_weights
from pipelines.system.portfolios import mean_variance_efficient
from pipelines.system.records import Alpha
from pipelines.system.constraints import (
    full_investment,
    no_buying_on_margin,
    long_only,
    unit_beta,
)
from pipelines.utils import get_last_market_date
from pipelines.system.covariance_matrix import construct_covariance_matrix
from pipelines.utils.tables import composite_alphas_table
import numpy as np


# ----- Parameters -----
date_ = get_last_market_date()[0]
filter = True
barrids_file = None
constraints = [
    full_investment,
    no_buying_on_margin,
    unit_beta,
    long_only,
]

print(date_)


# ----- Get Composite Alphas -----
composite_alphas = (
    composite_alphas_table.read()
    # .filter(pl.col("date").eq(date_))
    .filter(pl.col("name").eq("risk_parity"))
    .select(["date", "barrid", "alpha"])
    .sort("barrid")
    .collect()
)
print(composite_alphas)

# # ----- Get Barrids -----
# if barrids_file is None:
#     barrids = composite_alphas["barrid"].unique().sort().to_list()
# else:
#     barrids = pl.read_csv(barrids_file)["barrid"].unique().sort().to_list()


# def generate_portfolio(gamma: float) -> pl.DataFrame:
#     """Function for generating a portfolio given a value of gamma."""
#     # Get mean variance optimal weights
#     weights = mean_variance_efficient(
#         date_, barrids, Alpha(composite_alphas), constraints, gamma=gamma
#     )

#     # Join ticker column
#     portfolio = (
#         in_universe_assets.filter(pl.col("date").eq(date_))
#         .select("date", "barrid", "ticker")
#         .join(weights.lazy(), on=["date", "barrid"], how="left")
#         .filter(pl.col("weight").gt(0))  # Only keep positive weights
#         .select("barrid", "ticker", "weight")
#     )

#     # Join benchmark weights
#     benchmark = benchmark_weights.filter(pl.col("date").eq(date_)).select(
#         "barrid", pl.col("weight").alias("benchmark_weight")
#     )

#     # Compute active weights
#     active_portfolio = (
#         portfolio.join(benchmark, on=["barrid"], how="left")
#         .with_columns(
#             pl.col("weight").sub(pl.col("benchmark_weight")).alias("active_weight")
#         )
#         .sort("barrid")
#         .collect()
#     )

#     return active_portfolio


# def compute_active_risk(active_portfolio: pl.DataFrame) -> float:
#     """Function for computing active risk given an active portfolio."""
#     barrids = active_portfolio["barrid"].unique().sort().to_list()

#     covariance_matrix = (
#         construct_covariance_matrix(date_, barrids).drop("barrid").to_numpy()
#     )

#     active_weights = active_portfolio["active_weight"].to_numpy()
#     active_risk = np.sqrt(active_weights.T @ covariance_matrix @ active_weights)

#     return active_risk


# def get_target_active_risk_portfolio(
#     target_active_risk: float = 5.0
# ):
#     # Parameters
#     gammas = [.1 * i for i in range(1, 21)]

#     # Results
#     best_gamma = gammas[0]
#     best_active_risk = float("inf")
#     best_active_portfolio = None

#     for i, gamma in enumerate(gammas):
#         active_portfolio = generate_portfolio(gamma)
#         active_risk = compute_active_risk(active_portfolio)

#         if best_active_portfolio is None:
#             best_active_portfolio = active_portfolio

#         print(
#             f"Iteration {i + 1}: Gamma = {gamma:.6f}, Active Risk = {active_risk:.2f}%"
#         )

#         # Keep track of best gamma
#         if abs(active_risk - target_active_risk) < abs(best_active_risk - target_active_risk):
#             best_active_risk = active_risk
#             best_gamma = gamma
#             best_active_portfolio = active_portfolio

#     print(f"Best Gamma: {best_gamma:.6f}, Active Risk = {best_active_risk:.2f}%")

#     return best_active_portfolio, best_gamma, best_active_risk

# if __name__ == "__main__":
#     active_portfolio, _, _ = get_target_active_risk_portfolio()
#     print("Active Portfolio", active_portfolio.sort('active_weight', descending=True))
#     active_portfolio.write_csv('portfolio.csv')