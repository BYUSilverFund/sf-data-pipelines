import polars as pl
from datetime import date
from pipelines.utils.tables import assets_clean
import exchange_calendars as xcals
from portfolios import mean_variance_efficient
from records import Alpha
from constraints import full_investment, unit_beta, no_buying_on_margin, long_only

def get_market_date_with_look_back(current_date: date, look_back: int) -> date:
    # Load market calendar
    market_calendar = (
        pl.DataFrame(xcals.get_calendar("XNYS").schedule)
        .with_columns(pl.col("close").dt.date())
        .select(pl.col("close").alias("date"))
        .with_columns(pl.col("date").shift(look_back - 1).alias("prev_date"))
    )

    # Get previous date
    prev_date = market_calendar.filter(pl.col("date").eq(current_date))[
        "prev_date"
    ].max()

    if prev_date is None:
        raise ValueError(f"Not a valid market date: {current_date}")

    return prev_date


# ----- Look Back Parameters -----
max_signal_look_back = 252  # momentum
volatility_look_back = 22  # 1 month
look_back = max_signal_look_back + volatility_look_back - 1

# ----- Data Parameters -----
end_date = date(2025, 3, 25)
start_date = get_market_date_with_look_back(end_date, look_back)

data = (
    assets_clean
    .filter(pl.col('date').is_between(start_date, end_date))
    .select('date', 'barrid', 'ticker', 'price', 'return', 'specific_risk', 'predicted_beta')
    .collect()
)

# ----- Signal Functions -----
def momentum(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("return")
        .truediv(100)
        .log1p()
        .rolling_sum(window_size=230)
        .shift(22)
        .over("barrid")
        .alias(momentum.__name__)
    )


def beta(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col("predicted_beta").mul(-1).alias(beta.__name__))


def reversal(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("return")
        .truediv(100)
        .log1p()
        .rolling_sum(window_size=22)
        .mul(-1)
        .over("barrid")
        .alias(reversal.__name__)
    )

signal_fns = [momentum, beta, reversal]
signal_names = sorted([fn.__name__ for fn in signal_fns])

# ----- Compute signals -----
for signal_fn in signal_fns:
    data = signal_fn(data)

signals = data.select(
    ["date", "barrid", "specific_risk"] + signal_names
)

print(signals)

composite = (
    signals
    .with_columns(
        pl.col(signal_names).truediv(3)
    )
    .with_columns(
        pl.sum_horizontal(signal_names).alias('composite')
    )
    .select(['date', 'barrid', 'composite'])
)

print(composite)

scores = (
    composite
    .with_columns(
        pl.col('composite')
        .sub(pl.col('composite').mean())
        .truediv(pl.col('composite').std())
        .over('barrid')
        .alias('score')
    )
    .select(['date', 'barrid', 'score'])
)

print(scores)

alphas = (
    scores
    .with_columns(
        pl.col('score').mul(.05).mul('specific_risk').alias('alpha')
    )
    .filter(pl.col('date').eq(end_date))
    .select(['date', 'barrid', 'alpha'])
)

print(alphas)

barrids = alphas['barrid'].unique().sort().to_list()

constraints = [
    full_investment,
    no_buying_on_margin,
    unit_beta,
    long_only
]

weights = mean_variance_efficient(end_date, barrids, Alpha(alphas), constraints, gamma=2)

print(weights)

weights.write_csv("optimal_portfolio_weights.csv")