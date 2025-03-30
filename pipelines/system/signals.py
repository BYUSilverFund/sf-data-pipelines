import polars as pl


def momentum(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # Sort
        .sort(['barrid', 'date'])
        # Momentum from t-252 to t-22
        .with_columns(
            pl.col("return")
            .truediv(100)
            .log1p()
            .rolling_sum(window_size=230)
            .shift(22)
            .over("barrid")
            .alias(momentum.__name__)
        )
    )


def beta(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col("predicted_beta").mul(-1).alias(beta.__name__))


def reversal(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # Sort
        .sort(['barrid', 'date'])
        # Reversal from t-22 to t
        .with_columns(
            pl.col("return")
            .truediv(100)
            .log1p()
            .rolling_sum(window_size=22)
            .mul(-1)
            .over("barrid")
            .alias(reversal.__name__)
        )
    )