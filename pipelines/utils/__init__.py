from jinja2 import Template
import polars as pl
import exchange_calendars as xcals
from datetime import date, timedelta


def render_sql_file(sql_file: str, **kwargs) -> str:
    """
    Reads an SQL file, renders it using Jinja2 with given parameters.

    :param sql_file: Path to the SQL file
    :param params: Dictionary of parameters to replace in the SQL file
    :return: Parameterized string of sql query.
    """
    with open(sql_file, "r") as file:
        template = Template(file.read())

    query = template.render(**kwargs)

    return query


def get_last_market_date(current_date: date) -> date:
    """
    Get the last trading day before the given date.

    This function retrieves the previous market date based on the
    New York Stock Exchange (XNYS) trading calendar.

    Args:
        current_date (date): The reference date to find the previous trading day.

    Returns:
        date: The last trading day before the given date.
    """
    end_date = date.today()

    # Load market calendar
    market_calendar = (
        pl.DataFrame(xcals.get_calendar("XNYS").schedule)
        .with_columns(pl.col("close").dt.date())
        .select(pl.col("close").alias("date"))
        .with_columns(pl.col("date").shift(1).alias("prev_date"))
        .filter(pl.col("date").le(end_date))
        .sort("date")
    )

    # Get standard calendar
    start_date = market_calendar["date"].min()
    date_list = [
        start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)
    ]
    standard_calendar = pl.DataFrame({"date": date_list})

    market_calendar = standard_calendar.join(
        market_calendar, on="date", how="left"
    ).fill_null(strategy="backward")

    # Get previous date
    prev_date = market_calendar.filter(pl.col("date").le(current_date))[
        "prev_date"
    ].max()

    return prev_date
