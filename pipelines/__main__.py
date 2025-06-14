import click
import datetime as dt
from all_pipelines import backfill_pipeline
from enums import DatabaseName
from utils.tables import Database

VALID_DATABASES = ["research", "database"]


@click.group()
def cli():
    """Main CLI group"""
    pass


VALID_DATABASES = ["research", "database"]


@cli.command()
@click.argument("database", type=click.Choice(VALID_DATABASES, case_sensitive=False))
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1995, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD)",
)
def backfill(database, start_date, end_date):
    """Run the backfill pipeline for the given database."""
    start_date = start_date.date() if hasattr(start_date, "date") else start_date
    end_date = end_date.date() if hasattr(end_date, "date") else end_date
    database_name = DatabaseName(database)
    database_ = Database(database_name)
    backfill_pipeline(start_date, end_date, database_)


@cli.command()
@click.argument("database", type=click.Choice(VALID_DATABASES, case_sensitive=False))
def update(database):
    """Run the daily update pipeline for the given database."""
    click.echo(f"Running update for {database} database")


if __name__ == "__main__":
    cli()
