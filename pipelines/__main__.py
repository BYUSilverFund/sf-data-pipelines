import click
import datetime as dt
from all_pipelines import barra_backfill_pipeline, ftse_backfill_pipeline, crsp_backfill_pipeline
from enums import DatabaseName
from utils.tables import Database

# Valid options
VALID_DATABASES = ["research", "database"]
VALID_BACKFILL_GROUPS = ["barra", "ftse", "crsp"]


@click.group()
def cli():
    """Main CLI entrypoint."""
    pass


@cli.command()
@click.argument("backfill_group", type=click.Choice(VALID_BACKFILL_GROUPS, case_sensitive=False))
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1995, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def backfill(backfill_group, database, start_date, end_date):
    """Run the backfill pipeline for the given database and group."""
    start_date = start_date.date() if hasattr(start_date, "date") else start_date
    end_date = end_date.date() if hasattr(end_date, "date") else end_date

    click.echo(f"Running backfill for group '{backfill_group}' on '{database}' from {start_date} to {end_date}.")

    database_name = DatabaseName(database)
    database_instance = Database(database_name)

    match backfill_group:
        case 'barra':
            barra_backfill_pipeline(start_date, end_date, database_instance)
        
        case 'ftse':
            ftse_backfill_pipeline(start_date, end_date, database_instance)

        case 'crsp':
            crsp_backfill_pipeline(start_date, end_date, database_instance)

        case _:
            raise ValueError(f"Inavlid backfill_group: {backfill_group}")


@cli.command()
@click.argument("database", type=click.Choice(VALID_DATABASES, case_sensitive=False))
def update(database):
    """Run the daily update pipeline for the given database."""
    click.echo(f"Running update for {database} database.")


if __name__ == "__main__":
    cli()
