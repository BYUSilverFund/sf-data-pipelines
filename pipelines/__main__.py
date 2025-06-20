import click
import datetime as dt
from all_pipelines import (
    barra_backfill_pipeline,
    ftse_backfill_pipeline,
    crsp_backfill_pipeline,
    covariance_matrix_pipeline,
    barra_daily_pipeline,
    strategy_backfill_pipeline
)
from enums import DatabaseName
from utils.tables import Database

# Valid options
VALID_DATABASES = ["research", "production"]
PIPELINE_TYPES = ["backfill", "update"]


@click.group()
def cli():
    """Main CLI entrypoint."""
    pass


@cli.command()
@click.argument(
    "pipeline_type", type=click.Choice(PIPELINE_TYPES, case_sensitive=False)
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1995, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def barra(pipeline_type, database, start, end):
    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running barra backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            barra_backfill_pipeline(start, end, database_instance)

        case "update":
            click.echo(f"Running update for {database} database.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            barra_daily_pipeline(database_instance)


@cli.command()
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1925, 1, 1)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def crsp(pipeline_type, database, start, end):
    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running crsp backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            crsp_backfill_pipeline(start, end, database_instance)


@cli.command()
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1925, 1, 1)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def ftse(pipeline_type, database, start, end):
    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running ftse backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            ftse_backfill_pipeline(start, end, database_instance)

@cli.command()
@click.argument(
    "pipeline_type", type=click.Choice(PIPELINE_TYPES, case_sensitive=False)
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1995, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def strategy(pipeline_type, database, start, end):
    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running strategy backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            strategy_backfill_pipeline(start, end, database_instance)



@cli.command()
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=False,
    default="production",
    help="Target database (research or database).",
)
def covariance(database: Database):
    """Create and load today's covariance matrix into S3."""
    click.echo("Running covariance matrix pipeline.")
    database_name = DatabaseName(database)
    database_instance = Database(database_name)
    covariance_matrix_pipeline(database_instance)

if __name__ == "__main__":
    cli()
