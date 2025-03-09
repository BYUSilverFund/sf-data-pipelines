# Let's see if I can write a pipeline for Barra returns
from datetime import date, timedelta
import polars as pl
from zipfile import ZipFile
from io import BytesIO
from prefect import task, flow
from database import Database
from barra.files import (
    BarraFile,
    Folder,
    Model,
    ModelFolder,
    Frequency,
    ZipFolder,
    File,
)
from barra.utils import rename_barra_columns, cast_barra_columns
from jinja2 import Template


class Pipeline:
    def __init__(self, barra_file: BarraFile):
        pass


@task
def barra_file_exists(barra_file: BarraFile) -> bool:
    """Task for checking if a BarraFile exists inside the zipped folder."""
    try:
        with ZipFile(barra_file.zip_folder_path, "r") as zip_ref:
            return barra_file.file_path in zip_ref.namelist()
    except FileNotFoundError:
        return False


@task
def get_barra_file_as_df(barra_file: BarraFile) -> pl.DataFrame:
    """Task for getting a file given a BarraFile."""
    # Open zipped folder as a ref
    with ZipFile(barra_file.zip_folder_path, "r") as zip_ref:
        # Open a specific file in the zipped folder
        with zip_ref.open(barra_file.file_path) as file:
            # Read file as a csv
            return pl.read_csv(BytesIO(file.read()), skip_rows=1, separator="|")


@task
def clean_barra_df(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Task for cleaning barra files"""
    # Drop last row
    if raw_df.row(-1)[0] == "[End of File]":
        raw_df = raw_df.head(-1)

    # Rename columns
    df = rename_barra_columns(raw_df)

    # Cast data types
    df = cast_barra_columns(df)

    return df


@task
def ingest_df_into_db(df: pl.DataFrame, table: str) -> None:
    """Task for ingesting dataframe into duck db"""
    with Database() as db:
        db.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df;")


@task
def run_sql_file(sql_file: str, params: dict = {}) -> pl.DataFrame | None:
    """
    Reads an SQL file, renders it using Jinja2 with given parameters, and executes it.

    :param sql_file: Path to the SQL file
    :param params: Dictionary of parameters to replace in the SQL file
    :return: Polars dataframe of query results.
    """
    with open(sql_file, "r") as file:
        template = Template(file.read())

    query = template.render(**params)

    with Database() as db:
        db.execute(query).pl()


@flow(name="barra-backfill-flow")
def barra_backfill_flow(start_date: date, end_date: date) -> None:
    """Flow for orchestrating barra reutrns backfill."""

    current_date = start_date
    while current_date <= end_date:
        # Define barra file
        barra_file = BarraFile(
            folder=Folder.HISTORY,
            model=Model.USSLOW,
            model_folder=ModelFolder.SM,
            frequency=Frequency.DAILY,
            zip_folder=ZipFolder.SMD_USSLOW_100_D,
            file=File.USSLOW_Daily_Asset_Price,
            date_=current_date,
        )

        if barra_file_exists(barra_file):
            date_string = current_date.strftime("%Y%m%d")

            # Get raw dataframe
            df = get_barra_file_as_df(barra_file)

            # Clean dataframe
            df = clean_barra_df(df)

            # Create core table if not exists
            run_sql_file("sql/assets_create.sql")

            # Load into database
            ingest_df_into_db(df, f"assets_{date_string}_stg")

            # Transform table
            run_sql_file(
                sql_file="sql/assets_transform.sql",
                params={
                    "date": date_string,
                },
            )

            # Merge table
            run_sql_file(
                sql_file="sql/assets_merge.sql",
                params={
                    "date": date_string,
                },
            )

        current_date += timedelta(days=1)


if __name__ == "__main__":
    # barra_backfill_flow(start_date=date(2025, 1, 1), end_date=date.today())

    with Database() as db:
        result = db.execute("SELECT * FROM assets ORDER BY barrid, date;").pl()
        print(result)
