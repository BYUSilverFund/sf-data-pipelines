# Let's see if I can write a pipeline for Barra returns
from datetime import date
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


@flow(name="barra-backfill-flow")
def barra_backfill_flow() -> None:
    """Flow for orchestrating barra reutrns backfill."""
    # Define barra file
    barra_file = BarraFile(
        folder=Folder.HISTORY,
        model=Model.USSLOW,
        model_folder=ModelFolder.SM,
        frequency=Frequency.DAILY,
        zip_folder=ZipFolder.SMD_USSLOW_100_D,
        file=File.USSLOW_Daily_Asset_Price,
        date_=date(2025, 2, 21),
    )

    # Get raw dataframe
    raw_df = get_barra_file_as_df(barra_file)

    # Clean dataframe
    clean_df = clean_barra_df(raw_df)

    # Load into database
    ingest_df_into_db(clean_df, "barra_returns")


if __name__ == "__main__":
    barra_backfill_flow()

    with Database() as db:
        result = db.execute("SELECT * FROM barra_returns;").pl()
        print(result)
