from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import date
from zipfile import ZipFile
import polars as pl
from io import BytesIO


class Folder(Enum):
    HISTORY = "history"
    BIME = "bime"


class Model(Enum):
    USSLOW = "usslow"
    GEMLT = "gemlt"


class ModelFolder(Enum):
    SM = "sm"
    LEGACY = "legacy"


class Frequency(Enum):
    DAILY = "daily"
    MONTHLY = "monthly"


class ZipFolder(Enum):
    SMD_USSLOW_100_D = "SMD_USSLOW_100_D"  # _2025
    SMD_USSLOWL_100_D = "SMD_USSLOWL_100_D"
    SMD_USSLOWL_100 = "SMD_USSLOWL_100"
    SMD_USSLOW_XSEDOL_ID = "SMD_USSLOW_XSEDOL_ID"


class File(Enum):
    USSLOW_Daily_Asset_Price = "USSLOW_Daily_Asset_Price"  # .20250221
    USSLOWL_100_Asset_Data = "USSLOWL_100_Asset_Data"
    USSLOWL_100_Asset_Exposure = "USSLOWL_100_Asset_Exposure"
    USSLOW_100_Asset_DlySpecRet = "USSLOW_100_Asset_DlySpecRet"
    USSLOWL_100_Covariance = "USSLOWL_100_Covariance"
    USSLOWL_100_DlyFacRet = "USSLOWL_100_DlyFacRet"
    USA_XSEDOL_Asset_ID = "USA_XSEDOL_Asset_ID"
    USA_Asset_Identity = "USA_Asset_Identity"


@dataclass
class BarraFile:
    folder: Folder
    file: File
    date_: date
    model: Model | None = None
    model_folder: ModelFolder | None = None
    frequency: Frequency | None = None
    zip_folder: ZipFolder | None = None

    def __post_init__(self) -> None:
        """Build the base url"""
        load_dotenv(override=True)

        home, user = os.getenv("ROOT").split("/")[1:3]
        self.base_path = Path(f"/{home}/{user}/groups/grp_barra/barra")

        for path_part in [
            self.folder,
            self.model,
            self.model_folder,
            self.frequency,
        ]:
            if path_part is not None:
                self.base_path = self.base_path / path_part.value

    @property
    def zip_folder_name(self) -> str:
        match self.folder:
            case Folder.HISTORY:
                return f"{self.zip_folder.value}_{self.date_.year}"
            case Folder.BIME:
                return f"{self.zip_folder.value}_{self.date_.strftime('%y%m%d')}"

    @property
    def zip_folder_path(self):
        return self.base_path / f"{self.zip_folder_name}.zip"

    @property
    def file_name(self):
        return f"{self.file.value}.{self.date_.strftime('%Y%m%d')}"

    @property
    def file_path(self) -> str:
        return self.base_path / self.zip_folder_name / self.file_name

    @property
    def exists(self):
        """Check if the file exists inside the zipped folder."""
        try:
            with ZipFile(self.zip_folder_path, "r") as zip_ref:
                return self.file_name in zip_ref.namelist()

        except FileNotFoundError:
            return False

    @property
    def df(self) -> pl.DataFrame:
        """Task for getting a file given a BarraFile."""
        with ZipFile(self.zip_folder_path, "r") as zip_ref:
            with zip_ref.open(self.file_name) as file:
                skip = 0

                match self.file:
                    case File.USSLOW_Daily_Asset_Price:
                        skip = 1
                    case File.USSLOWL_100_Asset_Data:
                        skip = 2
                    case File.USSLOWL_100_Asset_Exposure:
                        skip = 2
                    case File.USSLOW_100_Asset_DlySpecRet:
                        skip = 2
                    case File.USSLOWL_100_Covariance:
                        skip = 2
                    case File.USSLOWL_100_DlyFacRet:
                        skip = 2
                    case File.USA_XSEDOL_Asset_ID:
                        skip = 1
                    case File.USA_Asset_Identity:
                        skip = 1
                    case _:
                        msg = "Not a valid barra_file.file."
                        raise ValueError(msg)

                return pl.read_csv(BytesIO(file.read()), skip_rows=skip, separator="|")


if __name__ == "__main__":
    barra_file = BarraFile(
        folder=Folder.BIME,
        zip_folder=ZipFolder.SMD_USSLOW_XSEDOL_ID,
        file=File.USA_Asset_Identity,
        date_=date(2025, 3, 7),
    )
    print(barra_file.df.glimpse())

    df = (
        barra_file.df
        .rename({
            '!Barrid': 'barrid',
            'Name': 'name',
            'Instrument': 'instrument',
            'IssuerID': 'issuerid',
            'ISOCountryCode': 'iso_country_code',
            'ISOCurrencyCode': 'iso_currency_code',
            'RootID': 'root_id',
            'StartDate': 'start_date',
            'EndDate': 'end_date'
        })
    )
    df = (
        df.with_columns(
            pl.col(['start_date', 'end_date']).cast(pl.String).str.strptime(dtype=pl.Date, format="%Y%m%d")
        )
        .filter(pl.col('barrid').ne('[End of File]'))
        .with_columns(pl.col('end_date').clip(upper_bound=date.today()))
        # .pivot(index=['start_date', 'end_date', 'barrid'], on='asset_id_type', values='asset_id')
        .with_columns(
            pl.date_ranges("start_date", "end_date").alias("date")
        )
        .explode('date')
        # .select(['date', 'barrid', 'cins', 'cusip', 'isin', 'localid'])
    )
    print(df)
    #  barra_file = BarraFile(
    #     folder=Folder.BIME,
    #     zip_folder=ZipFolder.SMD_USSLOW_XSEDOL_ID,
    #     file=File.USA_XSEDOL_Asset_ID,
    #     date_=date(2025, 3, 7),
    # )

    # # print(barra_file.zip_folder_path)
    # # print(barra_file.file_path)
    # # print(barra_file.df)

    # df = (
    #     barra_file.df
    #     .rename({
    #         '!Barrid': 'barrid',
    #         'AssetIDType': 'asset_id_type',
    #         'AssetID': 'asset_id',
    #         'StartDate': 'start_date',
    #         'EndDate': 'end_date'
    #     })
    # )
    # df = (
    #     df.with_columns(
    #         pl.col(['start_date', 'end_date']).cast(pl.String).str.strptime(dtype=pl.Date, format="%Y%m%d")
    #     )
    #     .filter(pl.col('barrid').ne('[End of File]'))
    #     .with_columns(pl.col('end_date').clip(upper_bound=date.today()))
    #     .pivot(index=['start_date', 'end_date', 'barrid'], on='asset_id_type', values='asset_id')
    #     # .with_columns(
    #     #     pl.date_ranges("start_date", "end_date").alias("date")
    #     # )
    #     # .explode('date')
    #     # .rename({col: col.lower() for col in df.columns})
    #     # .select(['date', 'barrid', 'cins', 'cusip', 'isin', 'localid'])
    # )

    # print(df)
