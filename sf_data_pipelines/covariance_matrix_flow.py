import datetime as dt
import sf_quant.data as sfd
import utils.s3
import polars as pl
import os
import zipfile
import io
from dotenv import load_dotenv
from rich import print

ROOT = os.getenv("ROOT", "/home/amh1124/groups/grp_msci_barra/nobackup/archive")

factors = [
    'USSLOWL_AERODEF',
    'USSLOWL_AIRLINES',
    'USSLOWL_ALUMSTEL',
    'USSLOWL_APPAREL',
    'USSLOWL_AUTO',
    'USSLOWL_BANKS',
    'USSLOWL_BETA',
    'USSLOWL_BEVTOB',
    'USSLOWL_BIOLIFE',
    'USSLOWL_BLDGPROD',
    'USSLOWL_CHEM',
    'USSLOWL_CNSTENG',
    'USSLOWL_CNSTMACH',
    'USSLOWL_CNSTMATL',
    'USSLOWL_COMMEQP',
    'USSLOWL_COMPELEC',
    'USSLOWL_COMSVCS',
    'USSLOWL_CONGLOM',
    'USSLOWL_CONTAINR',
    'USSLOWL_COUNTRY',
    'USSLOWL_DISTRIB',
    'USSLOWL_DIVFIN',
    'USSLOWL_DIVYILD',
    'USSLOWL_EARNQLTY',
    'USSLOWL_EARNYILD',
    'USSLOWL_ELECEQP',
    'USSLOWL_ELECUTIL',
    'USSLOWL_FOODPROD',
    'USSLOWL_FOODRET',
    'USSLOWL_GASUTIL',
    'USSLOWL_GROWTH',
    'USSLOWL_HLTHEQP',
    'USSLOWL_HLTHSVCS',
    'USSLOWL_HOMEBLDG',
    'USSLOWL_HOUSEDUR',
    'USSLOWL_INDMACH',
    'USSLOWL_INSURNCE',
    'USSLOWL_INTERNET',
    'USSLOWL_LEISPROD',
    'USSLOWL_LEISSVCS',
    'USSLOWL_LEVERAGE',
    'USSLOWL_LIFEINS',
    'USSLOWL_LIQUIDTY',
    'USSLOWL_LTREVRSL',
    'USSLOWL_MEDIA',
    'USSLOWL_MGDHLTH',
    'USSLOWL_MGMTQLTY',
    'USSLOWL_MIDCAP',
    'USSLOWL_MOMENTUM',
    'USSLOWL_MULTUTIL',
    'USSLOWL_OILGSCON',
    'USSLOWL_OILGSDRL',
    'USSLOWL_OILGSEQP',
    'USSLOWL_OILGSEXP',
    'USSLOWL_PAPER',
    'USSLOWL_PHARMA',
    'USSLOWL_PRECMTLS',
    'USSLOWL_PROFIT',
    'USSLOWL_PROSPECT',
    'USSLOWL_PSNLPROD',
    'USSLOWL_REALEST',
    'USSLOWL_RESTAUR',
    'USSLOWL_RESVOL',
    'USSLOWL_ROADRAIL',
    'USSLOWL_SEMICOND',
    'USSLOWL_SEMIEQP',
    'USSLOWL_SIZE',
    'USSLOWL_SOFTWARE',
    'USSLOWL_SPLTYRET',
    'USSLOWL_SPTYCHEM',
    'USSLOWL_SPTYSTOR',
    'USSLOWL_TELECOM',
    'USSLOWL_TRADECO',
    'USSLOWL_TRANSPRT',
    'USSLOWL_VALUE',
    'USSLOWL_WIRELESS'
]

exposures_column_mapping = {
    '!Barrid': 'barrid',
    'Factor': 'factor',
    'Exposure': 'exposure',
    'DataDate': 'date'
}

def clean_exposures(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .rename(exposures_column_mapping)
        .filter(pl.col('barrid').ne('[End of File]'))
        .with_columns(
            pl.col('date').cast(pl.String).str.strptime(pl.Date, "%Y%m%d")
        )
        .pivot(index=['date', 'barrid'], on='factor', values='exposure')
        .sort('barrid')
        .select('date', 'barrid', *factors)
    )

def get_latest_stock_exposures() -> pl.DataFrame:
    date_ = dt.date.today() - dt.timedelta(days=1)
    date_str_1 = date_.strftime('%y%m%d')
    date_str_2 = date_.strftime('%Y%m%d')

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_{date_str_1}.zip"
    file_name = f"USSLOWL_100_Asset_Exposure.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )
        
        return clean_exposures(df)
    
def get_latest_etf_exposures() -> pl.DataFrame:
    date_ = dt.date.today() - dt.timedelta(days=1)
    date_str_1 = date_.strftime('%y%m%d')
    date_str_2 = date_.strftime('%Y%m%d')

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_ETF_{date_str_1}.zip"
    file_name = f"USSLOWL_ETF_100_Asset_Exposure.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )
        
        return clean_exposures(df)
    
def get_latest_covariances():
    pass

def covariance_matrix_daily_flow() -> None:
    # get exposures
    # - stocks
    stock_exposures = get_latest_stock_exposures()
    # - etfs
    etf_exposures = get_latest_etf_exposures()

    # get covariances
    # - factors

    # get idio risk
    # - stocks
    # - etfs

    # construct covariance matrix

    # get barrid -> cusip mapping
    # get cusip -> ticker mapping

    # re-key covariance matrix

    # upload to s3
    

    # utils.s3.write_parquet(
    #     bucket_name='barra-covariance-matrices',
    #     file_name='latest.parquet',
    #     file_data=covariance_matrix,
    # )

if __name__ == '__main__':
    covariance_matrix_daily_flow()