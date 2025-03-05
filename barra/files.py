from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import date


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
    SMD_USSLOW_100_D = 'SMD_USSLOW_100_D' # _2025

class File(Enum):
    USSLOW_Daily_Asset_Price = 'USSLOW_Daily_Asset_Price' # .20250221

@dataclass
class BarraFile:
    folder: Folder
    model: Model | None
    model_folder: ModelFolder | None
    frequency: Frequency | None
    zip_folder: ZipFolder | None
    file: File
    date_: date

    @property
    def zip_folder_path(self):
        load_dotenv(override=True)

        home, user = os.getenv("ROOT").split("/")[1:3]
        path = Path(f"/{home}/{user}/groups/grp_barra/barra")

        for path_part in [
            self.folder,
            self.model,
            self.model_folder,
            self.frequency,
        ]:
            if path_part is not None:
                path = path / path_part.value    
        
        path = path / f"{self.zip_folder.value}_{self.date_.year}.zip"
        
        return path
    
    @property
    def file_path(self):
        return f"{self.file.value}.{self.date_.strftime('%Y%m%d')}"
    

if __name__ == '__main__':
    barra_file = BarraFile(
        folder=Folder.HISTORY,
        model=Model.USSLOW,
        model_folder=ModelFolder.SM,
        frequency=Frequency.DAILY,
        zip_folder=ZipFolder.SMD_USSLOW_100_D,
        file=File.USSLOW_Daily_Asset_Price,
        date_=date(2025, 2, 21)
    )

    print(barra_file.zip_folder_path)
    print(barra_file.file_path)