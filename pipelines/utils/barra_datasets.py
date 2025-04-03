from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import date
import os
from pathlib import Path

class BarraDataset:
    def __init__(self, history_folder: str, daily_folder: str, zip_folder_name: str, file_name: str) -> None:
        load_dotenv(override=True)

        home, user = os.getenv("ROOT").split("/")[1:3]
        self._base_path = Path(f"/{home}/{user}/groups/grp_msci_barra/nobackup/archive")

        self._history_folder = history_folder
        self._daily_folder = daily_folder
        self._zip_folder_name = zip_folder_name
        self._file_name = file_name

    def history_zip_folder_path(self, year: int) -> Path:
        return self._base_path / self._history_folder / f"{self._zip_folder_name}_{year}.zip"
    
    def file_name(self, date_: date | None = None) -> str:
        if date_:
            return f"{self._file_name}.{date_.strftime('%Y%m%d')}"
        else:
            return self._file_name
    
    def daily_zip_folder_path(self, date_: date) -> Path:
        return self._base_path / self._daily_folder / f"{self._zip_folder_name}_{date_.strftime('%y%m%d')}.zip"
    

barra_returns = BarraDataset(
    history_folder='history/usslow/sm/daily',
    daily_folder='us/usslow',
    zip_folder_name='SMD_USSLOWL_100_D',
    file_name='USSLOW_Daily_Asset_Price',
)    

# if __name__ == '__main__':

#     date_ = date.today()

#     print()
#     print(barra_returns.history_zip_folder_path(2025))
#     print()
#     print(barra_returns.file_name())
#     print()
#     print(barra_returns.daily_zip_folder_path(date_))
#     print()
#     print(barra_returns.file_name(date_))
#     print()
    
