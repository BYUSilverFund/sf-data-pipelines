import duckdb
from dotenv import load_dotenv
from pathlib import Path
import os


class Database:
    def __init__(self, db_name: str = "silverfund.duckdb"):
        load_dotenv(override=True)

        home, user = os.getenv("ROOT").split("/")[1:3]
        path = Path(f"/{home}/{user}/groups/grp_quant")
        # path = Path(f"/{home}/{user}/groups/grp_quant/sf-data-pipelines") # temp
        self.db_path = path / db_name

        self.con: duckdb.DuckDBPyConnection | None = None

    def connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        self.con = duckdb.connect(self.db_path, read_only=read_only)
        return self.con

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.con:
            self.con.close()
