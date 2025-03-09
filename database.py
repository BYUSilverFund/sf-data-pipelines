import duckdb
from dotenv import load_dotenv
from pathlib import Path
import os

class Database:
    def __init__(self, db_name: str = "silverfund.duckdb"):
        load_dotenv(override=True)

        # Define database path
        home, user = os.getenv("ROOT").split("/")[1:3]
        path = Path(f"/{home}/{user}/groups/grp_quant/data")
        self.db_path = path / db_name

        # Preset connection
        self.con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self.con = duckdb.connect(self.db_path)
        return self.con

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.con:
            self.con.close()

    def execute(self, query: str, params: tuple = ()) -> duckdb.DuckDBPyConnection:
        if not self.con:
            raise RuntimeError("Database connection is not open.")
        return self.con.execute(query, params)
