import duckdb

class Database:
    def __init__(self, db_path: str = "barra.duckdb"):
        self.db_path = db_path
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
