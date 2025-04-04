import polars as pl
import os
from dotenv import load_dotenv


class Table:

    def __init__(self, name: str, schema: dict[str, pl.DataType], ids=list[str]) -> None:
        load_dotenv(override=True)
        home, user = os.getenv("ROOT").split("/")[1:3]
        # self._base_path = f"/{home}/{user}/groups/grp_quant/database"  
        self._base_path = "data"
        
        self._name = name
        self._schema = schema
        self._ids = ids

        os.makedirs(f"data/{self._name}", exist_ok=True)

    def _file_path(self, year: int | None = None) -> str:
        if year is not None:
            return f"{self._base_path}/{self._name}/{self._name}_{year}.parquet"
        else:
            return f"{self._base_path}/{self._name}/{self._name}_*.parquet"
        
    def exists(self, year: int) -> bool:
        return os.path.exists(self._file_path(year))

    
    def create_if_not_exists(self, year: int) -> None:
        if not os.path.exists(self._file_path(year)):
            pl.DataFrame(schema=self._schema).write_parquet(self._file_path(year))
    
    def read(self, year: int | None = None) -> pl.LazyFrame:
        if year is None:
            return pl.scan_parquet(self._file_path())
        else:
            return pl.scan_parquet(self._file_path(year))
    
    def upsert(self, year: int, rows: pl.DataFrame) -> None:
        if os.path.exists(self._file_path(year)):
            (
                pl.scan_parquet()
                .update(rows.lazy(), on=self._ids, how='full')
                .collect()
                .write_parquet(self._file_path(year))
            )
        else:
            rows.write_parquet(self._file_path(year))

    def update(self, year: int, rows: pl.DataFrame) -> None:
        (
            pl.scan_parquet(self._file_path(year))
            .update(rows.lazy(), on=self._ids, how='left')
            .collect()
            .write_parquet(self._file_path(year))
        )


assets_table = Table(
    name='assets',
    schema={
        "date": pl.Date,
        "rootid": pl.String,
        "barrid": pl.String,
        "issuerid": pl.String,
        "instrument": pl.String,
        "name": pl.String,
        "cusip": pl.String,
        "ticker": pl.String,
        "price": pl.Float64,
        "return": pl.Float64,
        "market_cap": pl.Float64,
        "price_source": pl.String,
        "currency": pl.String,
        "iso_country_code": pl.String,
        "iso_currency_code": pl.String,
        "yield": pl.Float64,
        "total_risk": pl.Float64,
        "specific_risk": pl.Float64,
        "historical_beta": pl.Float64,
        "predicted_beta": pl.Float64,
        "russell_1000": pl.Boolean,
        "russell_2000": pl.Boolean,
    },
    ids=['date', 'barrid']
)

if __name__ == '__main__':
    print(len(assets_table.read().collect()))