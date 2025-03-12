import wrds
import polars as pl

# Connect to WRDS
db = wrds.Connection(wrds_username="amh1124")

dfs = []

# Define row fetching parameters
current_rows = 0
row_increment = 145000
total_rows = db.get_row_count(library="ftse_russell_us", table="idx_holdings_us")

while current_rows < total_rows:
    # Fetch data
    df = db.get_table(library="ftse_russell_us", table="idx_holdings_us", offset=current_rows, rows=row_increment)
    
    # Ensure df is not None (in case of an empty result)
    if df is not None and not df.empty:
        dfs.append(pl.from_pandas(df))  # Convert pandas DataFrame to polars DataFrame
    print(df)
    # Increment offset
    current_rows += row_increment

# Concatenate all fetched DataFrames
if dfs:
    df: pl.DataFrame = pl.concat(dfs)
else:
    df = pl.DataFrame()  # Empty DataFrame if no data was fetched

print(df)
