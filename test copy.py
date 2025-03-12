import wrds
import polars as pl
# Merging to CRSP:
# https://wrds-www.wharton.upenn.edu/pages/support/support-articles/mergent-fisd/linking-mergent-compustat-or-crsp/

# API Documentation:
# https://wrds-www.wharton.upenn.edu/documents/1443/wrds_connection.html

# Russell Methodology:
# https://wrds-www.wharton.upenn.edu/documents/836/Russell_US_Indexes_Methodology_Overview.pdf

# Russell Index Names:
# https://wrds-www.wharton.upenn.edu/documents/834/FTSE_Russell_Proposal_to_WRDS.pdf

# Connect to WRDS
db = wrds.Connection(wrds_username="amh1124")

dfs = []

# Fetch data
df = db.get_table(library="ftse_russell_us", table="idx_holdings_us", rows=1000)

print(pl.from_pandas(df).glimpse())
