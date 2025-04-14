import wrds
import polars as pl
from datetime import date

# wrds_db = wrds.Connection(wrds_username="amh1124")

# print(wrds_db.list_tables(library='crspm'))


wrds_db = wrds.Connection(wrds_username="amh1124")

start_date = date(2024, 1, 1)
end_date = date(2024, 12, 31)

df = wrds_db.raw_sql(
    f"""
    WITH names AS(
        SELECT DISTINCT
            TO_CHAR(date, 'YYYY-MM') AS month_date,
            permno,
            LAST_VALUE(ticker) OVER (
                PARTITION BY permno
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ticker
            -- shrcd,
            -- exchcd
        FROM crsp_m_stock.dse
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND event = 'NAMES'
            AND exchcd IN (1, 2, 3)
            AND shrcd IN (10, 11)
        GROUP BY month_date, permno
    )
    SELECT
        a.date,
        a.permno,
        a.cusip,
        a.ret,
        a.retx,
        a.prc,
        a.vol,
        a.shrout,
        b.ticker,
        b.shrcd,
        b.exchcd
    FROM crsp_m_stock.msf a
    LEFT JOIN names b ON a.permno = b.permno AND TO_CHAR(a.date, 'YYYY-MM') = b.month_date
    WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
    ;
    """
)
df = pl.from_pandas(df)
print(df.sort(['permno', 'date']))

# print(df.filter(pl.col('date').eq("2024-02-29")))
# print(df.filter(pl.col('date').eq("2024-02-29")).glimpse())

# df = (
#     df
#     .sort(['permno', 'date'])
#     .with_columns(
#         pl.col('ticker').fill_null(strategy='forward').over('permno'),
#         pl.col('shrcd').fill_null(strategy='forward').over('permno'),
#         pl.col('exchcd').fill_null(strategy='forward').over('permno'),
#     )
# )

print(df.filter(pl.col('permno').eq(13559)))
print(
    df.group_by(["permno"])
    .agg(pl.len())
    .filter(pl.col("len").gt(1))
    .sort("permno")
)

# print(df.select('event').unique())
