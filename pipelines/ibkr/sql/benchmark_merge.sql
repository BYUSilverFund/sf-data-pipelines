INSERT INTO benchmark_new (
    date,
    ticker,
    adjusted_close,
    return
)
SELECT
    date,
    ticker,
    adjusted_close,
    return
FROM "{{stage_table}}"
ON CONFLICT (date, ticker)
DO UPDATE SET 
    date = EXCLUDED.date,
    ticker = EXCLUDED.ticker,
    adjusted_close = EXCLUDED.adjusted_close,
    return = EXCLUDED.return
;
