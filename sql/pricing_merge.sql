INSERT INTO assets
(
    barrid, 
    date, 
    price, 
    market_cap, 
    price_source, 
    currency, 
    return
)
SELECT 
    s.barrid, 
    s.date, 
    s.price, 
    s.market_cap, 
    s.price_source, 
    s.currency, 
    s.return
FROM {{ source_table }} AS s
ON CONFLICT (date, barrid) DO UPDATE SET 
    price = excluded.price,
    market_cap = excluded.market_cap,
    price_source = excluded.price_source,
    currency = excluded.currency,
    return = excluded.return
;
