UPDATE assets
SET 
    ticker = s.ticker,
    russell_2000 = s.russell_2000,
    russell_1000 = s.russell_1000,
    russell_3000_weight = s.russell_3000_weight,
    russell_2000_weight = s.russell_2000_weight,
    russell_1000_weight = s.russell_1000_weight
FROM {{ source_table }} AS s
WHERE assets.date = s.date AND assets.cusip = s.cusip;