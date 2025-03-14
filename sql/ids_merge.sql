UPDATE assets
SET 
    cins = s.cins,
    cusip = s.cusip,
    isin = s.isin,
    localid = s.localid
FROM {{ source_table }} AS s
WHERE assets.date = s.date AND assets.barrid = s.barrid;