UPDATE assets
SET 
    rootid = s.rootid,
    name = s.name,
    instrument = s.instrument,
    issuerid = s.issuerid,
    iso_country_code = s.iso_country_code,
    iso_currency_code = s.iso_currency_code
FROM {{ source_table }} AS s
WHERE assets.date = s.date AND assets.barrid = s.barrid;