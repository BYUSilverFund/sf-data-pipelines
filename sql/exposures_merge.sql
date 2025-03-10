INSERT INTO exposures
(
    date, 
    barrid,
    factor, 
    exposure
)
SELECT 
    s.date, 
    s.barrid,
    s.factor, 
    s.exposure
FROM {{ source_table }} AS s
ON CONFLICT (date, barrid, factor) DO UPDATE SET 
    exposure = excluded.exposure
;
