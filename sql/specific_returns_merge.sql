INSERT INTO assets
(
    barrid, 
    date, 
    specific_return
)
SELECT 
    s.barrid, 
    s.date, 
    s.specific_return
FROM {{ source_table }} AS s
ON CONFLICT (date, barrid) DO UPDATE SET 
    specific_return = excluded.specific_return
;
