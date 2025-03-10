INSERT INTO factors
(
    date, 
    factor,
    return
)
SELECT 
    s.date, 
    s.factor,
    s.return
FROM {{ source_table }} AS s
ON CONFLICT (date, factor) DO UPDATE SET 
    return = excluded.return
;
