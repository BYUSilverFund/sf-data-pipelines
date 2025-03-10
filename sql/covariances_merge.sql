INSERT INTO covariances
(
    date, 
    factor1, 
    factor2,
    covariance
)
SELECT 
    s.date, 
    s.factor1,
    s.factor2, 
    s.covariance
FROM {{ source_table }} AS s
ON CONFLICT (date, factor1, factor2) DO UPDATE SET 
    covariance = excluded.covariance
;
