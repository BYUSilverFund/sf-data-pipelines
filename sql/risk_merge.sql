INSERT INTO assets
(
    barrid, 
    date, 
    yield,
    total_risk,
    specific_risk,
    historical_beta,
    predicted_beta
)
SELECT 
    s.barrid, 
    s.date, 
    s.yield,
    s.total_risk,
    s.specific_risk,
    s.historical_beta,
    s.predicted_beta
FROM {{ source_table }} AS s
ON CONFLICT (barrid, date) DO UPDATE SET 
    yield = excluded.yield,
    total_risk = excluded.total_risk,
    specific_risk = excluded.specific_risk,
    historical_beta = excluded.historical_beta,
    predicted_beta = excluded.predicted_beta
