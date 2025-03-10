CREATE OR REPLACE TEMPORARY TABLE {{ transform_table }} AS (
    SELECT
        "!Barrid"::STRING AS barrid,
        "Yield%"::DOUBLE AS yield,
        "TotalRisk%"::DOUBLE AS total_risk,
        "SpecRisk%"::DOUBLE AS specific_risk,
        HistBeta::DOUBLE AS historical_beta,
        PredBeta::DOUBLE AS predicted_beta,
        STRPTIME(DataDate::STRING, '%Y%m%d') AS date
    FROM {{ stage_table }}
    WHERE barrid != '[End of File]'
)
;