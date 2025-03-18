CREATE OR REPLACE TEMPORARY TABLE {{ transform_table }} AS (
    SELECT
        "!Factor1"::STRING AS factor1,
        Factor2::STRING AS factor2,
        VarCovar::DOUBLE AS covariance,
        STRPTIME(DataDate::STRING, '%Y%m%d') AS date
    FROM {{ stage_table }}
    WHERE factor1 != '[End of File]'
)
;
