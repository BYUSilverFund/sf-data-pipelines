CREATE OR REPLACE TEMPORARY TABLE {{ transform_table }} AS (
    SELECT
        "!Factor"::STRING AS factor,
        DlyReturn::DOUBLE AS return,
        STRPTIME(DataDate::STRING, '%Y%m%d') AS date
    FROM {{ stage_table }}
    WHERE factor != '[End of File]'
)
;
