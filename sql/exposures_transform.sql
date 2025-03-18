CREATE OR REPLACE TEMPORARY TABLE {{ transform_table }} AS (
    SELECT
        "!Barrid"::STRING AS barrid,
        Factor::STRING AS factor,
        Exposure::DOUBLE AS exposure,
        STRPTIME(DataDate::STRING, '%Y%m%d') AS date
    FROM {{ stage_table }}
    WHERE barrid != '[End of File]'
)
;
