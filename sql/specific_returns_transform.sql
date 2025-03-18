CREATE OR REPLACE TEMPORARY TABLE {{ transform_table }} AS (
    SELECT
        "!Barrid"::STRING AS barrid,
        SpecificReturn::DOUBLE AS specific_return,
        STRPTIME(DataDate::STRING, '%Y%m%d') AS date
    FROM {{ stage_table }}
    WHERE barrid != '[End of File]'
)
;
