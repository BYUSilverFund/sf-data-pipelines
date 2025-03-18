CREATE OR REPLACE TEMPORARY TABLE {{ transform_table }} AS (
    SELECT
        "!Barrid"::STRING AS barrid,
        Price::DOUBLE AS price,
        Capt::DOUBLE AS market_cap,
        PriceSource::STRING AS price_source,
        Currency::STRING AS currency,
        "DlyReturn%"::DOUBLE AS return,
        STRPTIME(DataDate::STRING, '%Y%m%d') AS date
    FROM {{ stage_table }}
    WHERE barrid != '[End of File]'
)
;
