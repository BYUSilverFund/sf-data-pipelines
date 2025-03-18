CREATE OR REPLACE VIEW assets_clean AS
SELECT 
    date,
    barrid,
    cins,
    cusip,
    isin,
    localid,
    LAST_VALUE(ticker IGNORE NULLS) OVER (PARTITION BY barrid ORDER BY date) AS ticker,
    name,
    instrument,
    issuerid,
    iso_country_code,
    iso_currency_code,
    price,
    market_cap,
    price_source,
    currency,
    return,
    specific_return,
    yield,
    total_risk,
    specific_risk,
    historical_beta,
    predicted_beta,
    LAST_VALUE(russell_2000 IGNORE NULLS) OVER (PARTITION BY barrid ORDER BY date) AS russell_2000,
    LAST_VALUE(russell_1000 IGNORE NULLS) OVER (PARTITION BY barrid ORDER BY date) AS russell_1000,
    LAST_VALUE(russell_3000_weight IGNORE NULLS) OVER (PARTITION BY barrid ORDER BY date) AS russell_3000_weight,
    LAST_VALUE(russell_2000_weight IGNORE NULLS) OVER (PARTITION BY barrid ORDER BY date) AS russell_2000_weight,
    LAST_VALUE(russell_1000_weight IGNORE NULLS) OVER (PARTITION BY barrid ORDER BY date) AS russell_1000_weight
FROM assets
WHERE rootid = barrid
ORDER BY barrid, date
;