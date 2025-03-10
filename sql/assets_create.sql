CREATE TABLE IF NOT EXISTS assets(
    date DATE,
    barrid TEXT,
    price DOUBLE,
    market_cap DOUBLE,
    price_source TEXT,
    currency TEXT,
    return DOUBLE,
    yield DOUBLE,
    total_risk DOUBLE,
    specific_risk DOUBLE,
    historical_beta DOUBLE,
    predicted_beta DOUBLE,
    PRIMARY KEY (date, barrid)
);