CREATE TABLE IF NOT EXISTS assets(
    date DATE,
    barrid TEXT,
    price DOUBLE,
    market_cap DOUBLE,
    price_source TEXT,
    currency TEXT,
    return DOUBLE,
    PRIMARY KEY (date, barrid)
);