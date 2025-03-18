CREATE TABLE IF NOT EXISTS factors(
    date DATE,
    factor TEXT,
    return DOUBLE,
    PRIMARY KEY (date, factor)
)
;