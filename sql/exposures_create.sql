CREATE TABLE IF NOT EXISTS exposures(
    date DATE,
    barrid TEXT,
    factor TEXT,
    exposure DOUBLE,
    PRIMARY KEY (date, barrid, factor)
)
;