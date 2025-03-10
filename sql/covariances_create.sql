CREATE TABLE IF NOT EXISTS covariances(
    date DATE,
    factor1 TEXT,
    factor2 TEXT,
    covariance DOUBLE,
    PRIMARY KEY (date, factor1, factor2)
)
;