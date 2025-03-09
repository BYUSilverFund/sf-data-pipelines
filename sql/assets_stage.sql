DROP TABLE IF EXISTS assets_{{ date }}_stg;
CREATE TABLE assets_{{ date }}_stg AS SELECT * FROM df;