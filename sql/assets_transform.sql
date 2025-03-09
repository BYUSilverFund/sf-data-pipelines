DROP TABLE IF EXISTS assets_{{ date }}_xf;
CREATE TABLE assets_{{ date }}_xf AS (
    SELECT *
    FROM assets_{{ date }}_stg
)
;
