CREATE TEMPORARY TABLE {{ wide_table }} AS
    SELECT * FROM (
        PIVOT {{ long_table }}
        ON factor
        USING MAX(return) 
    )
;