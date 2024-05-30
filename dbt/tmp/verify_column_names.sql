--  Verify that the use of `new_column` value is used consistently
--  across all source tables
SELECT
    new_column,
    COUNT(*)                        AS occurences,
    STRING_AGG(table_name,',')      AS table_names
FROM
    dbt_lookup.column_lookup
GROUP BY
    new_column
;
