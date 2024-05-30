{% macro generate_std_columns(data_set,fqtn) %}
--  parameters:
--      data_set: name of data set.
--      fqtn: fully qualified table name.

{% set retrieve_std_columns_sql %}

--  Create complete column definition as needed in a CREATE VIEW/TABLE statement
WITH fqtn AS
(
    --  Keep this simple, strip all but actual table name for the assignment
    --  The complete fully qualified table name should be preserved in a 
    --  production environment
    SELECT STRING_TO_ARRAY('{{fqtn}}','.') AS arr
),
table_name AS
(
    SELECT
        REPLACE((SELECT v FROM unnest(arr) WITH ORDINALITY AS t (v, o) ORDER BY o DESC LIMIT 1)::VARCHAR,'"','') AS table_name
    FROM
        fqtn
)
SELECT 
    --  There is more magic to be applied such as aligning the 'AS' keyword to make
    --  generated code more readable. That is left as homework :)
    '"' || cl.org_column || '"::' || dl.data_type || ' AS ' || cl.new_column
FROM 
    dbt_lookup.column_lookup cl
        JOIN dbt_lookup.datatype_lookup dl ON
            cl.data_set=dl.data_set AND
            cl.new_column=dl.field
        CROSS JOIN table_name AS tn
WHERE 
    cl.data_set='{{data_set}}' AND
    cl.table_name=tn.table_name

{% endset %}

{% set results = run_query(retrieve_std_columns_sql) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
