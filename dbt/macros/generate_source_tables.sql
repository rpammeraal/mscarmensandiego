{% macro generate_source_tables(data_set,fqtn) %}
--  parameters:
--      data_set: name of data set.
--      fqtn: fully qualified table name.


{% set retrieve_unique_columns_sql %}

--  Collect list of source tables.
--  The dbt_lookup.column_lookup is used for simplicity. The
--  postgres meta data tables should be used in a production
--  environment.
SELECT DISTINCT
    table_name
FROM
    dbt_lookup.column_lookup
WHERE
    data_set='{{data_set}}'

{% endset %}

{% set results = run_query(retrieve_unique_columns_sql) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
