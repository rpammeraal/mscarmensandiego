{% macro dynamic_crosstab(source_table,pivot_column,head_column,metric,metric_type) %}
--  parameters:
--      source_table: source table used for all data
--      pivot_column: name of datapoint to appear as column headings
--      head_column: name of datapoint to appear as row headings
--      metric: name of datapoint to use as metric
--      metric_type: datatype of metric


{% set retrieve_unique_columns_sql %}

--  As Postgres does not support dynamic columns in its CROSSTAB statement,
--  we'll employ Jinja to make this happen.
WITH unique_pivot_columns AS
(
    SELECT DISTINCT
        {{pivot_column}}
    FROM
        {{source_table}}
)
SELECT DISTINCT
    'SELECT * ' ||
    'FROM ' ||
    'CROSSTAB ( ' ||
        CHR(39) || 'SELECT ' ||
            '{{head_column}},{{pivot_column}},{{metric}} ' ||
        'FROM ' ||
            '{{source_table}} ' ||
        CHR(39) || ',' ||
        CHR(39) || 'SELECT DISTINCT {{pivot_column}} FROM {{source_table}} ORDER BY 1' || CHR(39) ||
    ') AS ct ({{head_column}} text, "' || STRING_AGG({{pivot_column}},'" {{metric_type}},"' ORDER BY {{pivot_column}}) || '" {{metric_type}})'
FROM
    unique_pivot_columns

{% endset %}

{% set results = run_query(retrieve_unique_columns_sql) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
