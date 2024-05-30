{%- set all_columns = generate_std_columns('sighting',this) -%}
SELECT
    {% for column in all_columns %}
        {{column}} {%- if not loop.last %},{% endif -%}
    {% endfor %}
FROM
    {{source('dbt_raw','AUSTRALIA')}}
