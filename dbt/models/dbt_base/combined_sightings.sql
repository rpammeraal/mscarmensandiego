{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': ['country','city','latitude','longitude'], 'unique': False}
        ]
    )
}}

{%- set all_source_tables = generate_source_tables('sighting',this) -%}
{%- set all_unique_columns = generate_unique_columns('sighting',this) -%}

--  Cannot put this in a jinja macro, as DBT does dependency checking 
--  before running jinja. Some solutions are possible when DBT is run
--  as part of Airflow.

-- depends_on: {{ ref('source_africa') }}
-- depends_on: {{ ref('source_america') }}
-- depends_on: {{ ref('source_asia') }}
-- depends_on: {{ ref('source_atlantic') }}
-- depends_on: {{ ref('source_australia') }}
-- depends_on: {{ ref('source_europe') }}
-- depends_on: {{ ref('source_indian') }}
-- depends_on: {{ ref('source_pacific') }}

{% for table_name in all_source_tables %}
SELECT
    {% for column in all_unique_columns %}
        {{column}} {%- if not loop.last %},{% endif -%}
    {% endfor %}
FROM
    {{ref(table_name)}}

{% if not loop.last %}UNION{% endif %}
{% endfor %}

