{%- set allq = dynamic_crosstab('dbt_intermediate.top_three_behavior_probability_by_month_staging','behavior','month','probability','FLOAT') -%}

-- depends_on: {{ ref('top_three_behavior_probability_by_month_staging') }}

{% for i in allq %}
    {{i}}
{% endfor %}
