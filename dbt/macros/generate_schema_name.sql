{#
  Override dbt's default schema naming. Out of the box dbt would create
  "<target_schema>_staging", "<target_schema>_marts" etc.; we want the exact
  layer names (staging / marts) so the warehouse reads as a clean medallion
  layout for anyone connecting with psql or Power BI.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
