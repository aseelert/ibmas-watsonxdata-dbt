{% macro create_medallion_schemas() %}
  {% set base_schema = env_var('WXD_SCHEMA', 'dbt_demo') %}
  {% set schemas = [
    env_var('WXD_RAW_SCHEMA', base_schema ~ '_raw'),
    env_var('WXD_BRONZE_SCHEMA', base_schema ~ '_bronze'),
    env_var('WXD_SILVER_SCHEMA', base_schema ~ '_silver'),
    env_var('WXD_GOLD_SCHEMA', base_schema ~ '_gold')
  ] %}
  {% set location_base = env_var('WXD_SCHEMA_LOCATION_BASE', '') %}

  {% for schema_name in schemas %}
    {% set sql %}
      create schema if not exists {{ target.catalog }}.{{ schema_name }}
      {%- if location_base %}
        with (location = '{{ location_base.rstrip("/") }}/{{ schema_name }}')
      {%- endif %}
    {% endset %}
    {% do run_query(sql) %}
    {{ log("Ensured schema exists: " ~ target.catalog ~ "." ~ schema_name, info=True) }}
  {% endfor %}
{% endmacro %}
