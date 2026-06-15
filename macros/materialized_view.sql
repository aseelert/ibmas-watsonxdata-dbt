{% materialization materialized_view, adapter="watsonx_presto" %}

  {%- set existing_relation = adapter.get_relation(
        database=this.database,
        schema=this.schema,
        identifier=this.identifier
  ) -%}

  {%- if existing_relation is not none -%}
    {%- if existing_relation.type == 'view' -%}
      {% call statement('drop_view', fetch_result=False, auto_begin=False) %}
        DROP VIEW IF EXISTS {{ existing_relation }}
      {% endcall %}
    {%- else -%}
      {% call statement('drop_materialized_view', fetch_result=False, auto_begin=False) %}
        DROP MATERIALIZED VIEW IF EXISTS {{ existing_relation }}
      {% endcall %}
    {%- endif -%}
  {%- endif -%}

  {% call statement("main") %}
    CREATE MATERIALIZED VIEW {{ this }} AS {{ sql }}
  {% endcall %}

  {% call statement('refresh_materialized_view', fetch_result=False, auto_begin=False) %}
    REFRESH MATERIALIZED VIEW {{ this }}
  {% endcall %}

  {{ return({'relations': [this]}) }}

{% endmaterialization %}
