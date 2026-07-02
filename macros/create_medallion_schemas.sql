{#
  create_medallion_schemas()
  ==========================

  PURPOSE
    One-shot idempotent macro that pre-creates all four dbt medallion schemas
    (_raw, _bronze, _silver, _gold) inside the Iceberg catalog before any dbt
    model runs. It issues `CREATE SCHEMA IF NOT EXISTS` through Presto, so the
    namespace exists on-disk from the first `dbt run` onward and never needs a
    manual setup step.

  WHY IT EXISTS — THE .db SUFFIX PROBLEM
    When Iceberg Spark (or dbt) writes a table into a schema that does not yet
    exist on object storage, the Iceberg catalog auto-creates the namespace
    directory with a ".db" suffix  (e.g. dbt_demo_bronze.db/).  Once the
    directory has that suffix it is permanent: you cannot rename it without
    dropping and recreating the schema, and it differs from the layout the Presto
    engine expects (it wants dbt_demo_bronze/ at the bucket root, no suffix).

    Pre-creating via Presto SQL avoids this entirely: Presto respects the catalog
    warehouse root and creates the directory WITHOUT a ".db" suffix.  Both dbt
    models and the Spark ETL job then read/write to the same consistent path.

    The identical problem exists for the Spark path, which is why
    submit_spark_application.py also pre-creates spark_demo_bronze/silver/gold
    via Presto before submitting the PySpark job (see _ensure_spark_schemas_via_presto()).

  WHEN IT IS CALLED
    This macro is invoked from the on-run-start hook in dbt_project.yml:

        on-run-start:
          - "{{ create_medallion_schemas() }}"

    It runs once at the very beginning of every `dbt run` / `dbt build` /
    `dbt seed` invocation, before any model is compiled or executed.  It is a
    no-op if all four schemas already exist (IF NOT EXISTS).

  ENV VARS READ (via env_var())
    WXD_SCHEMA              — base schema prefix, default: "dbt_demo"
    WXD_RAW_SCHEMA          — override for the raw schema
    WXD_BRONZE_SCHEMA       — override for the bronze schema
    WXD_SILVER_SCHEMA       — override for the silver schema
    WXD_GOLD_SCHEMA         — override for the gold schema
    WXD_SCHEMA_LOCATION_BASE — optional: S3 location prefix; when set, each
                               schema is created WITH (location = '<base>/<schema>').
                               Leave UNSET (recommended) to use the catalog
                               default warehouse root — this keeps dbt schemas
                               co-located with the Spark schemas that land at
                               the bucket root by default.

  IDEMPOTENCY
    Safe to run repeatedly.  `CREATE SCHEMA IF NOT EXISTS` is a no-op when the
    schema already exists; it does not drop or modify any existing tables.
#}
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
