{#- Managed Iceberg table on Snowflake storage (EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED').
    dbt-snowflake's built-in `table_format='iceberg'` always adds BASE_LOCATION, which Snowflake
    storage rejects, hence this small materialization.
    First run: CREATE ICEBERG TABLE ... AS; later runs: INSERT OVERWRITE, so Iceberg snapshots
    (and Snowflake time travel) accumulate across runs. -#}
{% materialization iceberg_table, adapter='snowflake' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') %}
    {% if existing is none %}
      create iceberg table {{ target_relation }}
        catalog = 'SNOWFLAKE' external_volume = 'SNOWFLAKE_MANAGED'
      as {{ sql }}
    {% else %}
      insert overwrite into {{ target_relation }} {{ sql }}
    {% endif %}
  {% endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
