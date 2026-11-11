{# Use the configured schema verbatim (silver / gold) instead of <target>_<custom>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
