{#
    Default dbt behaviour concatenates the target schema with the model's custom
    schema, giving names like `gold_staging`. Here the catalog already provides
    the namespace, so use the custom schema as-is when one is set. This is what
    lets CI point everything at the throwaway `ci` schema by changing one
    profile value.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none or target.name == 'ci' -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
