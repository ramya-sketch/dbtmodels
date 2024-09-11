{% macro snowflake_schema(custom_schema_name, node) %}
    {% if node.name == 'snowflake_model' %}
        {{ 'reporting' }}
    {% elif custom_schema_name is none %}
        {{ target.schema }}  -- default behavior for other models
    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}
