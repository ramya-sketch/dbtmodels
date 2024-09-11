{% macro generate_schema_name(custom_schema_name, node) %}
    {% if node.name in ['employee_sales_vw', 'product_sales_vw'] %}
        {{ 'reporting' }}
    {% elif custom_schema_name is none %}
        {{ target.schema }}
    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}
