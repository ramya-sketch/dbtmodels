{% macro get_incremental_merge_sql(target_relation, source_relation, unique_key) %}
    {% if not source_relation %}
        {{ exceptions.raise_compiler_error("parameter 'source_relation' was not provided") }}
    {% endif %}

    merge into {{ target_relation }} as target
    using {{ source_relation }} as source
    on target.{{ unique_key }} = source.{{ unique_key }}
    
    when matched then
        update set
        {% for col in source_relation.columns %}
            target.{{ col }} = source.{{ col }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    
    when not matched then
        insert (
            {% for col in source_relation.columns %}
                {{ col }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        values (
            {% for col in source_relation.columns %}
                source.{{ col }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        );
{% endmacro %}
