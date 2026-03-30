{% macro ensure_rows_affected_audit_table() %}
  {% if execute %}
    {% set create_schema %}
      create schema if not exists DQLABS_QA.RUNFAILURE
    {% endset %}
    {% do run_query(create_schema) %}
    {% set create_table %}
      create table if not exists DQLABS_QA.RUNFAILURE.DBT_ROWS_AFFECTED_LOG (
        run_at timestamp_tz,
        invocation_id varchar(255),
        resource_type varchar(64),
        node_name varchar(255),
        status varchar(64),
        rows_affected number(38,0),
        execution_time_sec float,
        message varchar(16777216)
      )
    {% endset %}
    {% do run_query(create_table) %}
  {% endif %}
{% endmacro %}

{% macro log_rows_affected_from_run(results) %}
  {% if execute and results %}
    {% for res in results %}
      {% if res.node.resource_type == 'model' %}
        {% set ra = none %}
        {% if res.adapter_response %}
          {% set ra = res.adapter_response.get('rows_affected', res.adapter_response.get('query_rows_affected', none)) %}
        {% endif %}
        {% if ra is none %}
          {% set ra_sql = 'null::number' %}
        {% else %}
          {% set ra_sql = (ra | string) ~ '::number' %}
        {% endif %}
        {% set msg = (res.message or '') | replace("'", "''") %}
        {% set nm = res.node.name | replace("'", "''") %}
        {% set st = res.status | string | replace("'", "''") %}
        {% set rt = res.node.resource_type | string | replace("'", "''") %}
        {% set et = res.execution_time | default(0) %}
        {% set sql %}
          insert into DQLABS_QA.RUNFAILURE.DBT_ROWS_AFFECTED_LOG (
            run_at,
            invocation_id,
            resource_type,
            node_name,
            status,
            rows_affected,
            execution_time_sec,
            message
          ) select
            current_timestamp(),
            '{{ invocation_id }}',
            '{{ rt }}',
            '{{ nm }}',
            '{{ st }}',
            {{ ra_sql }},
            {{ et }}::float,
            '{{ msg }}'
        {% endset %}
        {% do run_query(sql) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro claim_analytics_hooks_run_start() %}
  {% if var('claim_analytics_audit', false) %}
    {{ ensure_rows_affected_audit_table() }}
  {% endif %}
{% endmacro %}

{% macro claim_analytics_hooks_run_end(results) %}
  {% if var('claim_analytics_audit', false) %}
    {{ log_rows_affected_from_run(results) }}
  {% endif %}
{% endmacro %}
