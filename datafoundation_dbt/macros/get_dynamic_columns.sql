-- macros/get_dynamic_columns.sql

{% macro get_dynamic_columns(schema, table) %}
    {% set query %}
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{{ schema }}'
          AND TABLE_NAME = '{{ table }}'
          AND COLUMN_NAME NOT IN ('CREATED_BY','CREATED_DATETIME', 'MODIFIED_BY', 'MODIFIED_DATETIME')
          ORDER BY ORDINAL_POSITION
    {% endset %}

    {% set result = run_query(query) %}
    
    {% if result %}
        {% set columns = result.rows | map(attribute='COLUMN_NAME') | join(', ') %}
        {{ return(columns) }}
    {% else %}
        {% do log('No columns found or query failed.', info=True) %}
        {{ return('') }}  -- Return an empty string if no columns are found
    {% endif %}
{% endmacro %}
