{% macro generate_merge_query(source_schema, source_table, target_table, unique_key) %}

{# Get the list of columns from both source and target tables #}
{% set source_columns_query %}
    SELECT column_name
    FROM {{ source_schema }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{{ source_table }}'
{% endset %}

{% set target_columns_query %}
    SELECT column_name
    FROM {{ target_table }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{{ target_table }}'
{% endset %}

{% set source_columns = run_query(source_columns_query).columns() %}
{% set target_columns = run_query(target_columns_query).columns() %}

{# Dynamically determine which columns to include in the merge statement #}
{% set common_columns = [] %}
{% for column in source_columns %}
    {% if column in target_columns %}
        {% set common_columns = common_columns.append(column) %}
    {% endif %}
{% endfor %}

{# Construct the UPDATE SET clause dynamically for matched rows #}
{% set update_set_clause = [] %}
{% for column in common_columns %}
    {% set update_set_clause = update_set_clause.append("target." ~ column ~ " = source." ~ column) %}
{% endfor %}

{# Construct the INSERT clause dynamically #}
{% set insert_columns = [] %}
{% set insert_values = [] %}
{% for column in common_columns %}
    {% set insert_columns = insert_columns.append(column) %}
    {% set insert_values = insert_values.append("source." ~ column) %}
{% endfor %}

{# Generate the full MERGE SQL statement #}
MERGE INTO {{ target_table }} AS target
USING {{ source_schema }}.{{ source_table }} AS source
ON target.{{ unique_key }} = source.{{ unique_key }}

-- Update matched rows
WHEN MATCHED THEN
    UPDATE SET 
        {% for set_clause in update_set_clause %}
            {{ set_clause }}{% if not loop.last %}, {% endif %}
        {% endfor %}

-- Insert new rows
WHEN NOT MATCHED THEN
    INSERT ({{ insert_columns | join(', ') }})
    VALUES (
        {% for value in insert_values %}
            {{ value }}{% if not loop.last %}, {% endif %}
        {% endfor %}

;

{% endmacro %}
