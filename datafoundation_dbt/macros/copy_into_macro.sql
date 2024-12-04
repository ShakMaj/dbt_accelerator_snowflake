-- macros/copy_into_macro.sql

{% macro copy_into_macro(stage_name, target_table, force_load, pattern) %}
    COPY INTO {{ target_table }}
    FROM @{{ stage_name }}
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE'
    FORCE = {{ force_load }}
    INCLUDE_METADATA = (
    ingestdate = METADATA$START_SCAN_TIME, filename = METADATA$FILENAME
    )
    PATTERN =  '{{ pattern }}';


{% endmacro %}
