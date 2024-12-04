
{{ config(
    profile = 'snowflake',
    materialized = 'table',
    
)}}

WITH field_names AS (
    SELECT
        field_name
    FROM {{ ref('fields_extraction') }}  -- Reference the previous model that extracted the field names
),

dynamic_query AS (
    SELECT
        STRING_AGG(field_name, ', ') AS query_columns  -- Combine the field names into a comma-separated list
    FROM
        field_names
)

SELECT 
    'SELECT ' || query_columns || ' FROM your_table_name' AS dynamic_sql
FROM 
    dynamic_query
