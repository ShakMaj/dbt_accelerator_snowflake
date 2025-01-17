{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'BOAT_ID, ADD_INT_ID', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.boat_add_int_rel_transient', false, '(?i)TDB2WPLQ.BOAT_ADD_INT_REL/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.boat_add_int_rel_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(BOAT_ID,' | ',ADD_INT_ID)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH boat_add_int_rel_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'BOAT_ADD_INT_REL_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.boat_add_int_rel_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.BOAT_ID AS SOURCE_BOAT_ID, s.ADD_INT_ID AS SOURCE_ADD_INT_ID,  -- Source IDs
        t.BOAT_ID AS TARGET_BOAT_ID, t.ADD_INT_ID AS TARGET_ADD_INT_ID,  -- Target IDs
        CASE 
            WHEN t.BOAT_ID OR t.ADD_INT_ID IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM boat_add_int_rel_source_data s
    LEFT JOIN {{ this }} t
    ON s.BOAT_ID = t.BOAT_ID AND s.ADD_INT_ID = t.ADD_INT_ID
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM boat_add_int_rel_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.BOAT_ID = st.SOURCE_BOAT_ID AND s.ADD_INT_ID = st.SOURCE_ADD_INT_ID
    LEFT JOIN {{ this }} t
        ON s.BOAT_ID = t.BOAT_ID AND s.ADD_INT_ID = t.ADD_INT_ID
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'BOAT_ADD_INT_REL_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(BOAT_ID, ADD_INT_ID), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

    -- Set CREATED_DATETIME only on inserts
    , CASE 
        WHEN FLAG = 'I' THEN CURRENT_TIMESTAMP() 
        ELSE AUDIT_CREATED_DATETIME  -- Keep the same value for updates
    END AS AUDIT_CREATED_DATETIME
    
    -- Set CREATED_BY only on inserts
    , CASE 
        WHEN FLAG = 'I' THEN CURRENT_USER()  
        ELSE AUDIT_CREATED_BY  -- Keep the same value for updates
    END AS AUDIT_CREATED_BY                          

    , CURRENT_TIMESTAMP() as AUDIT_MODIFIED_DATETIME
    , CURRENT_USER() as AUDIT_MODIFIED_BY

FROM joined_data
