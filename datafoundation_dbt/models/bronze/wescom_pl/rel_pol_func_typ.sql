{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'REL_POL_FUNC_TYP', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.rel_pol_func_typ_transient', false, '(?i)TDB2WPLQ.REL_POL_FUNC_TYP/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.rel_pol_func_typ_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(REL_POL_FUNC_TYP)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH rel_pol_func_typ_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'REL_POL_FUNC_TYP_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.rel_pol_func_typ_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.REL_POL_FUNC_TYP AS SOURCE_REL_POL_FUNC_TYP,  -- Source IDs
        t.REL_POL_FUNC_TYP AS TARGET_REL_POL_FUNC_TYP,  -- Target IDs
        CASE 
            WHEN t.REL_POL_FUNC_TYP IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM rel_pol_func_typ_source_data s
    LEFT JOIN {{ this }} t
    ON s.REL_POL_FUNC_TYP = t.REL_POL_FUNC_TYP
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM rel_pol_func_typ_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.REL_POL_FUNC_TYP = st.SOURCE_REL_POL_FUNC_TYP
    LEFT JOIN {{ this }} t
        ON s.REL_POL_FUNC_TYP = t.REL_POL_FUNC_TYP
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'REL_POL_FUNC_TYP_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(REL_POL_FUNC_TYP), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
