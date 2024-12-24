{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'VER_ID, REL_WFLD_POL_SYM, REL_POL_FUNC_TYP, REL_SEQ_NR', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.rel_pol_func_transient', false, '(?i)TDB2WPLQ.REL_POL_FUNC/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.rel_pol_func_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(VER_ID,' | ',REL_WFLD_POL_SYM,' | ',REL_POL_FUNC_TYP,' | ',REL_SEQ_NR)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH rel_pol_func_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'REL_POL_FUNC_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.rel_pol_func_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.VER_ID AS SOURCE_VER_ID, s.REL_WFLD_POL_SYM AS SOURCE_REL_WFLD_POL_SYM, s.REL_POL_FUNC_TYP AS SOURCE_REL_POL_FUNC_TYP, s.REL_SEQ_NR AS SOURCE_REL_SEQ_NR,  -- Source IDs
        t.VER_ID AS TARGET_VER_ID, t.REL_WFLD_POL_SYM AS TARGET_REL_WFLD_POL_SYM, t.REL_POL_FUNC_TYP AS TARGET_REL_POL_FUNC_TYP, t.REL_SEQ_NR AS TARGET_REL_SEQ_NR,  -- Target IDs
        CASE 
            WHEN t.VER_ID OR t.REL_WFLD_POL_SYM OR t.REL_POL_FUNC_TYP OR t.REL_SEQ_NR IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM rel_pol_func_source_data s
    LEFT JOIN {{ this }} t
    ON s.VER_ID = t.VER_ID AND s.REL_WFLD_POL_SYM = t.REL_WFLD_POL_SYM AND s.REL_POL_FUNC_TYP = t.REL_POL_FUNC_TYP AND s.REL_SEQ_NR = t.REL_SEQ_NR
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM rel_pol_func_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.VER_ID = st.SOURCE_VER_ID AND s.REL_WFLD_POL_SYM = st.SOURCE_REL_WFLD_POL_SYM AND s.REL_POL_FUNC_TYP = st.SOURCE_REL_POL_FUNC_TYP AND s.REL_SEQ_NR = st.SOURCE_REL_SEQ_NR
    LEFT JOIN {{ this }} t
        ON s.VER_ID = t.VER_ID AND s.REL_WFLD_POL_SYM = t.REL_WFLD_POL_SYM AND s.REL_POL_FUNC_TYP = t.REL_POL_FUNC_TYP AND s.REL_SEQ_NR = t.REL_SEQ_NR
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'REL_POL_FUNC_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(VER_ID, REL_WFLD_POL_SYM, REL_POL_FUNC_TYP, REL_SEQ_NR), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
