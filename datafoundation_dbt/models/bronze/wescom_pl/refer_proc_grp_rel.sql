{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'PROC_GRP_ID, ACTV_CD, DSPL_SEQ_NR', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.refer_proc_grp_rel_transient', false, '(?i)TDB2WPLQ.REFER_PROC_GRP_REL/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.refer_proc_grp_rel_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(PROC_GRP_ID,' | ',ACTV_CD,' | ',DSPL_SEQ_NR)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH refer_proc_grp_rel_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'REFER_PROC_GRP_REL_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.refer_proc_grp_rel_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.PROC_GRP_ID AS SOURCE_PROC_GRP_ID, s.ACTV_CD AS SOURCE_ACTV_CD, s.DSPL_SEQ_NR AS SOURCE_DSPL_SEQ_NR,  -- Source IDs
        t.PROC_GRP_ID AS TARGET_PROC_GRP_ID, t.ACTV_CD AS TARGET_ACTV_CD, t.DSPL_SEQ_NR AS TARGET_DSPL_SEQ_NR,  -- Target IDs
        CASE 
            WHEN t.PROC_GRP_ID OR t.ACTV_CD OR t.DSPL_SEQ_NR IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM refer_proc_grp_rel_source_data s
    LEFT JOIN {{ this }} t
    ON s.PROC_GRP_ID = t.PROC_GRP_ID AND s.ACTV_CD = t.ACTV_CD AND s.DSPL_SEQ_NR = t.DSPL_SEQ_NR
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM refer_proc_grp_rel_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.PROC_GRP_ID = st.SOURCE_PROC_GRP_ID AND s.ACTV_CD = st.SOURCE_ACTV_CD AND s.DSPL_SEQ_NR = st.SOURCE_DSPL_SEQ_NR
    LEFT JOIN {{ this }} t
        ON s.PROC_GRP_ID = t.PROC_GRP_ID AND s.ACTV_CD = t.ACTV_CD AND s.DSPL_SEQ_NR = t.DSPL_SEQ_NR
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'REFER_PROC_GRP_REL_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(PROC_GRP_ID, ACTV_CD, DSPL_SEQ_NR), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
