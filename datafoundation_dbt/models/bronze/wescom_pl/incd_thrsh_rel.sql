{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'INCD_THRSH_VER_ID, INCD_CATG_CD, VLS_SRC_CD, BGN_LS_DT', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.incd_thrsh_rel_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.incd_thrsh_rel",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.incd_thrsh_rel AS SELECT * FROM bronze_zone_dev.wescom_pl.incd_thrsh_rel_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.incd_thrsh_rel_transient', true, '(?i)TDB2WPLQ.INCD_THRSH_REL/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.incd_thrsh_rel_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(INCD_THRSH_VER_ID,' | ',INCD_CATG_CD,' | ',VLS_SRC_CD,' | ',BGN_LS_DT)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.incd_thrsh_rel AS SELECT * FROM bronze_zone_dev.wescom_pl.incd_thrsh_rel_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH incd_thrsh_rel_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'INCD_THRSH_REL_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.incd_thrsh_rel_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.INCD_THRSH_VER_ID AS SOURCE_INCD_THRSH_VER_ID, s.INCD_CATG_CD AS SOURCE_INCD_CATG_CD, s.VLS_SRC_CD AS SOURCE_VLS_SRC_CD, s.BGN_LS_DT AS SOURCE_BGN_LS_DT,  -- Source IDs
        t.INCD_THRSH_VER_ID AS TARGET_INCD_THRSH_VER_ID, t.INCD_CATG_CD AS TARGET_INCD_CATG_CD, t.VLS_SRC_CD AS TARGET_VLS_SRC_CD, t.BGN_LS_DT AS TARGET_BGN_LS_DT,  -- Target IDs
        CASE 
            --WHEN t.INCD_THRSH_VER_ID OR t.INCD_CATG_CD OR t.VLS_SRC_CD OR t.BGN_LS_DT IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.INCD_THRSH_VER_ID) OR TO_CHAR(t.INCD_CATG_CD) OR TO_CHAR(t.VLS_SRC_CD) OR TO_CHAR(t.BGN_LS_DT) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM incd_thrsh_rel_source_data s
    LEFT JOIN {{ this }} t
    ON s.INCD_THRSH_VER_ID = t.INCD_THRSH_VER_ID AND s.INCD_CATG_CD = t.INCD_CATG_CD AND s.VLS_SRC_CD = t.VLS_SRC_CD AND s.BGN_LS_DT = t.BGN_LS_DT
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM incd_thrsh_rel_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.INCD_THRSH_VER_ID = st.SOURCE_INCD_THRSH_VER_ID AND s.INCD_CATG_CD = st.SOURCE_INCD_CATG_CD AND s.VLS_SRC_CD = st.SOURCE_VLS_SRC_CD AND s.BGN_LS_DT = st.SOURCE_BGN_LS_DT
    LEFT JOIN {{ this }} t
        ON s.INCD_THRSH_VER_ID = t.INCD_THRSH_VER_ID AND s.INCD_CATG_CD = t.INCD_CATG_CD AND s.VLS_SRC_CD = t.VLS_SRC_CD AND s.BGN_LS_DT = t.BGN_LS_DT
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'INCD_THRSH_REL_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(INCD_THRSH_VER_ID, INCD_CATG_CD, VLS_SRC_CD, BGN_LS_DT,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

    -- Set CREATED_DATETIME only on inserts
    , CASE 
        WHEN UPSERT_FLAG = 'I' THEN CURRENT_TIMESTAMP() 
        ELSE AUDIT_CREATED_DATETIME  -- Keep the same value for updates
    END AS AUDIT_CREATED_DATETIME
    
    -- Set CREATED_BY only on inserts
    , CASE 
        WHEN UPSERT_FLAG = 'I' THEN CURRENT_USER()  
        ELSE AUDIT_CREATED_BY  -- Keep the same value for updates
    END AS AUDIT_CREATED_BY                          

    , CURRENT_TIMESTAMP() as AUDIT_MODIFIED_DATETIME
    , CURRENT_USER() as AUDIT_MODIFIED_BY

FROM joined_data
