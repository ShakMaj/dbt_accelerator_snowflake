{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'VER_ID, SYS_OF_ENT_CD, SYS_ENT_VLS_CD, VLS_SRC_CD, CAT_LS_IND', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.violt_ls_map_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.violt_ls_map",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.violt_ls_map AS SELECT * FROM bronze_zone_dev.wescom_pl.violt_ls_map_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.violt_ls_map_transient', true, '(?i)TDB2WPLQ.VIOLT_LS_MAP/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.violt_ls_map_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(VER_ID,' | ',SYS_OF_ENT_CD,' | ',SYS_ENT_VLS_CD,' | ',VLS_SRC_CD,' | ',CAT_LS_IND)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.violt_ls_map AS SELECT * FROM bronze_zone_dev.wescom_pl.violt_ls_map_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH violt_ls_map_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'VIOLT_LS_MAP_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.violt_ls_map_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.VER_ID AS SOURCE_VER_ID, s.SYS_OF_ENT_CD AS SOURCE_SYS_OF_ENT_CD, s.SYS_ENT_VLS_CD AS SOURCE_SYS_ENT_VLS_CD, s.VLS_SRC_CD AS SOURCE_VLS_SRC_CD, s.CAT_LS_IND AS SOURCE_CAT_LS_IND,  -- Source IDs
        t.VER_ID AS TARGET_VER_ID, t.SYS_OF_ENT_CD AS TARGET_SYS_OF_ENT_CD, t.SYS_ENT_VLS_CD AS TARGET_SYS_ENT_VLS_CD, t.VLS_SRC_CD AS TARGET_VLS_SRC_CD, t.CAT_LS_IND AS TARGET_CAT_LS_IND,  -- Target IDs
        CASE 
            --WHEN t.VER_ID OR t.SYS_OF_ENT_CD OR t.SYS_ENT_VLS_CD OR t.VLS_SRC_CD OR t.CAT_LS_IND IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.VER_ID) OR TO_CHAR(t.SYS_OF_ENT_CD) OR TO_CHAR(t.SYS_ENT_VLS_CD) OR TO_CHAR(t.VLS_SRC_CD) OR TO_CHAR(t.CAT_LS_IND) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM violt_ls_map_source_data s
    LEFT JOIN {{ this }} t
    ON s.VER_ID = t.VER_ID AND s.SYS_OF_ENT_CD = t.SYS_OF_ENT_CD AND s.SYS_ENT_VLS_CD = t.SYS_ENT_VLS_CD AND s.VLS_SRC_CD = t.VLS_SRC_CD AND s.CAT_LS_IND = t.CAT_LS_IND
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM violt_ls_map_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.VER_ID = st.SOURCE_VER_ID AND s.SYS_OF_ENT_CD = st.SOURCE_SYS_OF_ENT_CD AND s.SYS_ENT_VLS_CD = st.SOURCE_SYS_ENT_VLS_CD AND s.VLS_SRC_CD = st.SOURCE_VLS_SRC_CD AND s.CAT_LS_IND = st.SOURCE_CAT_LS_IND
    LEFT JOIN {{ this }} t
        ON s.VER_ID = t.VER_ID AND s.SYS_OF_ENT_CD = t.SYS_OF_ENT_CD AND s.SYS_ENT_VLS_CD = t.SYS_ENT_VLS_CD AND s.VLS_SRC_CD = t.VLS_SRC_CD AND s.CAT_LS_IND = t.CAT_LS_IND
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'VIOLT_LS_MAP_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(VER_ID, SYS_OF_ENT_CD, SYS_ENT_VLS_CD, VLS_SRC_CD, CAT_LS_IND,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
