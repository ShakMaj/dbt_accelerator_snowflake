{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'VEND_PRDT_NM, PRDT_CTGZ_CD, ST_CD, USE_STRT_DT', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel AS SELECT * FROM bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel_transient', true, '(?i)TDB2WPLQ.PRCH_PRDT_INFO_STATE_REL/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(VEND_PRDT_NM,' | ',PRDT_CTGZ_CD,' | ',ST_CD,' | ',USE_STRT_DT)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.prch_prdt_info_state_rel AS SELECT * FROM bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH prch_prdt_info_state_rel_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'PRCH_PRDT_INFO_STATE_REL_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.prch_prdt_info_state_rel_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.VEND_PRDT_NM AS SOURCE_VEND_PRDT_NM, s.PRDT_CTGZ_CD AS SOURCE_PRDT_CTGZ_CD, s.ST_CD AS SOURCE_ST_CD, s.USE_STRT_DT AS SOURCE_USE_STRT_DT,  -- Source IDs
        t.VEND_PRDT_NM AS TARGET_VEND_PRDT_NM, t.PRDT_CTGZ_CD AS TARGET_PRDT_CTGZ_CD, t.ST_CD AS TARGET_ST_CD, t.USE_STRT_DT AS TARGET_USE_STRT_DT,  -- Target IDs
        CASE 
            --WHEN t.VEND_PRDT_NM OR t.PRDT_CTGZ_CD OR t.ST_CD OR t.USE_STRT_DT IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.VEND_PRDT_NM) OR TO_CHAR(t.PRDT_CTGZ_CD) OR TO_CHAR(t.ST_CD) OR TO_CHAR(t.USE_STRT_DT) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM prch_prdt_info_state_rel_source_data s
    LEFT JOIN {{ this }} t
    ON s.VEND_PRDT_NM = t.VEND_PRDT_NM AND s.PRDT_CTGZ_CD = t.PRDT_CTGZ_CD AND s.ST_CD = t.ST_CD AND s.USE_STRT_DT = t.USE_STRT_DT
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM prch_prdt_info_state_rel_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.VEND_PRDT_NM = st.SOURCE_VEND_PRDT_NM AND s.PRDT_CTGZ_CD = st.SOURCE_PRDT_CTGZ_CD AND s.ST_CD = st.SOURCE_ST_CD AND s.USE_STRT_DT = st.SOURCE_USE_STRT_DT
    LEFT JOIN {{ this }} t
        ON s.VEND_PRDT_NM = t.VEND_PRDT_NM AND s.PRDT_CTGZ_CD = t.PRDT_CTGZ_CD AND s.ST_CD = t.ST_CD AND s.USE_STRT_DT = t.USE_STRT_DT
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'PRCH_PRDT_INFO_STATE_REL_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(VEND_PRDT_NM, PRDT_CTGZ_CD, ST_CD, USE_STRT_DT,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
