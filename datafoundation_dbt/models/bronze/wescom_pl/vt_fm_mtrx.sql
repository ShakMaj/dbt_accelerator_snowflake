{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'VER_ID, VT_TYP_CD, COV_REF_IND', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.vt_fm_mtrx_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.vt_fm_mtrx",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.vt_fm_mtrx AS SELECT * FROM bronze_zone_dev.wescom_pl.vt_fm_mtrx_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.vt_fm_mtrx_transient', true, '(?i)TDB2WPLQ.VT_FM_MTRX/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.vt_fm_mtrx_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(VER_ID,' | ',VT_TYP_CD,' | ',COV_REF_IND)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.vt_fm_mtrx AS SELECT * FROM bronze_zone_dev.wescom_pl.vt_fm_mtrx_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH vt_fm_mtrx_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'VT_FM_MTRX_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.vt_fm_mtrx_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.VER_ID AS SOURCE_VER_ID, s.VT_TYP_CD AS SOURCE_VT_TYP_CD, s.COV_REF_IND AS SOURCE_COV_REF_IND,  -- Source IDs
        t.VER_ID AS TARGET_VER_ID, t.VT_TYP_CD AS TARGET_VT_TYP_CD, t.COV_REF_IND AS TARGET_COV_REF_IND,  -- Target IDs
        CASE 
            --WHEN t.VER_ID OR t.VT_TYP_CD OR t.COV_REF_IND IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.VER_ID) OR TO_CHAR(t.VT_TYP_CD) OR TO_CHAR(t.COV_REF_IND) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM vt_fm_mtrx_source_data s
    LEFT JOIN {{ this }} t
    ON s.VER_ID = t.VER_ID AND s.VT_TYP_CD = t.VT_TYP_CD AND s.COV_REF_IND = t.COV_REF_IND
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM vt_fm_mtrx_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.VER_ID = st.SOURCE_VER_ID AND s.VT_TYP_CD = st.SOURCE_VT_TYP_CD AND s.COV_REF_IND = st.SOURCE_COV_REF_IND
    LEFT JOIN {{ this }} t
        ON s.VER_ID = t.VER_ID AND s.VT_TYP_CD = t.VT_TYP_CD AND s.COV_REF_IND = t.COV_REF_IND
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'VT_FM_MTRX_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(VER_ID, VT_TYP_CD, COV_REF_IND,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
