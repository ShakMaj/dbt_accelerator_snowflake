{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'MDL_YR, VEH_MK_CD, MDL_NM, TRIM_LVL, RSTRC_STRT_DT', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.wfld_rstrc_veh_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.wfld_rstrc_veh",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.wfld_rstrc_veh AS SELECT * FROM bronze_zone_dev.wescom_pl.wfld_rstrc_veh_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.wfld_rstrc_veh_transient', true, '(?i)TDB2WPLQ.WFLD_RSTRC_VEH/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.wfld_rstrc_veh_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(MDL_YR,' | ',VEH_MK_CD,' | ',MDL_NM,' | ',TRIM_LVL,' | ',RSTRC_STRT_DT)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.wfld_rstrc_veh AS SELECT * FROM bronze_zone_dev.wescom_pl.wfld_rstrc_veh_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH wfld_rstrc_veh_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'WFLD_RSTRC_VEH_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.wfld_rstrc_veh_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.MDL_YR AS SOURCE_MDL_YR, s.VEH_MK_CD AS SOURCE_VEH_MK_CD, s.MDL_NM AS SOURCE_MDL_NM, s.TRIM_LVL AS SOURCE_TRIM_LVL, s.RSTRC_STRT_DT AS SOURCE_RSTRC_STRT_DT,  -- Source IDs
        t.MDL_YR AS TARGET_MDL_YR, t.VEH_MK_CD AS TARGET_VEH_MK_CD, t.MDL_NM AS TARGET_MDL_NM, t.TRIM_LVL AS TARGET_TRIM_LVL, t.RSTRC_STRT_DT AS TARGET_RSTRC_STRT_DT,  -- Target IDs
        CASE 
            --WHEN t.MDL_YR OR t.VEH_MK_CD OR t.MDL_NM OR t.TRIM_LVL OR t.RSTRC_STRT_DT IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.MDL_YR) OR TO_CHAR(t.VEH_MK_CD) OR TO_CHAR(t.MDL_NM) OR TO_CHAR(t.TRIM_LVL) OR TO_CHAR(t.RSTRC_STRT_DT) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM wfld_rstrc_veh_source_data s
    LEFT JOIN {{ this }} t
    ON s.MDL_YR = t.MDL_YR AND s.VEH_MK_CD = t.VEH_MK_CD AND s.MDL_NM = t.MDL_NM AND s.TRIM_LVL = t.TRIM_LVL AND s.RSTRC_STRT_DT = t.RSTRC_STRT_DT
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM wfld_rstrc_veh_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.MDL_YR = st.SOURCE_MDL_YR AND s.VEH_MK_CD = st.SOURCE_VEH_MK_CD AND s.MDL_NM = st.SOURCE_MDL_NM AND s.TRIM_LVL = st.SOURCE_TRIM_LVL AND s.RSTRC_STRT_DT = st.SOURCE_RSTRC_STRT_DT
    LEFT JOIN {{ this }} t
        ON s.MDL_YR = t.MDL_YR AND s.VEH_MK_CD = t.VEH_MK_CD AND s.MDL_NM = t.MDL_NM AND s.TRIM_LVL = t.TRIM_LVL AND s.RSTRC_STRT_DT = t.RSTRC_STRT_DT
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'WFLD_RSTRC_VEH_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(MDL_YR, VEH_MK_CD, MDL_NM, TRIM_LVL, RSTRC_STRT_DT,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
