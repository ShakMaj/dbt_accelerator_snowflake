{{ config(
    schema = 'wescom_policy_mgt_ext',
    materialized = 'table',
    unique_key = 'PST_CD, VAR_NM', 
    tags=["bronze", 'wescom_policy_mgt_ext'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg AS SELECT * FROM bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient WHERE 1 = 0",
        "ALTER TABLE bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg ADD COLUMN HASH_KEY STRING",
        "{{ copy_into_macro('bronze_zone_dev.wescom_policy_mgt_ext.wescom_policy_mgt_ext_stage', 'bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient', true, '.*WescomParquet/TDB2PMED.PST_CD_DEMG/.*.parquet') }}",
        "UPDATE bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER() WHERE AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_CREATED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient)"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_policy_mgt_ext.pst_cd_demg AS SELECT * FROM bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH pst_cd_demg_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_POLICY_MGT_EXT', 'PST_CD_DEMG_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_policy_mgt_ext.pst_cd_demg_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.PST_CD AS SOURCE_PST_CD, s.VAR_NM AS SOURCE_VAR_NM,  -- Source IDs
        t.PST_CD AS TARGET_PST_CD, t.VAR_NM AS TARGET_VAR_NM,  -- Target IDs
        CASE 
            --WHEN t.PST_CD OR t.VAR_NM IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.PST_CD) OR TO_CHAR(t.VAR_NM) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM pst_cd_demg_source_data s
    LEFT JOIN {{ this }} t
    ON s.PST_CD = t.PST_CD AND s.VAR_NM = t.VAR_NM
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM pst_cd_demg_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.PST_CD = st.SOURCE_PST_CD AND s.VAR_NM = st.SOURCE_VAR_NM
    LEFT JOIN {{ this }} t
        ON s.PST_CD = t.PST_CD AND s.VAR_NM = t.VAR_NM
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_POLICY_MGT_EXT', 'PST_CD_DEMG_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(PST_CD, VAR_NM,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
