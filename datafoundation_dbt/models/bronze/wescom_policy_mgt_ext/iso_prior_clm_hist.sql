{{ config(
    schema = 'wescom_policy_mgt_ext',
    materialized = 'table',
    unique_key = 'ISO_PRR_CLM_HST_ID', 
    tags=["bronze", 'wescom_policy_mgt_ext'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist AS SELECT * FROM bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient WHERE 1 = 0",
        "ALTER TABLE bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist ADD COLUMN HASH_KEY STRING",
        "{{ copy_into_macro('bronze_zone_dev.wescom_policy_mgt_ext.wescom_policy_mgt_ext_stage', 'bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient', true, '.*WescomParquet/TDB2PMED.ISO_PRIOR_CLM_HIST/.*.parquet') }}",
        "UPDATE bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER() WHERE AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_CREATED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient)"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_policy_mgt_ext.iso_prior_clm_hist AS SELECT * FROM bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH iso_prior_clm_hist_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_POLICY_MGT_EXT', 'ISO_PRIOR_CLM_HIST_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_policy_mgt_ext.iso_prior_clm_hist_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.ISO_PRR_CLM_HST_ID AS SOURCE_ISO_PRR_CLM_HST_ID,  -- Source IDs
        t.ISO_PRR_CLM_HST_ID AS TARGET_ISO_PRR_CLM_HST_ID,  -- Target IDs
        CASE 
            --WHEN t.ISO_PRR_CLM_HST_ID IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.ISO_PRR_CLM_HST_ID) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM iso_prior_clm_hist_source_data s
    LEFT JOIN {{ this }} t
    ON s.ISO_PRR_CLM_HST_ID = t.ISO_PRR_CLM_HST_ID
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM iso_prior_clm_hist_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.ISO_PRR_CLM_HST_ID = st.SOURCE_ISO_PRR_CLM_HST_ID
    LEFT JOIN {{ this }} t
        ON s.ISO_PRR_CLM_HST_ID = t.ISO_PRR_CLM_HST_ID
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_POLICY_MGT_EXT', 'ISO_PRIOR_CLM_HIST_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(ISO_PRR_CLM_HST_ID,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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