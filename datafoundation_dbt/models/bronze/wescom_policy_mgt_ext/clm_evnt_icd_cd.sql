{{ config(
    schema = 'wescom_policy_mgt_ext',
    materialized = 'table',
    unique_key = 'CLM_EVNT_ICD_CD_ID', 
    tags=["bronze", 'wescom_policy_mgt_ext'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_policy_mgt_ext.wescom_policy_mgt_ext_stage', 'bronze_zone_dev.wescom_policy_mgt_ext.clm_evnt_icd_cd_transient', false, '(?i)TDB2PMED.CLM_EVNT_ICD_CD/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_policy_mgt_ext.clm_evnt_icd_cd_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(CLM_EVNT_ICD_CD_ID)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH clm_evnt_icd_cd_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_POLICY_MGT_EXT', 'CLM_EVNT_ICD_CD_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_policy_mgt_ext.clm_evnt_icd_cd_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.CLM_EVNT_ICD_CD_ID AS SOURCE_CLM_EVNT_ICD_CD_ID,  -- Source IDs
        t.CLM_EVNT_ICD_CD_ID AS TARGET_CLM_EVNT_ICD_CD_ID,  -- Target IDs
        CASE 
            WHEN t.CLM_EVNT_ICD_CD_ID IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM clm_evnt_icd_cd_source_data s
    LEFT JOIN {{ this }} t
    ON s.CLM_EVNT_ICD_CD_ID = t.CLM_EVNT_ICD_CD_ID
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM clm_evnt_icd_cd_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.CLM_EVNT_ICD_CD_ID = st.SOURCE_CLM_EVNT_ICD_CD_ID
    LEFT JOIN {{ this }} t
        ON s.CLM_EVNT_ICD_CD_ID = t.CLM_EVNT_ICD_CD_ID
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_POLICY_MGT_EXT', 'CLM_EVNT_ICD_CD_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(CLM_EVNT_ICD_CD_ID), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
