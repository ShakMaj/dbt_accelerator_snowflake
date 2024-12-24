{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'WFLD_DERV_RCC_ID, SEQ_NR, BLD_ATR_NM', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.wfld_derv_rcc_atr_transient', false, '(?i)TDB2WPLQ.WFLD_DERV_RCC_ATR/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.wfld_derv_rcc_atr_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(WFLD_DERV_RCC_ID,' | ',SEQ_NR,' | ',BLD_ATR_NM)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH wfld_derv_rcc_atr_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'WFLD_DERV_RCC_ATR_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.wfld_derv_rcc_atr_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.WFLD_DERV_RCC_ID AS SOURCE_WFLD_DERV_RCC_ID, s.SEQ_NR AS SOURCE_SEQ_NR, s.BLD_ATR_NM AS SOURCE_BLD_ATR_NM,  -- Source IDs
        t.WFLD_DERV_RCC_ID AS TARGET_WFLD_DERV_RCC_ID, t.SEQ_NR AS TARGET_SEQ_NR, t.BLD_ATR_NM AS TARGET_BLD_ATR_NM,  -- Target IDs
        CASE 
            WHEN t.WFLD_DERV_RCC_ID OR t.SEQ_NR OR t.BLD_ATR_NM IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM wfld_derv_rcc_atr_source_data s
    LEFT JOIN {{ this }} t
    ON s.WFLD_DERV_RCC_ID = t.WFLD_DERV_RCC_ID AND s.SEQ_NR = t.SEQ_NR AND s.BLD_ATR_NM = t.BLD_ATR_NM
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM wfld_derv_rcc_atr_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.WFLD_DERV_RCC_ID = st.SOURCE_WFLD_DERV_RCC_ID AND s.SEQ_NR = st.SOURCE_SEQ_NR AND s.BLD_ATR_NM = st.SOURCE_BLD_ATR_NM
    LEFT JOIN {{ this }} t
        ON s.WFLD_DERV_RCC_ID = t.WFLD_DERV_RCC_ID AND s.SEQ_NR = t.SEQ_NR AND s.BLD_ATR_NM = t.BLD_ATR_NM
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'WFLD_DERV_RCC_ATR_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(WFLD_DERV_RCC_ID, SEQ_NR, BLD_ATR_NM), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
