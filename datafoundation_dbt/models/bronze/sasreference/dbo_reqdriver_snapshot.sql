{{ config( 
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
) }}

-- Step 1: Source Data with FLAG Calculation
WITH dbo_reqdriver_transient_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'DBO_REQDRIVER_TRANSIENT') }},

    FROM 
        {{ ref('dbo_reqdriver_transient') }} s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.id AS SOURCE_ID,  -- Source ID
        t.id AS TARGET_ID,  -- Target ID (to check if it exists in the target table)
        CASE 
            WHEN t.id IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'                   -- Update if target ID exists
        END AS FLAG
    FROM dbo_reqdriver_transient_source_data s
    LEFT JOIN {{ this }} t
    ON s.id = t.id
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.CREATED_BY AS CREATED_BY,
        t.CREATED_DATETIME AS CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM dbo_reqdriver_transient_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.id = st.SOURCE_ID
    LEFT JOIN {{ this }} t
        ON s.id = t.id
)

-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'DBO_REQDRIVER_TRANSIENT') }},
    
    -- Include the FLAG and calculate the HASH_KEY
    FLAG,
    SHA2(id) AS HASH_KEY,

    -- Set CREATED_DATETIME only on inserts
    CASE 
        WHEN FLAG = 'I' THEN CURRENT_TIMESTAMP() 
        ELSE CREATED_DATETIME  -- Keep the same value for updates
    END AS CREATED_DATETIME,
    
    -- Set CREATED_BY only on inserts
    CASE 
        WHEN FLAG = 'I' THEN CURRENT_USER()  
        ELSE CREATED_BY  -- Keep the same value for updates
    END AS CREATED_BY,                          

    -- Set MODIFIED_DATETIME only on updates
    /*CASE 
        WHEN FLAG = 'U' THEN CURRENT_TIMESTAMP() 
        ELSE NULL
    END AS MODIFIED_DATETIME,
    
    -- Set MODIFIED_BY only on updates
    CASE 
        WHEN FLAG = 'U' THEN CURRENT_USER() 
        ELSE NULL
    END AS MODIFIED_BY*/
    CURRENT_TIMESTAMP() as MODIFIED_DATETIME,
    CURRENT_USER() as MODIFIED_BY


FROM joined_data
