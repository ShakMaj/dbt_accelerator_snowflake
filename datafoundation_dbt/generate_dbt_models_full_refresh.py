#20122024: Removing audit columns from update statement of transient table

import csv
import os

# Path to your DBT models directory (adjust to your project structure)
schema = 'wescom_pl'
u_schema = schema.upper()
models_dir = os.getcwd()+'/models/bronze/'+schema+'/'

# Path to your CSV file with table details
csv_file = os.getcwd()+'/seeds/table_list_wescom_pl.csv'

# Read the CSV and generate models dynamically
with open(csv_file, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        snapshot_table_name = row['SNAPSHOT_TABLE_NAME']
        file_location = row['FILE_LOCATION']
        key_columns = row['KEY_COLUMN'].upper().split(',') # This assumes the key columns are in a comma-separated string
        all_columns = row['ALL_COLUMN'].upper().split(',') 

        
        
        transient_table_name = row['TRANSIENT_TABLE_NAME'] 
        u_transient_table_name = transient_table_name.upper()

        # Join the key columns to create the unique key and for join conditions
        unique_key = ', '.join(key_columns)
       
     

        concatenated_keys = ' | '.join([f'"{key.strip()}"' for key in key_columns])  # Ensure double quotes for Snowflake
        hash_all_columns = ",' | ',".join([f'CAST("{col.strip()}" AS STRING)' for col in all_columns])
        
        # Prepare the hash for all keys with proper string casting for Snowflake
        #hash_all_keys = ",' | ',".join([f'CAST("{key.strip()}" AS STRING)' for key in key_columns])
        hash_all_keys = ",' | ',".join([f'{key.strip()}' for key in key_columns])
        # Define the model file path (dynamic file name based on table_name)
        model_file_path = os.path.join(models_dir, f"{snapshot_table_name}.sql")

        # Define the model file path (dynamic file name based on table_name)
        

        # Create the model content (using your provided SQL template with dynamic values)
        model_content = f"""{{{{ config(
    schema = '{schema}',
    materialized = 'table',
    transient = false,
    unique_key = '{unique_key}', 
    tags=["bronze", '{schema}'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.{schema}.{transient_table_name}",
        "DROP TABLE IF EXISTS bronze_zone_dev.{schema}.{snapshot_table_name}",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.{schema}.{snapshot_table_name} AS SELECT * FROM bronze_zone_dev.{schema}.{transient_table_name} WHERE 1 = 0",
        "{{{{ copy_into_macro('bronze_zone_dev.{schema}.{schema}_stage', 'bronze_zone_dev.{schema}.{transient_table_name}', true, '{file_location}') }}}}",
         "UPDATE bronze_zone_dev.{schema}.{transient_table_name} SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT({hash_all_keys})) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_{schema}.{snapshot_table_name} AS SELECT * FROM bronze_zone_dev.{schema}.{transient_table_name}"
    ]
) }}}}

-- Step 1: Source Data with FLAG Calculation
WITH {snapshot_table_name}_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{{{ get_dynamic_columns('{u_schema}', '{u_transient_table_name}') }}}},
    FROM 
        bronze_zone_dev.{schema}.{transient_table_name} s

        {{% if is_incremental() %}}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{{{ this }}}})  -- Filter only modified records
        {{% endif %}}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        {', '.join([f's.{key.strip()} AS SOURCE_{key.strip()}' for key in key_columns])},  -- Source IDs
        {', '.join([f't.{key.strip()} AS TARGET_{key.strip()}' for key in key_columns])},  -- Target IDs
        CASE 
            --WHEN {' OR '.join([f't.{key.strip()}' for key in key_columns])} IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN {' OR '.join([f'TO_CHAR(t.{key.strip()})' for key in key_columns])} IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM {snapshot_table_name}_source_data s
    LEFT JOIN {{{{ this }}}} t
    ON {' AND '.join([f's.{key.strip()} = t.{key.strip()}' for key in key_columns])}
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM {snapshot_table_name}_source_data s
    LEFT JOIN source_and_target_ids st
        ON {' AND '.join([f's.{key.strip()} = st.SOURCE_{key.strip()}' for key in key_columns])}
    LEFT JOIN {{{{ this }}}} t
        ON {' AND '.join([f's.{key.strip()} = t.{key.strip()}' for key in key_columns])}
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{{{ get_dynamic_columns('{u_schema}', '{u_transient_table_name}') }}}}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT({unique_key},'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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
"""

        # Write the model content to a .sql file
        with open(model_file_path, 'w') as model_file:
            model_file.write(model_content)
            print(f"Generated model for table: {snapshot_table_name}")
