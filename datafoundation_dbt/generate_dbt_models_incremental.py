import csv
import os

# Path to your DBT models directory (adjust to your project structure)
models_dir = os.getcwd()+'/models/bronze/wescom_pl/'

# Path to your CSV file with table details
csv_file = os.getcwd()+'/seeds/table_list.csv'

# Read the CSV and generate models dynamically
with open(csv_file, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        snapshot_table_name = row['snapshot_table_name']
        file_location = row['file_location']
        key_columns = row['key_column'].split(',')  # This assumes the key columns are in a comma-separated string
        transient_table_name = row['transient_table_name'] 
        u_transient_table_name = transient_table_name.upper()

        # Join the key columns to create the unique key and for join conditions
        unique_key = ', '.join(key_columns)
        concatenated_keys = ' || '.join([f"s.{key.strip()}" for key in key_columns])  # Concatenate key columns for hash

        # Define the model file path (dynamic file name based on table_name)
        model_file_path = os.path.join(models_dir, f"{snapshot_table_name}.sql")

        # Define the model file path (dynamic file name based on table_name)
        

        # Create the model content (using your provided SQL template with dynamic values)
        model_content = f"""{{{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = '{unique_key}', 
    incremental_strategy = 'merge',
    pre_hook= [
        "{{{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.{transient_table_name}', true, '{file_location}') }}}}",
        "UPDATE bronze_zone_dev.wescom_pl.{transient_table_name} SET CREATED_DATETIME = CURRENT_TIMESTAMP(), CREATED_BY = CURRENT_USER() WHERE ingestdate > (SELECT COALESCE(MAX(CREATED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM bronze_zone_dev.wescom_pl.{transient_table_name})"
    ]
) }}}}

-- Step 1: Source Data with FLAG Calculation
WITH {snapshot_table_name}_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{{{ get_dynamic_columns('WESCOM_PL', '{u_transient_table_name}') }}}},
    FROM 
        bronze_zone_dev.wescom_pl.{transient_table_name} s

        {{% if is_incremental() %}}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{{{ this }}}})  -- Filter only modified records
        {{% endif %}}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        {', '.join([f's.{key.strip()} AS SOURCE_{key.strip()}' for key in key_columns])},  -- Source IDs
        {', '.join([f't.{key.strip()} AS TARGET_{key.strip()}' for key in key_columns])},  -- Target IDs
        CASE 
            WHEN {' OR '.join([f't.{key.strip()}' for key in key_columns])} IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM {snapshot_table_name}_source_data s
    LEFT JOIN {{{{ this }}}} t
    ON {' AND '.join([f's.{key.strip()} = t.{key.strip()}' for key in key_columns])}
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.CREATED_BY AS CREATED_BY,
        t.CREATED_DATETIME AS CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM {snapshot_table_name}_source_data s
    LEFT JOIN source_and_target_ids st
        ON {' AND '.join([f's.{key.strip()} = st.SOURCE_{key.strip()}' for key in key_columns])}
    LEFT JOIN {{{{ this }}}} t
        ON {' AND '.join([f's.{key.strip()} = t.{key.strip()}' for key in key_columns])}
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{{{ get_dynamic_columns('WESCOM_PL', '{u_transient_table_name}') }}}}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT({unique_key}), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

    -- Set CREATED_DATETIME only on inserts
    , CASE 
        WHEN FLAG = 'I' THEN CURRENT_TIMESTAMP() 
        ELSE CREATED_DATETIME  -- Keep the same value for updates
    END AS CREATED_DATETIME
    
    -- Set CREATED_BY only on inserts
    , CASE 
        WHEN FLAG = 'I' THEN CURRENT_USER()  
        ELSE CREATED_BY  -- Keep the same value for updates
    END AS CREATED_BY                          

    , CURRENT_TIMESTAMP() as MODIFIED_DATETIME
    , CURRENT_USER() as MODIFIED_BY

FROM joined_data
"""

        # Write the model content to a .sql file
        with open(model_file_path, 'w') as model_file:
            model_file.write(model_content)
            print(f"Generated model for table: {snapshot_table_name}")
