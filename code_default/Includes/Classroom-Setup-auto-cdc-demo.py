# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

## Set user catalog
my_catalog = build_user_catalog(catalog_forced = None) ## <-- Forces the usage of a catalog if you can't create one. Catalog is assumed to exist. Reference as 'string'.

# COMMAND ----------

########################################
# DO NOT MODIFY BELOW
########################################


########################################
## Get user's Includes/data/customers file path
########################################
 
import os

# Get the current working directory
current_dir = os.getcwd()

# Build the data folder path
data_folder = os.path.join(current_dir, "Includes", "data","customers")

# Print the source file path
print(f"User's source file path: {data_folder}/")

# COMMAND ----------

def auto_cdc_demo_setup(
    my_catalog: str, 
    data_folder: str, 
    schema: str,
    source_volume: str,
    reset_volume = False
):

    ########################################
    ## Set default catalog
    ########################################
    r = spark.sql(f"USE CATALOG {my_catalog}")


    ########################################
    ## Create the user schemas and volume. Bronze, silver, gold
    ########################################
    bronze_schema = f'{schema}_1_bronze'
    print(f'Creating schema {my_catalog}.{bronze_schema}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{bronze_schema}')

    silver_schema = f'{schema}_2_silver'
    print(f'Creating schema {my_catalog}.{silver_schema}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{silver_schema}')

    gold_schema = f'{schema}_3_gold'
    print(f'Creating schema {my_catalog}.{gold_schema}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{gold_schema}')


    ########################################
    ## Move Workspaces data files to staging volume
    ########################################
    # Making staging volume
    spark.sql(f'CREATE VOLUME IF NOT EXISTS {my_catalog}.{bronze_schema}.staging');

    staging_vol_path = f'/Volumes/{my_catalog}/{bronze_schema}/staging/customers'
    os.makedirs(staging_vol_path, exist_ok=True)

    # Copy content data files to staging volume
    raw_file_names = ['00.json','01.json','02.json','03.json']

    for file in raw_file_names:
        copy_file_to_volume(
            src_workspace_path=f"{data_folder}/{file}",
            target_volume_path=f"{staging_vol_path}/{file}",
            overwrite=False
        )

    ########################################
    ## Create source SDP pipeline volume in the bronze schema by copying from staging
    ########################################
    create_volume = f'{my_catalog}.{bronze_schema}.{source_volume}'
    print(f'Creating volume {create_volume}...')
    spark.sql(f'CREATE VOLUME IF NOT EXISTS {create_volume}')

    ## Create and store the path to their volume and return
    my_vol_path = f'/Volumes/{my_catalog}/{bronze_schema}/{source_volume}'

    ## Create a SQL variable to avoid using spark.sql in queries
    spark.sql(f"DECLARE OR REPLACE VARIABLE my_vol_path STRING DEFAULT '{my_vol_path}'")

    print('\n---------- Schema and volume setup complete ----------\n')


    ########################################
    ## Delete all files in the labuser's volume to reset the class if necessary. Otherwise does nothing.
    ########################################
    if reset_volume == True:
        print('Reset volume by deleting files')

        delete_source_files(f'{my_vol_path}/')


    ########################################
    ## Copy 1 file into the user's volume
    ########################################
    copy_files(
        copy_from = f'/Volumes/{my_catalog}/{bronze_schema}/staging/customers', 
        copy_to = f'{my_vol_path}', 
        n = 1
    )

    compute_validation(recommend_dbr_classic_version=None, recommended_serverless_version=4)

    ## Setup complete displayed
    setup_complete_msg()

    display_config_values([
        ('Your Catalog', my_catalog),
        ('Your Schemas', f"{bronze_schema}, {silver_schema}, {gold_schema}"),
        ('Your Source Volume Path', my_vol_path)
    ])

    ## Return this to path to to use in the notebook
    return my_vol_path

# COMMAND ----------

## Setup your notebook environment
my_vol_path = auto_cdc_demo_setup(
    my_catalog = my_catalog, 
    data_folder = data_folder, 
    schema = 'sdp_cdc',
    source_volume = 'customer_source_files',
    reset_volume = True ## <-- Set to True to delete all files in your volumes to start fresh if you've already complete the demo
)

## Set default catalog
_ = spark.sql(f'USE CATALOG {my_catalog}')