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
## Get user's Includes/data/sales file path
########################################
 
import os

# Get the current working directory
current_dir = os.getcwd()

# Build the data folder path
data_folder = os.path.join(current_dir, "Includes", "data","sales")

# Print the source file path
print(f"User's source file path: {data_folder}/")

# COMMAND ----------

import time



def dq_demo_setup(
    my_catalog: str, 
    schema: str,
    source_volumes: list,
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
    ## Creating volumes (list)
    ########################################

    ## Create staging volumes (ops for all 3 files, sales for prod) for each flow in the bronze schema
    for volume in source_volumes:
        create_volume = f'{my_catalog}.{bronze_schema}.{volume}'
        print(f'Creating volume {volume}...')
        spark.sql(f'CREATE VOLUME IF NOT EXISTS {create_volume}')


    # ## Create python varibles to volume paths
    # print('Creating Python and SQL variables to volume paths')

    ## Path to the bronze schema
    vol_path = f'/Volumes/{my_catalog}/{bronze_schema}'
    ops_path = f'{vol_path}/ops'

    ## Path to the sales  and opts source volumes
    sales_path = f'{vol_path}/sales'


    ########################################
    ## Delete all files in the labuser's volume to reset the class if necessary. Otherwise does nothing.
    ########################################
    if reset_volume == True:
        print('Reset volumes by deleting files')
        delete_source_files(sales_path + '/')
        time.sleep(5)


    ########################################
    ## Move Workspaces data files to staging volume
    ########################################

    # Copy content data files to staging volume
    raw_file_names = ['sales_1.csv','sales_2.csv']

    for file in raw_file_names:
        copy_file_to_volume(
            src_workspace_path=f"{data_folder}/{file}",
            target_volume_path=f"{ops_path}/{file}",
            overwrite=False
        )
    ## Wait for all files to be copied. Running into issues here occasionally
    time.sleep(5)


    ########################################
    ## Copy 1 file into the user's volume
    ########################################
    ## Copies one file from the staging ops volume to the prod volume
    copy_files(
        copy_from = ops_path, 
        copy_to = sales_path, 
        n = 1
    )


    ## Setup complete displayed
    setup_complete_msg()

    display_config_values([
        ('Your Catalog', my_catalog),
        ('Your Schemas', f"{bronze_schema}, {silver_schema}, {gold_schema}"),
        ('Your Data Source Volume Path', sales_path )
    ])

    ## Check user's compute
    compute_validation(recommend_dbr_classic_version=None, recommended_serverless_version=4)

    ## Return this to path to to use in the notebook
    return vol_path

# COMMAND ----------

my_vol_path = dq_demo_setup(
    my_catalog = my_catalog, 
    schema = 'dq',
    source_volumes = ['sales','ops'],
    reset_volume = True  ## <-- Set to True to delete all files in your volumes to start fresh if you've already completed the demo
)

## Create a SQL variable to avoid using spark.sql in queries
spark.sql(f"DECLARE OR REPLACE VARIABLE my_vol_path STRING DEFAULT '{my_vol_path}'")