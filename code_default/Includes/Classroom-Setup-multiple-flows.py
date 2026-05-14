# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

## Set user catalog
my_catalog = build_user_catalog(catalog_forced = None) ## <-- Forces the usage of a catalog if you can't create one. Catalog is assumed to exist. Reference as 'string'.

# COMMAND ----------

# MAGIC %skip
# MAGIC check_required_vars("your_marketplace_share_catalog_name")

# COMMAND ----------

########################################
# DO NOT MODIFY BELOW
########################################

def multi_flow_demo_setup(
    my_catalog: str, 
    marketplace_catalog: str, 
    schema: str,
    source_volumes: list,
    reset_volume = False
):

    ## Set the default catalog
    r = spark.sql(f"USE CATALOG {my_catalog}")

    ## Bronze, silver, gold schemas
    bronze_schema = f'{schema}_1_bronze'
    print(f'Creating schema {my_catalog}.{bronze_schema}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{bronze_schema}')

    silver_schema = f'{schema}_2_silver'
    print(f'Creating schema {my_catalog}.{silver_schema}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{silver_schema}')

    gold_schema = f'{schema}_3_gold'
    print(f'Creating schema {my_catalog}.{gold_schema}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{gold_schema}')

    ## Create staging volumes for each flow in the bronze schema
    for volume in source_volumes:
        create_volume = f'{my_catalog}.{bronze_schema}.{volume}'
        print(f'Creating volume {volume}...')
        spark.sql(f'CREATE VOLUME IF NOT EXISTS {create_volume}')

    ## Create python varibles to volume paths
    print('Creating Python and SQL variables to volume paths: bright_home_orders_path, lumina_sports_orders_path, northstar_outfitters_orders_path')

    vol_path = f'/Volumes/{my_catalog}/{bronze_schema}'

    bright_home_orders_path = f'/Volumes/{my_catalog}/{bronze_schema}/bright_home_orders'
    lumina_sports_orders_path = f'/Volumes/{my_catalog}/{bronze_schema}/lumina_sports_orders'
    northstar_outfitters_orders_path = f'/Volumes/{my_catalog}/{bronze_schema}/northstar_outfitters_orders'

    ## Create a SQL variable to avoid using spark.sql in queries
    spark.sql(f"DECLARE OR REPLACE VARIABLE my_vol_path STRING DEFAULT '{vol_path}'")

    ## Delete all files in the labuser's volume to reset the class if necessary. Otherwise does nothing.
    if reset_volume == True:
        print('Reset volumes by deleting files')
        delete_source_files(bright_home_orders_path + '/')
        delete_source_files(lumina_sports_orders_path + '/')
        delete_source_files(northstar_outfitters_orders_path + '/')


    # Copy 1 file into each user's volume from marketplace
    marketplace_catalog_schema = f'/Volumes/{marketplace_catalog}/v02/subsidiary_daily_orders'
    copy_files(copy_from = f'{marketplace_catalog_schema}/bright_home_orders', copy_to = bright_home_orders_path, n = 1)
    copy_files(copy_from = f'{marketplace_catalog_schema}/lumina_sports_orders', copy_to = lumina_sports_orders_path, n = 1)
    copy_files(copy_from = f'{marketplace_catalog_schema}/northstar_outfitters_orders', copy_to = northstar_outfitters_orders_path, n = 1)

    ## Check user's compute
    compute_validation(recommend_dbr_classic_version=None, recommended_serverless_version=4)

    ## Setup complete displayed
    setup_complete_msg()

    display_config_values([
        ('Your Marketplace Share Catalog', marketplace_catalog),
        ('Your Catalog', my_catalog),
        ('Your Schemas', f"{bronze_schema}, {silver_schema}, {gold_schema}"),
        ('Your Data Source Volume Path', bright_home_orders_path),
        ('Your Data Source Volume Path', lumina_sports_orders_path),
        ('Your Data Source Volume Path', northstar_outfitters_orders_path)
    ])

    ## Return this to path to to use in the notebook
    return vol_path

# COMMAND ----------

my_vol_path = multi_flow_demo_setup(
    my_catalog = my_catalog, 
    marketplace_catalog = 'dbacademy_retail_customer_wesllan', 
    schema = 'multi_flow',
    source_volumes = ['bright_home_orders','lumina_sports_orders','northstar_outfitters_orders'],
    reset_volume = True  ## <-- Set to True to delete all files in your volumes to start fresh if you've already complete the demo
)
