# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This demonstration showcases how to build a multiplex data pipeline using Spark Declarative Pipelines (Lakeflow). You will learn how to ingest multiplexed data from a single source into a bronze streaming table, then demultiplex that data into multiple silver streaming tables based on event types, and finally create aggregated gold-layer views for analytics.
# MAGIC
# MAGIC The demo simulates a real-world scenario where multiple business domains (store operations, marketing, and logistics) generate events that are ingested into a single data stream. Using the medallion architecture pattern, you will process this multiplexed data through bronze, silver, and gold layers while enabling Iceberg compatibility for cross-platform analytics.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC - **Build multiplex data pipelines** using Spark Declarative Pipelines to handle multiple event types from a single source
# MAGIC - **Demultiplex streaming data** by filtering and transforming events into separate domain-specific tables
# MAGIC - **Build Delta sinks** using the Python API to create analytics-ready tables from streaming sources
# MAGIC - **Enable Iceberg compatibility** on Delta tables to support cross-platform data access and analytics

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Before starting this notebook, select the required compute environment listed below.
# MAGIC
# MAGIC - **Serverless Compute, Version 4**  
# MAGIC   - [How to select an environment version](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version)
# MAGIC
# MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V4**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiplex Pipeline Demonstration Overview
# MAGIC In this demonstration, you'll build a Lakeflow Spark Declarative Pipeline that implements the full medallion architecture, from raw data ingestion to curated analytics.
# MAGIC
# MAGIC 1. **Ingest multiplexed files** from cloud storage and write them into a single bronze table.  
# MAGIC     - These files contain multiplexed data, meaning they include events for **marketing**, **logistics**, and **store operations**, all dumped into a single cloud location.
# MAGIC 2. **Demultiplex the bronze table** into intermediate tables based on event group. Three intermediate tables will be created in the bronze layer.
# MAGIC 3. **Transform intermediate tables** to create silver tables for each event group, adding new columns as required by business logic and casting columns to the appropriate data types.
# MAGIC 4. **Create gold materialized views** from the marketing data that automatically refresh and provide analytics-ready results.
# MAGIC 5. **Write the logistics table to a Delta sink** to enable external access via Iceberg reads.
# MAGIC
# MAGIC ![Multi Flow Pipeline Overview](./Includes/images/multiplex/multiplex_demo_pipeline_overview.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size:1.1em;">
# MAGIC     Option 1 - Databricks Academy Provided Workspace (Vocareum Workspace)
# MAGIC   </strong>
# MAGIC   <details>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you are running this notebook in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC Your catalog name matches your Vocareum username and looks like: <strong>labuser12345</strong> (series of unique numbers)
# MAGIC   </div>
# MAGIC   </details>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size:1.1em;">
# MAGIC     Option 2 - Other Workspaces or Databricks Free Edition
# MAGIC   </strong>
# MAGIC   <details>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you are running this notebook in your own Databricks workspace or Databricks Free Edition, the setup will
# MAGIC <strong>create a Unity Catalog catalog and schema for you</strong>. **Create catalog permission is required.**
# MAGIC
# MAGIC The catalog name is derived from your Databricks username and follows this pattern: <strong>labuser_username</strong>
# MAGIC   </div>
# MAGIC   </details>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Do Not Run in Production Environments</strong>
# MAGIC   <div style="color:#333;">
# MAGIC   <ul>
# MAGIC       <li>Only run this notebook in <strong>development or sandbox workspaces</strong>.</li>
# MAGIC       <li>Do not run this in production environments. The setup script creates a catalog and schemas in your workspace.</li>
# MAGIC   </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Configure Your Catalog and Schema
# MAGIC
# MAGIC 1. Run the cell below to initialize your environment. This setup step does the following:
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks provided Vocareum workspace
# MAGIC     - Create three schemas in your specified catalog:
# MAGIC         - **multiplex_1_bronze**
# MAGIC         - **multiplex_2_silver**
# MAGIC         - **multiplex_3_gold**
# MAGIC     - Create `business_events` volume in your **YOUR_LABUSER_CATALOG.multiplex_1_bronze** schema, and adds a single file to your volume.
# MAGIC     - Verifies your selected compute environment
# MAGIC
# MAGIC     This ensures that all schemas, tables and objects are created in your catalog.
# MAGIC
# MAGIC > **Important:** You must have permission to create catalogs in your own non Vocareum workspace. If you do not have the required permissions, this step will fail. Review the note below before continuing.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Troubleshooting Setup - Missing Create Catalog Permissions
# MAGIC   </strong>
# MAGIC <details>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you do not have permission to create a new catalog but already have one available, you can explicitly specify an existing catalog by using the `catalog_forced` argument in the `build_user_catalog_name` function.
# MAGIC
# MAGIC This function is defined in the notebook: `./Includes/Classroom-Setup-multiplex`
# MAGIC
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-multiplex

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the value of the `my_vol_path` variable.
# MAGIC
# MAGIC     Confirm that the value references your **your-catalog.multiplex_1_bronze** path. This will be used to dynamically reference your source volumes throughout this demonstration.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Raw Source Data for the Multiplex Data Ingestion
# MAGIC
# MAGIC Before building the multiplex pipeline, start by exploring the raw source data stored in the volume.
# MAGIC
# MAGIC The source volume receives multiplexed data from a Kafka stream, containing three distinct event groups:
# MAGIC - **store_ops**
# MAGIC - **marketing**
# MAGIC - **logistics**
# MAGIC
# MAGIC All event types are ingested into a single file within a volume, simulating a real-world scenario where multiple business domains generate events concurrently.
# MAGIC
# MAGIC For this demo, we process each file individually from the source location to demonstrate streaming ingestion and multiplexing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore Business Events Daily Volume

# COMMAND ----------

# MAGIC %md
# MAGIC 1. View the files in the **multiplex_1_bronze.business_events** volume. 
# MAGIC   
# MAGIC     You will see one parquet file exists in this volume which contains event data.

# COMMAND ----------

spark.sql(f"LIST '{my_vol_path}/business_events' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Query the raw Parquet in volume to view the raw data and note the following:
# MAGIC     - The data is packed in **BINARY** format
# MAGIC       - The **key** columns holds a unique identifier for the data 
# MAGIC       - The **volume** column holds the actual data.
# MAGIC       - The **topic** column indicates the different event types present in this source location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events'
# MAGIC )
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View the data by **casting `BINARY` columns to `STRING`** to view the values.
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - The **value_str** column contains a JSON string of key-value pairs, which vary depending on the business event group
# MAGIC     - The **topic** column indicates the different event types present in this source location
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CAST(key AS STRING) AS key,
# MAGIC   CAST(value AS STRING) AS value_str,
# MAGIC   topic,
# MAGIC   partition,
# MAGIC   offset
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events'
# MAGIC )
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Introduction to the Variant Data Type
# MAGIC
# MAGIC 1. The incoming event data is stored as JSON strings. Because each business event has a different structure, we use the **VARIANT** data type to handle this flexibility.
# MAGIC
# MAGIC ##### Why VARIANT:
# MAGIC
# MAGIC - Supports semi structured JSON data
# MAGIC - Handles multiple event shapes in a single column
# MAGIC - Allows fields to be extracted when needed
# MAGIC - Works well for multiplex streaming data
# MAGIC
# MAGIC ##### What This Query Is Doing
# MAGIC
# MAGIC This query converts raw JSON into a usable format.
# MAGIC
# MAGIC - Reads raw event files with `read_files`
# MAGIC - Casts the raw value to a string
# MAGIC - Parses the JSON string into a VARIANT column
# MAGIC - Extracts `event_id` as a string
# MAGIC - Extracts and casts `timestamp` to a TIMESTAMP
# MAGIC - Keeps both raw and extracted fields for downstream use
# MAGIC
# MAGIC VARIANT lets us ingest once, then shape the data later as part of the pipeline.
# MAGIC
# MAGIC **NOTE:** This is a **quick introduction** to the VARIANT data type used in this notebook. We will use the `VARIANT` data type in your Spark Declarative Pipeline (SDP).
# MAGIC
# MAGIC - [VARIANT type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type)
# MAGIC - [Variant Data Type - Making Semi-Structured Data Fast and Simple - Deep Dive](https://www.youtube.com/watch?v=jtjOfggD4YY)
# MAGIC - [Introducing the Open Variant Data Type in Delta Lake and Apache Spark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   -- Raw JSON payload read from the file and cast to a string
# MAGIC   CAST(value AS STRING) AS value_str,
# MAGIC
# MAGIC   -- Parse the JSON string into a VARIANT column for flexible field access
# MAGIC   parse_json(value_str) AS event_data_variant,
# MAGIC
# MAGIC   -- Extract the event_id field from the VARIANT and cast it to STRING
# MAGIC   CAST(event_data_variant:event_id AS STRING) AS extracted_event_id,
# MAGIC
# MAGIC   -- Extract the timestamp field from the VARIANT and cast it to TIMESTAMP
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS extracted_timestamp
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events'
# MAGIC )
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Analyze Raw Data Statistics
# MAGIC
# MAGIC 1. Explore the raw data for **business_events**. The cell below performs the following:
# MAGIC     - Counts the number of records in the file within the volume  
# MAGIC     - Counts the number of records in the file for each event group in our volume
# MAGIC
# MAGIC     In the output, notice the following:
# MAGIC     - **372** rows are present in this file  
# MAGIC     - You will see counts for each of the three different sources

# COMMAND ----------

# a. Total row count
df_count = spark.sql(f"""
    SELECT COUNT(*) AS total_rows
    FROM  read_files('{my_vol_path}/business_events')
""")
display(df_count)

# b. Data source count by topic
df_data_source_count = spark.sql(f"""
    SELECT 
        topic,
        COUNT(*) AS total_rows
    FROM read_files('{my_vol_path}/business_events')
    GROUP BY topic
    ORDER BY topic
""")
display(df_data_source_count) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create the Spark Declarative Pipeline
# MAGIC
# MAGIC Now that we have explored the data and reviewed casting from `BINARY` to `STRING` along with the `VARIANT` data type, we are ready to create the Spark Declarative Pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Enable the Lakeflow Pipelines Editor
# MAGIC
# MAGIC Complete the following steps to confirm or enable the **Lakeflow Pipelines Editor**:
# MAGIC
# MAGIC 1. In the top-right corner of the workspace, select your **account icon** ![Account Icon](./Includes/images/account_icon.png) (*Your icon letter will differ*).  
# MAGIC
# MAGIC 2. Right-click **Settings** and choose **Open link in new tab**.  
# MAGIC
# MAGIC 3. In the left sidebar, select **Developer** under **User**.  
# MAGIC
# MAGIC 4. In the **Experimental features** section, locate **Lakeflow Pipelines Editor** and toggle it **on**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Create a Lakeflow Spark Declarative Pipeline Using the Lakeflow Pipelines Editor
# MAGIC
# MAGIC Complete the following steps to create your Spark Declarative Pipeline:
# MAGIC
# MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open link in New Tab**.  
# MAGIC
# MAGIC 2. In the new tab, select **Create → ETL Pipeline**.  
# MAGIC
# MAGIC    **NOTE:** If prompted to **Try the new Lakeflow Pipelines Editor**, choose **Enable Lakeflow Pipelines Editor**. This appears only if you did not complete the previous step.  
# MAGIC
# MAGIC 3. At the top, complete the following:
# MAGIC    - Name your pipeline `demo_multiplex_yourname`
# MAGIC    - Select your default **catalog** and **schema**:  
# MAGIC         - **Catalog:** `YOUR_LABUSER_CATALOG`
# MAGIC         - **Schema:** **multiplex_1_bronze**  
# MAGIC       **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. Rename the **transformations** folder to `multiplex_pipeline`.
# MAGIC
# MAGIC 5. Rename the **my_transformations.sql** file to `ingestion.sql`.
# MAGIC
# MAGIC 6. Leave the **Lakeflow Pipelines Editor** page open.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create the First Bronze Table for All Business Events Data
# MAGIC
# MAGIC Start by creating the **bronze streaming table** to ingest data from the source.
# MAGIC - This table will capture raw business event data streaming in from multiple sources.
# MAGIC - We will use the `VARIANT` data type to parse JSON formatting strings.
# MAGIC - Two metadata columns are added to track when new data is ingested.
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE BRONZE TABLE FOR BUSINESS EVENTS DATA
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.bronze_demo
# MAGIC TBLPROPERTIES (
# MAGIC   'pipelines.reset.allowed' = false,
# MAGIC   'delta.feature.variantType-preview' = 'supported'
# MAGIC ) 
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(key AS STRING) AS event_id,
# MAGIC   PARSE_JSON(CAST(value AS STRING)) AS event_data_variant,
# MAGIC   CAST(topic AS STRING) AS event_group,
# MAGIC   CAST(partition AS STRING) AS partition,
# MAGIC   CAST(offset AS STRING) AS offset,
# MAGIC   -- Adding metadata columns
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${business_events_source}'
# MAGIC );
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the Code**
# MAGIC
# MAGIC The `TBLPROPERTIES` statement configures the following settings:
# MAGIC - **Reset Protection**: The `'pipelines.reset.allowed' = false` property prevents full refreshes on the streaming table, which helps avoid accidentally removing checkpoints and truncating the streaming table data
# MAGIC - **Variant Type Support**: `'delta.feature.variantType-preview' = 'supported'` enables the variant data type, which lets you efficiently store and query semi-structured data like JSON. This property must be set to unlock native support for variant columns
# MAGIC
# MAGIC #### IMPORTANT: Understanding Full Table Refresh Protection
# MAGIC
# MAGIC This protection is particularly important when your raw data source automatically removes files after a certain timeframe. Without this setting, data that is no longer present in the source directory would not be re-ingested into the target table during a **Run pipeline with full table refresh** operation.
# MAGIC
# MAGIC **NOTE:** For guidance on when to use full refreshes, see the [Should I use a full refresh?](https://docs.databricks.com/aws/en/ldp/updates#should-i-use-a-full-refresh) documentation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Fan Out the Bronze Table by Business Event
# MAGIC
# MAGIC At this stage, we started with a single multiplexed bronze streaming table. The goal is to demultiplex this data by fanning it out into separate intermediate tables, one for each business event type.
# MAGIC
# MAGIC This step creates three domain specific intermediate tables:
# MAGIC
# MAGIC - **marketing_intermediate**
# MAGIC - **logistics_intermediate**
# MAGIC - **store_ops_intermediate**
# MAGIC
# MAGIC Each table contains only the events relevant to its business domain, making downstream processing and analytics simpler and more focused.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Create the Marketing Bronze Table
# MAGIC
# MAGIC Begin by creating **the** marketing intermediate table by:
# MAGIC - Filtering for records where `event_group = 'business_events_marketing'`
# MAGIC - Converting columns to their appropriate data types
# MAGIC - Including metadata columns to track ingested records
# MAGIC
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE MARKETING INTERMEDIATE TABLE
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.marketing_intermediate
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.variantType-preview' = 'supported'
# MAGIC ) 
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_data_variant,
# MAGIC   event_group,
# MAGIC   event_data_variant:event_id::STRING AS extracted_event_id,
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS timestamp,
# MAGIC   event_data_variant:event_type::STRING AS event_type,
# MAGIC   event_data_variant:subsidiary_id::STRING AS subsidiary_id,
# MAGIC   event_data_variant:campaign_id::STRING AS campaign_id,
# MAGIC   event_data_variant:channel::STRING AS channel,
# MAGIC   event_data_variant:impressions::LONG AS impressions,
# MAGIC   event_data_variant:clicks::LONG AS clicks,
# MAGIC   event_data_variant:conversions::LONG AS conversions,
# MAGIC   event_data_variant:spend_usd::DOUBLE AS spend_usd,
# MAGIC   source_file,
# MAGIC   file_mod_time
# MAGIC FROM STREAM multiplex_1_bronze.bronze_demo
# MAGIC WHERE event_group = 'business_events_marketing';
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Create the Logistics Bronze Table
# MAGIC
# MAGIC Now, creating the logistics intermediate table by:
# MAGIC - Filtering for records where `event_group = 'business_events_logistics'`
# MAGIC - Converting columns to their appropriate data types
# MAGIC - Including metadata columns to track ingested records
# MAGIC
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE LOGISTICS INTERMEDIATE TABLE
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.logistics_intermediate
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.variantType-preview' = 'supported'
# MAGIC ) 
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_data_variant,
# MAGIC   event_group,
# MAGIC   event_data_variant:event_id::STRING AS extracted_event_id,
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS timestamp,
# MAGIC   event_data_variant:event_type::STRING AS event_type,
# MAGIC   event_data_variant:subsidiary_id::STRING AS subsidiary_id,
# MAGIC   event_data_variant:warehouse_id::STRING AS warehouse_id,
# MAGIC   event_data_variant:carrier::STRING AS carrier,
# MAGIC   event_data_variant:batch_id::STRING AS batch_id,
# MAGIC   event_data_variant:num_packages::LONG AS num_packages,
# MAGIC   event_data_variant:destination_region::STRING AS destination_region,
# MAGIC   source_file,
# MAGIC   file_mod_time
# MAGIC FROM STREAM multiplex_1_bronze.bronze_demo
# MAGIC WHERE event_group = 'business_events_logistics';
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Create the Store Operations Bronze Table
# MAGIC
# MAGIC Lastly, create the store operation intermediate table by:
# MAGIC - Filtering for records where `event_group = 'business_events_store_ops'`
# MAGIC - Converting columns to their appropriate data types
# MAGIC - Including metadata columns to track ingested records
# MAGIC
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE STORE OPERATIONS INTERMEDIATE TABLE
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.store_ops_intermediate
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.variantType-preview' = 'supported'
# MAGIC ) 
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_data_variant,
# MAGIC   event_group,
# MAGIC   event_data_variant:event_id::STRING AS extracted_event_id,
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS extracted_timestamp,
# MAGIC   event_data_variant:event_type::STRING AS event_type,
# MAGIC   event_data_variant:subsidiary_id::STRING AS subsidiary_id,
# MAGIC   event_data_variant:store_id::STRING AS store_id,
# MAGIC   event_data_variant:city::STRING AS city,
# MAGIC   event_data_variant:region::STRING AS region,
# MAGIC   event_data_variant:opened_by_employee_id::STRING AS opened_by_employee_id,
# MAGIC   source_file,
# MAGIC   file_mod_time
# MAGIC FROM STREAM multiplex_1_bronze.bronze_demo
# MAGIC WHERE event_group = 'business_events_store_ops';
# MAGIC
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Configure the Pipeline Parameters
# MAGIC
# MAGIC 1. Run the cell below to retrieve the key-value pairs needed to set your pipeline configuration parameters for the **source volume**.

# COMMAND ----------

config_parameters = [
    ('business_events_source', f'{my_vol_path}/business_events'),
    ('my_catalog',my_catalog)
]

for key, value in config_parameters:
    print(f"Key: {key}\nValue: {value}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the paths above and add each one as a configuration parameter in your **Spark Declarative Pipeline**.
# MAGIC
# MAGIC     This will allow your pipeline to reference each volume through parameters.
# MAGIC
# MAGIC    a. Select **Settings** in your pipeline tab  
# MAGIC
# MAGIC    b. Under **Configuration**, select **Add configuration**
# MAGIC
# MAGIC    c. For each **Key**, enter the key name shown above  
# MAGIC
# MAGIC    d. For each **Value**, enter the corresponding volume path  
# MAGIC    
# MAGIC    e. Select **Save**
# MAGIC
# MAGIC **NOTE:** For more details on configuration parameters, see the Databricks documentation: [Use parameters with Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E5. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm that it completes successfully.
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines editor, explore the pipeline run:
# MAGIC    - Verify that **372** rows were ingested into the bronze demo table from the `business_events` source volume
# MAGIC    - Confirm the row counts for each intermediate streaming table:
# MAGIC      - **194** rows in **logistics_intermediate**
# MAGIC      - **105** rows in **marketing_intermediate**
# MAGIC      - **73** rows in **store_ops_intermediate**
# MAGIC    - Select the **bronze_demo** table and open the **Data** tab to preview all of the ingested records business event data.
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, make sure your volumes are created and your configuration parameters are set correctly.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC
# MAGIC <img src="./Includes/images/multiplex/checkpoint_bronze.png" alt="Bronze" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Create Silver Layer Tables
# MAGIC
# MAGIC Next, you will create the silver layer tables that process and clean the bronze data for each business domain. Start by creating a new file in your **multiplex_pipeline**:
# MAGIC
# MAGIC 1. Click the kebab menu next to your **multiplex_pipeline** folder.
# MAGIC
# MAGIC 2. Select **Create file**
# MAGIC
# MAGIC 3. Select the language as **SQL**
# MAGIC
# MAGIC 4. Name the file `silver_transformation.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create the Silver Table for Marketing Data
# MAGIC
# MAGIC In this step, we transform the raw marketing events into a curated silver table, **multiplex_2_silver.marketing_silver_demo**, which is optimized for analytics.
# MAGIC
# MAGIC Here's what the silver transformation does:
# MAGIC
# MAGIC - Reads data from the **marketing_intermediate** streaming table.
# MAGIC - Uses `COALESCE` to create a reliable **event_id**, ensuring each row has a unique identifier.
# MAGIC - Selects key business fields needed for analysis, such as campaign, channel, impressions, clicks, and spend.
# MAGIC - Keeps the data at the same level of detail (no aggregation or filtering that changes the number of rows).
# MAGIC - Calculates important marketing metrics:
# MAGIC   - **click_through_rate**: Measures how often people click after seeing an ad (clicks divided by impressions).
# MAGIC   - **cost_per_click**: Shows how much each click costs (spend divided by clicks).
# MAGIC - Safely handles cases where impressions or clicks are zero to prevent errors in metric calculations.
# MAGIC - Produces a clean, structured streaming table ready for dashboards and reporting.
# MAGIC
# MAGIC This silver table ensures consistent data and trusted metrics for all downstream analytics.
# MAGIC
# MAGIC
# MAGIC 1. Copy the code into your `silver_transformation.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE SILVER TABLE FOR MARKETING DATA
# MAGIC ------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.marketing_silver_demo
# MAGIC AS SELECT 
# MAGIC   COALESCE(extracted_event_id, event_id) AS event_id,
# MAGIC   timestamp,
# MAGIC   event_group,
# MAGIC   event_type,
# MAGIC   subsidiary_id,
# MAGIC   campaign_id,
# MAGIC   channel,
# MAGIC   impressions,
# MAGIC   clicks,
# MAGIC   conversions,
# MAGIC   spend_usd,
# MAGIC   CASE WHEN impressions > 0 
# MAGIC     THEN clicks / impressions 
# MAGIC     ELSE 0 
# MAGIC   END AS click_through_rate,
# MAGIC   CASE WHEN clicks > 0 
# MAGIC     THEN spend_usd / clicks 
# MAGIC     ELSE 0 
# MAGIC   END AS cost_per_click
# MAGIC FROM STREAM multiplex_1_bronze.marketing_intermediate;
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Create Silver Table for Logistics Data
# MAGIC
# MAGIC In this step, we transform the raw logistics events into a clean silver table, **multiplex_2_silver.logistics_silver_demo**, which is optimized for supply chain tracking and reporting.
# MAGIC
# MAGIC Here's what the silver transformation does:
# MAGIC
# MAGIC - Reads data from the **logistics_intermediate** streaming table.
# MAGIC - Uses `COALESCE` to create a reliable **event_id**, ensuring every shipment record is uniquely identifiable.
# MAGIC - Applies data quality filters to remove incomplete records (filters out rows where **warehouse_id** or **batch_id** are missing).
# MAGIC - Selects essential logistics attributes such as warehouse, carrier, batch ID, and destination region.
# MAGIC - Adds calculated fields:
# MAGIC   - **is_valid_shipment**: A boolean flag that verifies if the shipment contains actual packages (true if `num_packages` > 0), helping to identify ghost shipments or system errors.
# MAGIC   - **event_date**: Extracts the date from the timestamp to support daily reporting and partitioning.
# MAGIC - Produces a trusted, high-quality streaming table ready for operational dashboards.
# MAGIC
# MAGIC This silver table ensures that downstream analysis is performed only on valid, complete logistics records.
# MAGIC
# MAGIC 1. Copy the code into your `silver_transformation.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE SILVER TABLE FOR LOGISTICS DATA
# MAGIC ------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.logistics_silver_demo
# MAGIC AS SELECT 
# MAGIC   COALESCE(extracted_event_id, event_id) AS event_id,
# MAGIC   timestamp,
# MAGIC   event_group,
# MAGIC   event_type,
# MAGIC   subsidiary_id,
# MAGIC   warehouse_id,
# MAGIC   carrier,
# MAGIC   batch_id,
# MAGIC   num_packages,
# MAGIC   destination_region,
# MAGIC   CASE WHEN num_packages > 0 THEN TRUE ELSE FALSE END AS is_valid_shipment,
# MAGIC   DATE(timestamp) AS event_date
# MAGIC FROM STREAM multiplex_1_bronze.logistics_intermediate
# MAGIC WHERE warehouse_id IS NOT NULL
# MAGIC   AND batch_id IS NOT NULL;
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. Create Silver Table for Store Operations Data
# MAGIC
# MAGIC In this step, we transform raw store operations events into a structured silver table, **multiplex_2_silver.store_ops_silver_demo**, designed for operational oversight and workforce analysis.
# MAGIC
# MAGIC Key aspects of the silver transformation:
# MAGIC
# MAGIC - Reads data from the **store_ops_intermediate** streaming table.
# MAGIC - Uses `COALESCE` to ensure every operation has a consistent and unique **event_id**.
# MAGIC - Enforces strict data quality by filtering out records missing critical fields (timestamp, store ID, or event type).
# MAGIC - Standardizes the time column by renaming **extracted_timestamp** to a common **timestamp** field.
# MAGIC - Enriches the data with derived columns for temporal analysis and store identification:
# MAGIC   - **event_date** and **event_hour**: Extracted to support daily reporting and analysis of peak operational hours.
# MAGIC   - **store_number**: Parses the **store_id** (using a split function) to isolate the numeric identifier, making reporting easier.
# MAGIC - Produces a high-quality streaming table ready for regional and store-level dashboards.
# MAGIC
# MAGIC This silver table ensures that operational analytics are based on valid, complete records with detailed time dimensions.
# MAGIC
# MAGIC 1. Copy the code into your `silver_transformation.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE SILVER TABLE FOR STORE OPERATIONS DATA
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.store_ops_silver_demo
# MAGIC AS SELECT 
# MAGIC   COALESCE(extracted_event_id, event_id) AS event_id,
# MAGIC   extracted_timestamp AS timestamp,
# MAGIC   event_group,
# MAGIC   event_type,
# MAGIC   subsidiary_id,
# MAGIC   store_id,
# MAGIC   city,
# MAGIC   region,
# MAGIC   opened_by_employee_id,
# MAGIC   DATE(extracted_timestamp) AS event_date,
# MAGIC   HOUR(extracted_timestamp) AS event_hour,
# MAGIC   SPLIT(store_id, '_')[2] AS store_number
# MAGIC FROM STREAM multiplex_1_bronze.store_ops_intermediate
# MAGIC WHERE extracted_timestamp IS NOT NULL
# MAGIC   AND store_id IS NOT NULL
# MAGIC   AND event_type IS NOT NULL;
# MAGIC
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F4. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully 
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines editor, explore the pipeline run:
# MAGIC    - Confirm the row counts for each silver streaming table:
# MAGIC      - **194** rows in **logistics_silver_demo**
# MAGIC      - **105** rows in **marketing_silver_demo**
# MAGIC      - **73** rows in **store_ops_silver_demo**
# MAGIC    - Select any silver table and open the **Data** tab to preview the results
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, confirm that your volumes were created and that your configuration parameters are set correctly.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC
# MAGIC <img src="./Includes/images/multiplex/checkpoint_silver.png" alt="silver" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### F5. Query the Silver Streaming Tables
# MAGIC
# MAGIC 1. Verify that the data has been properly demultiplexed into the silver layer tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_2_silver.marketing_silver_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_2_silver.logistics_silver_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_2_silver.store_ops_silver_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Create Gold Layer Views
# MAGIC
# MAGIC With the silver tables in place, we can now build the gold layer.
# MAGIC
# MAGIC The gold layer consists of materialized views that:
# MAGIC
# MAGIC - Aggregate data from the silver tables
# MAGIC - Apply business friendly calculations and groupings
# MAGIC - Provide stable, analytics ready datasets for dashboards and reporting
# MAGIC - Reduce query complexity for downstream consumers
# MAGIC
# MAGIC In this section, we will create a materialized view on top of the marketing silver table to support common business questions.
# MAGIC
# MAGIC **NOTE:** For this training, we will create a single materialized view. In a real production scenario, you would typically create multiple materialized views to support different analytics needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Create a New SQL File in Your Pipeline
# MAGIC
# MAGIC 1. On your **multiplex_pipeline** folder select the kebab menu and select **Create File**
# MAGIC
# MAGIC 2. Select the language as **SQL**
# MAGIC
# MAGIC 3. Name the file `gold_view.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Create Gold View for Marketing Campaign Summary
# MAGIC In this step, we elevate the data to the Gold layer by creating a materialized view, **multiplex_3_gold.marketing_campaign_summary**. This view aggregates the granular event data into high-level performance metrics, optimized for executive reporting and dashboards.
# MAGIC
# MAGIC Here's what this transformation does:
# MAGIC
# MAGIC - Aggregates data from the **marketing_silver_demo** table, grouping it by Campaign, Subsidiary, and Channel to provide a clear performance summary.
# MAGIC - **Calculates Totals:** Sums up key volume metrics (events, impressions, clicks, conversions) and financial metrics (total spend) for each group.
# MAGIC - Derives Strategic KPIs:
# MAGIC   - **ctr_percentage**: The Click-Through Rate formatted as a percentage.
# MAGIC   - **conversion_rate_percentage**: The percentage of clicks that resulted in a successful conversion.
# MAGIC   - **cost_per_conversion**: The average amount spent to acquire a single conversion (CPA).
# MAGIC - Ensures Data Quality:
# MAGIC   - Uses `NULLIF` to prevent "division by zero" errors when calculating ratios.
# MAGIC   - Uses `ROUND` to format currency and percentages to two decimal places for cleaner presentation.
# MAGIC
# MAGIC This materialized view serves as the "source of truth" for campaign performance dashboards, enabling stakeholders to compare channel effectiveness and ROI instantly.
# MAGIC
# MAGIC 1. Copy the code below in your `gold_view.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE MATERIALIZED VIEW FOR MARKETING DATA
# MAGIC ------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW multiplex_3_gold.marketing_campaign_summary
# MAGIC AS SELECT
# MAGIC   campaign_id,
# MAGIC   subsidiary_id,
# MAGIC   channel,
# MAGIC   COUNT(*) AS total_events,
# MAGIC   SUM(impressions) AS total_impressions,
# MAGIC   SUM(clicks) AS total_clicks,
# MAGIC   SUM(conversions) AS total_conversions,
# MAGIC   ROUND(SUM(spend_usd), 2) AS total_spend_usd,
# MAGIC   ROUND((SUM(clicks) * 1.0 / NULLIF(SUM(impressions), 0)) * 100, 2) AS ctr_percentage,
# MAGIC   ROUND((SUM(conversions) * 1.0 / NULLIF(SUM(clicks), 0)) * 100, 2) AS conversion_rate_percentage,
# MAGIC   ROUND(SUM(spend_usd) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion
# MAGIC FROM multiplex_2_silver.marketing_silver_demo
# MAGIC GROUP BY campaign_id, subsidiary_id, channel;
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully 
# MAGIC
# MAGIC 2. Explore the run in the Lakeflow Pipelines Editor:
# MAGIC    - Confirm **96** rows were ingested into the **marketing_campaign_summary** materialized view
# MAGIC    - Preview the data in the editor (Select **marketing_campaign_summary** → **Data** tab)
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, confirm that your volumes were created and that your configuration parameters are set correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_3_gold.marketing_campaign_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Enable Iceberg Reads
# MAGIC
# MAGIC Now, we want to enable Iceberg reads on our final streaming table to support cross-platform analytics and data sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. Create a Delta Sink
# MAGIC
# MAGIC We need to create a Delta sink table because Iceberg reads cannot be enabled directly on streaming tables or views. To do this, we will create a Delta sink from one of our streaming tables.
# MAGIC
# MAGIC 1. What are sinks in Spark Declarative Pipelines, and how do we use them?
# MAGIC
# MAGIC - [Sinks in Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/sinks)
# MAGIC
# MAGIC - [Using sinks in pipelines](https://docs.databricks.com/aws/en/ldp/ldp-sinks)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Begin by creating a new Python File in Your Pipeline to create the **delta_sink_logistics** sink . 
# MAGIC     - **NOTE: Only the Python API is supported. SQL is not supported.**
# MAGIC
# MAGIC    a. Click the kebab menu on your **multiplex_pipeline** folder and select **Create file**
# MAGIC
# MAGIC    b. Select the language as **Python**
# MAGIC
# MAGIC    c. Name the file `delta_sink.py`
# MAGIC
# MAGIC    d. Copy the code below and paste in your `delta_sink.py` file.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Here is what the code below does:
# MAGIC - **Defines a Destination (Sink):** The `create_sink` function registers a specific endpoint for your data—a Delta table named `logistics_delta_sink` in the Gold schema—separately from the logic that populates it.
# MAGIC - **Establishes a Streaming Flow:** The `@dp.append_flow` decorator creates a continuous data pipeline that links your processing logic directly to the defined sink, automating the movement of data.
# MAGIC - **Enables Append-Only Logic:** The append flow optimizes performance by adding new records as they arrive, rather than re-processing or overwriting existing data.
# MAGIC - **Automates Incremental Processing:** By using `readStream`, the pipeline automatically tracks which data has already been processed (using checkpointing), ensuring that only new records from the Silver table are moved to Gold.
# MAGIC
# MAGIC 4. Copy and paste into your `delta_sink.py` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC # ------------------------------------------------------
# MAGIC #       CREATE DELTA SINK
# MAGIC # ------------------------------------------------------
# MAGIC
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC my_catalog = spark.conf.get("my_catalog")
# MAGIC
# MAGIC dp.create_sink(
# MAGIC   name = "delta_sink_logistics",
# MAGIC   format = "delta",
# MAGIC   options = { "tableName": f"{my_catalog}.multiplex_3_gold.logistics_delta_sink" }
# MAGIC )
# MAGIC
# MAGIC @dp.append_flow(name = "delta_sink_logistics_flow", target="delta_sink_logistics")
# MAGIC def delta_sink_logistics_flow():
# MAGIC   return(
# MAGIC   spark.readStream.table("multiplex_2_silver.logistics_silver_demo")
# MAGIC )
# MAGIC
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### H2. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully 
# MAGIC
# MAGIC 2. Review the pipeline results in the Lakeflow Pipelines Editor:
# MAGIC    - Ensure the **marketing_campaign_summary** materialized view contains **96** records.
# MAGIC    - Check that the record count in **logistics_silver_demo** matches the output in **delta_sink_logistics** (should be **194** records).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Pipeline Final State
# MAGIC
# MAGIC <img src="./Includes/images/multiplex/checkpoint_final.png" alt="final" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3. Examine Table Properties

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Review the table properties and note the following:
# MAGIC - In **Detailed Table Information**, confirm that the **type** is **managed** and the **provider** is **delta**. This indicates it is a managed Delta table.
# MAGIC - In the **table_properties** column of the same section, you will see that deletion vectors are enabled: `delta.enableDeletionVectors=true`.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED multiplex_3_gold.logistics_delta_sink

# COMMAND ----------

# MAGIC %md
# MAGIC ### H4. Enable Iceberg Reads
# MAGIC
# MAGIC 1. To enable Iceberg reads on a Delta table, you must **disable deletion vectors**. Deletion vectors allow soft deletes, but Iceberg requires hard deletes for compatibility.
# MAGIC
# MAGIC       **How to enable Iceberg reads:**
# MAGIC
# MAGIC       a. **Disable deletion vectors:** Turn off deletion vectors for your Delta table.
# MAGIC
# MAGIC
# MAGIC       b. **Enable Iceberg compatibility:** Set the table property to allow Iceberg reads.
# MAGIC
# MAGIC > **NOTE:**  
# MAGIC > With [Iceberg v3](https://docs.databricks.com/aws/en/iceberg/iceberg-v3) (private preview as of January 21, 2026), you do **not** need to disable deletion vectors. 
# MAGIC
# MAGIC For more details, see the [Databricks documentation on deletion vectors](https://docs.databricks.com/aws/en/delta/deletion-vectors).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Disable deletion vectors
# MAGIC ALTER TABLE multiplex_3_gold.logistics_delta_sink 
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.enableDeletionVectors' = 'false'
# MAGIC   );
# MAGIC
# MAGIC -- Step 2: Enable Iceberg compatibility
# MAGIC ALTER TABLE multiplex_3_gold.logistics_delta_sink 
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'delta.enableIcebergCompatV2' = 'true',
# MAGIC     'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's review the table properties and observe the changes:
# MAGIC - You will see a new section called **Delta Uniform Iceberg**, which includes details such as 
# MAGIC   - **metadata location**, 
# MAGIC   - **converted delta version**,
# MAGIC   - **converted delta timestamp**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED multiplex_3_gold.logistics_delta_sink;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Let's view the table properties:
# MAGIC - `delta.universalFormat.enabledFormats = iceberg` confirms that Iceberg format is enabled.
# MAGIC - `delta.enableDeletionVectors = false` shows that deletion vectors is not enabled.
# MAGIC - The table currently supports Iceberg version 2: `delta.feature.icebergCompatV2 = supported`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES multiplex_3_gold.logistics_delta_sink;

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Land Another File in Cloud Storage
# MAGIC
# MAGIC Earlier in the workshop, we ingested a single file. In this step, we will land an additional file into the cloud storage volume to demonstrate how Spark Declarative Pipelines respond when new data is detected.
# MAGIC
# MAGIC 1. Run the command below to add the new file to your source volume, then confirm that the volume now contains two files.
# MAGIC

# COMMAND ----------

ops_path = f'/Volumes/{my_catalog}/multiplex_1_bronze/ops'
business_events_source_path = f'/Volumes/{my_catalog}/multiplex_1_bronze/business_events'


#copy files from Ops location to user's source volume
copy_files(copy_from = f'{ops_path}', copy_to = business_events_source_path, n = 2)

spark.sql(f"LIST '{business_events_source_path}' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's review the number of rows present in the newly landed file that will be ingested by our pipeline:
# MAGIC - **Marketing**: 62 new records
# MAGIC - **Store Ops**: 80 new records
# MAGIC - **Logistics**: 158 new records
# MAGIC - **Total**: 300 new records in this file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     topic,
# MAGIC     count(*) as number_of_records_per_event_group , 
# MAGIC     sum(count(*)) OVER () as total_records_in_raw_file
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events/part-02_business_events.parquet'
# MAGIC )
# MAGIC GROUP BY topic;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the Spark Declarative Pipeline to incrementally process the new file and confirm that it completes successfully.
# MAGIC
# MAGIC 4. In the Lakeflow Pipelines editor, review the pipeline run:
# MAGIC    - Verify that **300** rows were ingested into the bronze demo table from the `business_events` source volume with the new landed file.
# MAGIC    - Confirm the row counts for each intermediate bronze and silver streaming table:
# MAGIC      - **158** rows in both **logistics_intermediate** and **logistics_silver_demo**
# MAGIC      - **80** rows in both **store_ops_intermediate** and **store_ops_silver_ops**
# MAGIC      - **62** rows in both **marketing_intermediate** and **marketing_silver_demo**
# MAGIC    - Verify that the materialized view **marketing_campaign_summary** contains **141** output records.
# MAGIC    - Ensure that **delta_sink_logistics** has the same number of records as **logistics_silver_demo** - **158** records.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Summary and Key Takeaways 
# MAGIC - **Multiplex Data Pipeline Architecture**: Successfully built a complete multiplex streaming pipeline using Spark Declarative Pipelines that ingested data from a single source and demultiplexed it into three separate business domain tables (marketing, logistics, and store operations)
# MAGIC - **VARIANT Data Type Implementation**: Leveraged the VARIANT data type to efficiently handle semi-structured JSON data with different schemas across multiple event types in a single bronze table
# MAGIC - **Medallion Architecture with Streaming**: Implemented the full medallion pattern (bronze → silver → gold) using streaming tables, intermediate transformations, and materialized views for real-time data processing
# MAGIC - **Delta Sinks and Iceberg Compatibility**: Created Delta sinks using the Python API and enabled Iceberg compatibility for cross-platform analytics by disabling deletion vectors and configuring universal format properties
# MAGIC
# MAGIC #### Business Value Delivered
# MAGIC
# MAGIC The pipeline processed **672 total records** from two source files containing multiplexed business events, creating domain-specific analytics tables that enable:
# MAGIC - Real-time operational insights across marketing campaigns, logistics shipments, and store operations
# MAGIC - Cross-platform data access through Iceberg compatibility for broader ecosystem integration  
# MAGIC - Scalable event processing architecture that can handle additional business domains and event types
# MAGIC - Automated incremental processing with streaming checkpoints ensuring exactly-once delivery
# MAGIC
# MAGIC This multiplex architecture provides a foundation for enterprise-scale event processing while maintaining data lineage, real-time capabilities, and cross-platform compatibility through Delta Universal Format.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>