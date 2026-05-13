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
# MAGIC # Lab - Building Multi-Source E-commerce Pipeline with Spark Declarative Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Scenario
# MAGIC
# MAGIC You are a data engineer working for an e-commerce company that receives orders from two channels: their website and mobile app. These channels store data in different formats and may have some similarities and differences in how they capture order information. Your role is to build a streaming data pipeline using Spark Declarative Pipelines that can process data from these different channels and generate weekly revenue reports for business stakeholders.
# MAGIC
# MAGIC This lab will demonstrate advanced techniques including multi-flow ingestion, stream-static joins, data quality expectations, and materialized views for analytics.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Classroom Setup
# MAGIC Follow the cells below to set up your workspace for the lab.
# MAGIC
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
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this notebook in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like:
# MAGIC     <strong>labuser12345</strong> (series of unique numbers)
# MAGIC   </div>
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
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this notebook in your own Databricks workspace or Databricks Free Edition, the setup will
# MAGIC <strong>create a Unity Catalog catalog and schema for you</strong>.
# MAGIC   - **Create catalog permission is required.**
# MAGIC
# MAGIC - The catalog name is derived from your Databricks username and follows this pattern: <strong>labuser_username</strong>
# MAGIC   </div>
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
# MAGIC Run the cell below to initialize your environment. This setup step does the following:
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks provided Vocareum workspace
# MAGIC     - Creates three schemas in your specified catalog:
# MAGIC         - **lab_1_bronze**
# MAGIC         - **lab_2_silver**
# MAGIC         - **lab_3_gold**
# MAGIC     - Creates `ops` and `source` volumes in your **YOUR_LABUSER_CATALOG.lab_1_bronze** schema, and adds sample files to your volumes.
# MAGIC     - Verifies your selected compute environment
# MAGIC
# MAGIC     This ensures that all schemas, tables and objects are created in your catalog.
# MAGIC
# MAGIC > **Important:** You must have permission to create catalogs in your own non-Vocareum workspace. If you do not have the required permissions, this step will fail. Review the note below before continuing.

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

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-lab

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Verify Volume Path Configuration
# MAGIC
# MAGIC Run the cell below to view the value of the `my_vol_path` variable.
# MAGIC
# MAGIC Confirm that the value references your **your-catalog.lab_1_bronze** path. This will be used to dynamically reference your source volumes throughout this lab.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Source Data Characteristics
# MAGIC
# MAGIC Before building our pipeline, let's understand the structure and characteristics of our source data. This e-commerce company receives orders from two different channels, each with its own data format and schema variations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Understanding Data File Structure
# MAGIC
# MAGIC Our pipeline will process data from multiple sources, each stored in different volumes with specific formats and purposes:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | File                | Destination Volume | Format | Purpose                                 |
# MAGIC |---------------------|-------------------|--------|-----------------------------------------|
# MAGIC | `web_orders_1.csv`  | web_orders        | CSV    | Web channel orders                      |
# MAGIC | `app_orders_1.json` | app_orders        | JSON   | Mobile app orders                       |
# MAGIC | `product_catalog.csv`| ops              | CSV    | Static product reference data           |

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Listing Data in Destination Volumes
# MAGIC
# MAGIC Let's examine the files available in each volume to understand our data sources.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Operations Volume
# MAGIC
# MAGIC The operations volume contains static reference data like product catalogs.

# COMMAND ----------

# List files in ops volume
display(spark.sql(f"LIST '{my_vol_path}/ops'"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Web Orders Volume
# MAGIC
# MAGIC The web orders volume contains CSV files with order data from the company's website.

# COMMAND ----------

# List files in web_orders volume
display(spark.sql(f"LIST '{my_vol_path}/web_orders'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. App Orders Volume
# MAGIC
# MAGIC The app orders volume contains JSON files with order data from the mobile application.

# COMMAND ----------

# List files in app_orders volume
display(spark.sql(f"LIST '{my_vol_path}/app_orders'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Analyzing Data Structure and Content
# MAGIC
# MAGIC Now let's examine the actual data structure and content from each source to understand schema differences and commonalities.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Analyzing Operations Data
# MAGIC
# MAGIC The operations volume contains product catalog information that will be used for enriching order data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/ops/'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Analyzing Web Orders Data
# MAGIC
# MAGIC Web orders are stored in CSV format and contain web-specific metadata like browser and session information.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/web_orders/'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Analyzing App Orders Data
# MAGIC
# MAGIC App orders are stored in JSON format and contain app-specific metadata like app version and device model.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/app_orders/'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint - Data Exploration
# MAGIC
# MAGIC Confirm the following data counts:
# MAGIC - Web orders: Rows loaded from `web_orders_1.csv`
# MAGIC - App orders: Rows loaded from `app_orders_1.json`
# MAGIC - Product catalog: 50 products
# MAGIC
# MAGIC **TROUBLESHOOTING:** If any file shows 0 rows, verify `my_vol_path` is correct and all files were uploaded successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create the Spark Declarative Pipeline
# MAGIC
# MAGIC Now we'll create our Spark Declarative Pipeline using the Lakeflow Pipelines Editor to process data from multiple sources.

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
# MAGIC ### C2. Create a Lakeflow Spark Declarative Pipeline
# MAGIC
# MAGIC Complete the following steps to create your Spark Declarative Pipeline using the Lakeflow Pipelines Editor:
# MAGIC
# MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open link in New Tab**.  
# MAGIC
# MAGIC 2. In the new tab, select **Create → ETL Pipeline**.  
# MAGIC
# MAGIC    **NOTE:** If prompted to **Try the new Lakeflow Pipelines Editor**, choose **Enable Lakeflow Pipelines Editor**. This appears only if you did not complete the previous step.  
# MAGIC
# MAGIC 3. Configure the pipeline settings:
# MAGIC    - **Pipeline Name**: `lab_ecommerce_yourname`
# MAGIC    - **Default Catalog**: `YOUR_LABUSER_CATALOG`
# MAGIC    - **Default Schema**: `lab_1_bronze`  
# MAGIC    **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. **Rename pipeline components**:
# MAGIC    - Rename the **transformations** folder to `ecommerce_pipeline`
# MAGIC    - Rename **my_transformations.sql** file to `bronze_ingestion.sql`
# MAGIC
# MAGIC 5. Leave the **Lakeflow Pipelines Editor** page open for the next steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Configure Pipeline Parameters
# MAGIC
# MAGIC Pipeline parameters allow us to dynamically reference our source volumes across different environments.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to retrieve the key-value pairs needed to set your pipeline configuration parameters for the source volumes.

# COMMAND ----------

config_parameters = [
    ("web_orders_source",      f"{my_vol_path}/web_orders"),
    ("app_orders_source",      f"{my_vol_path}/app_orders"),
    ("product_catalog_source", f"{my_vol_path}/ops"),
]

print("=" * 65)
print("  Add these as Configuration Parameters in your Pipeline:")
print("=" * 65)
for key, value in config_parameters:
    print(f"  Key  : {key}")
    print(f"  Value: {value}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the paths above and add each one as a configuration parameter in your **Spark Declarative Pipeline**.
# MAGIC
# MAGIC This will allow your pipeline to reference each volume through parameters.
# MAGIC
# MAGIC 1. Select **Settings** in your pipeline tab  
# MAGIC
# MAGIC 2. Under **Configuration**, select **Add configuration**
# MAGIC
# MAGIC 3. For each **Key**, enter the key name shown above  
# MAGIC
# MAGIC 4. For each **Value**, enter the corresponding volume path  
# MAGIC
# MAGIC 5. Select **Save**
# MAGIC
# MAGIC **NOTE:** For more details on configuration parameters, see the Databricks documentation: [Use parameters with Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Bronze Layer — Multi-Flow Ingestion
# MAGIC
# MAGIC We have orders data coming from two sources: **web and app**. The Bronze layer will use multi-flow ingestion to combine data from both sources into a unified table.
# MAGIC
# MAGIC To build the Bronze layer, follow these three steps:
# MAGIC 1. Create a Bronze table that can accept data from both sources.
# MAGIC 2. Create a flow for web orders data.
# MAGIC 3. Create a flow for app orders data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Understand the Multi-Flow Design
# MAGIC
# MAGIC Both channels share core order columns but differ in channel-specific metadata:
# MAGIC
# MAGIC | Column | Web CSV | App JSON | Bronze Handling |
# MAGIC |--------|---------|----------|----------------|
# MAGIC | **order_id**, **customer_id**, **sku** | Yes | Yes | Both flows |
# MAGIC  **discount_code** | Yes | Yes | Schema hint applied |
# MAGIC  **browser**, **session_id** | Yes | No | Web-only; NULL for app |
# MAGIC  **app_version**, **device_model** | No | Yes | App-only; NULL for web |

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Creating Bronze Table
# MAGIC
# MAGIC Create a **Bronze** streaming table that acts as a single raw landing table for both web and app orders using `CREATE OR REPLACE STREAMING TABLE`.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Create the table in the **lab_1_bronze** schema and name the table **combined_orders_raw**.  
# MAGIC - Include:
# MAGIC   - All common columns.
# MAGIC   - Source‑specific columns.  
# MAGIC   - Future‑facing columns that may not exist yet in current files (`discount_code`).  
# MAGIC - Add metadata columns:
# MAGIC   - **source_file** (file path or logical source name)  
# MAGIC   - **file_mod_time** (`TIMESTAMP`)  
# MAGIC   - **ingestion_time** (`TIMESTAMP`)  
# MAGIC - Use `STRING` for all business columns, and `TIMESTAMP` only for the time metadata columns.   
# MAGIC - In `TBLPROPERTIES`, set `pipelines.reset.allowed = false`.  
# MAGIC
# MAGIC **To Do:**
# MAGIC Using these requirements, write the full `CREATE OR REPLACE STREAMING TABLE` statement, including all columns, data types, `COMMENT`, and `TBLPROPERTIES`.

# COMMAND ----------

## Create the Bronze streaming table for combined orders
## Use the requirements specified above

%sql
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### CODE ANSWER - Bronze Table Structure
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ----------------------------------------------------------------
# MAGIC -- STEP 1: Define the unified Bronze streaming table
# MAGIC -- All business columns stored as STRING for schema flexibility.
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE OR REPLACE STREAMING TABLE lab_1_bronze.combined_orders_raw
# MAGIC (
# MAGIC   order_id          STRING,
# MAGIC   order_date        STRING,
# MAGIC   customer_id       STRING,
# MAGIC   sku               STRING,
# MAGIC   model             STRING,
# MAGIC   category          STRING,
# MAGIC   order_type        STRING,
# MAGIC   channel           STRING,
# MAGIC   quantity          STRING,
# MAGIC   unit_price        STRING,
# MAGIC   discount_code     STRING,
# MAGIC   discount_amount   STRING,
# MAGIC   total_amount      STRING,
# MAGIC   payment_method    STRING,
# MAGIC   order_status      STRING,
# MAGIC   city              STRING,
# MAGIC   region            STRING,
# MAGIC   ship_date         STRING,
# MAGIC   browser           STRING,
# MAGIC   session_id        STRING,
# MAGIC   os                STRING,
# MAGIC   app_version       STRING,
# MAGIC   device_model      STRING,
# MAGIC   source_file       STRING,
# MAGIC   file_mod_time     TIMESTAMP,
# MAGIC   ingestion_time    TIMESTAMP
# MAGIC )
# MAGIC COMMENT "Unified Bronze streaming table - web and app orders combined via multi-flow ingestion."
# MAGIC TBLPROPERTIES (
# MAGIC   'pipelines.reset.allowed' = false
# MAGIC );
# MAGIC </code></pre>
# MAGIC
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
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Creating Flows for Each Source
# MAGIC
# MAGIC Now that our Bronze table is ready, we will create two flows: one for app orders and another for web orders. Each flow will ingest data into our streaming Bronze table.

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. App Orders Flow
# MAGIC
# MAGIC Now, we are going to create a flow for ingesting App Orders:
# MAGIC - Create a flow to ingest **JSON** app orders using `read_files`.
# MAGIC - Cast all business fields to `STRING` for schema flexibility.
# MAGIC - Add metadata columns (`source_file`, `file_mod_time`) and `ingestion_time`.
# MAGIC - Insert data into the unified Bronze table using `INSERT INTO ... BY NAME` to align columns by name.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- STEP 2: App Orders Flow
# MAGIC -- Reads JSON files from the app_orders volume.
# MAGIC -- Web-specific columns set to NULL.
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE FLOW app_orders_flow
# MAGIC AS INSERT INTO lab_1_bronze.combined_orders_raw BY NAME
# MAGIC SELECT
# MAGIC   CAST(order_id        AS STRING)  AS order_id,
# MAGIC   CAST(order_date      AS STRING)  AS order_date,
# MAGIC   CAST(customer_id     AS STRING)  AS customer_id,
# MAGIC   CAST(sku             AS STRING)  AS sku,
# MAGIC   CAST(model           AS STRING)  AS model,
# MAGIC   CAST(category        AS STRING)  AS category,
# MAGIC   CAST(order_type      AS STRING)  AS order_type,
# MAGIC   CAST(channel         AS STRING)  AS channel,
# MAGIC   CAST(quantity        AS STRING)  AS quantity,
# MAGIC   CAST(unit_price      AS STRING)  AS unit_price,
# MAGIC   CAST(discount_code   AS STRING)  AS discount_code,
# MAGIC   CAST(discount_amount AS STRING)  AS discount_amount,
# MAGIC   CAST(total_amount    AS STRING)  AS total_amount,
# MAGIC   CAST(payment_method  AS STRING)  AS payment_method,
# MAGIC   CAST(order_status    AS STRING)  AS order_status,
# MAGIC   CAST(city            AS STRING)  AS city,
# MAGIC   CAST(region          AS STRING)  AS region,
# MAGIC   CAST(ship_date       AS STRING)  AS ship_date,
# MAGIC   CAST(os              AS STRING)  AS os,
# MAGIC   CAST(app_version     AS STRING)  AS app_version,
# MAGIC   CAST(device_model    AS STRING)  AS device_model,
# MAGIC   _metadata.file_name              AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time,
# MAGIC   current_timestamp()              AS ingestion_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${app_orders_source}',
# MAGIC   format => 'json',
# MAGIC   schemaHints => 'discount_code STRING'
# MAGIC );
# MAGIC
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
# MAGIC #### 2. Web Orders Flow
# MAGIC
# MAGIC Create a **flow** for web orders that writes into the unified **lab_1_bronze.combined_orders_raw** table using `CREATE FLOW` command.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read all the columns from the web orders source using `read_files()`
# MAGIC - Select and cast all business columns to `STRING` for schema flexibility.  
# MAGIC - Populate the metadata columns to track lineage:  
# MAGIC   - **source_file** from `_metadata.file_name`  
# MAGIC   - **file_mod_time** from `_metadata.file_modification_time`  
# MAGIC   - **ingestion_time** using `current_timestamp()` 
# MAGIC - Use `schemaHints => 'discount_code STRING'` in `read_files()` so that the **discount_code** column is detected even if it is missing in some CSV files.   
# MAGIC - Use `AS INSERT INTO lab_1_bronze.combined_orders_raw BY NAME` so columns align by name with the Bronze table schema.
# MAGIC
# MAGIC **To Do:**
# MAGIC
# MAGIC Using these requirements, write the full flow for web orders yourself.

# COMMAND ----------

## Create the web orders flow
## Follow the requirements specified above

## <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### CODE ANSWER - Web Orders Flow
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- STEP 3: Web Orders Flow
# MAGIC -- Reads CSV files from the web_orders volume.
# MAGIC -- App-specific columns set to NULL.
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE FLOW web_orders_flow
# MAGIC AS INSERT INTO lab_1_bronze.combined_orders_raw BY NAME
# MAGIC SELECT
# MAGIC   CAST(order_id        AS STRING)  AS order_id,
# MAGIC   CAST(order_date      AS STRING)  AS order_date,
# MAGIC   CAST(customer_id     AS STRING)  AS customer_id,
# MAGIC   CAST(sku             AS STRING)  AS sku,
# MAGIC   CAST(model           AS STRING)  AS model,
# MAGIC   CAST(category        AS STRING)  AS category,
# MAGIC   CAST(order_type      AS STRING)  AS order_type,
# MAGIC   CAST(channel         AS STRING)  AS channel,
# MAGIC   CAST(quantity        AS STRING)  AS quantity,
# MAGIC   CAST(unit_price      AS STRING)  AS unit_price,
# MAGIC   CAST(discount_code   AS STRING)  AS discount_code,
# MAGIC   CAST(discount_amount AS STRING)  AS discount_amount,
# MAGIC   CAST(total_amount    AS STRING)  AS total_amount,
# MAGIC   CAST(payment_method  AS STRING)  AS payment_method,
# MAGIC   CAST(order_status    AS STRING)  AS order_status,
# MAGIC   CAST(city            AS STRING)  AS city,
# MAGIC   CAST(region          AS STRING)  AS region,
# MAGIC   CAST(ship_date       AS STRING)  AS ship_date,
# MAGIC   CAST(browser         AS STRING)  AS browser,
# MAGIC   CAST(session_id      AS STRING)  AS session_id,
# MAGIC   CAST(os              AS STRING)  AS os,
# MAGIC   _metadata.file_name              AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time,
# MAGIC   current_timestamp()              AS ingestion_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${web_orders_source}',
# MAGIC   format      => 'csv',
# MAGIC   header      => true,
# MAGIC   schemaHints => 'discount_code STRING'
# MAGIC );
# MAGIC </code></pre>
# MAGIC
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm that it completes successfully.
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines Editor, explore your pipeline run:
# MAGIC    - Confirm that **200** rows were ingested into the **combined_orders_raw** table from both source volumes.
# MAGIC    - Select the **combined_orders_raw** table and open the **Data** tab to preview all ingested business event records.
# MAGIC    - Observe that web-specific columns like **browser** and **session_id** are populated only for web source records, while app-specific columns like **device_model** and **app_version** are populated only for app source records (and are NULL for web records).
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, make sure your volumes are created and your configuration parameters are set correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D5. Confirming Ingestion of Records
# MAGIC
# MAGIC Let's verify that our multi-flow ingestion is working correctly by examining the ingested data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm both flows ingested records
# MAGIC SELECT
# MAGIC   source_file,
# MAGIC   order_type,
# MAGIC   COUNT(*)            AS total_rows,
# MAGIC   MIN(ingestion_time) AS first_ingested,
# MAGIC   MAX(ingestion_time) AS last_ingested
# MAGIC FROM lab_1_bronze.combined_orders_raw
# MAGIC GROUP BY source_file, order_type
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify channel-specific columns are correctly populated or NULL
# MAGIC SELECT
# MAGIC   order_type,
# MAGIC   COUNT(*)             AS total_orders,
# MAGIC   -- Web Specific Columns
# MAGIC   COUNT(browser)       AS browser_count, 
# MAGIC   COUNT(session_id)    AS session_id_count,
# MAGIC   -- App Specific Columns
# MAGIC   COUNT(app_version)   AS app_version_count,
# MAGIC   COUNT(device_model)  AS device_model_count
# MAGIC FROM lab_1_bronze.combined_orders_raw
# MAGIC GROUP BY order_type;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint - Bronze Layer
# MAGIC
# MAGIC | Source            | Expected                                              | Notes                                                                 |
# MAGIC |-------------------|------------------------------------------------------|----------------------------------------------------------------------|
# MAGIC | `web_orders_1.csv`| Web rows ingested|  **browser** and **session_id** populated|
# MAGIC | `app_orders_1.json`| App rows ingested | **app_version** and **device_model** populated|
# MAGIC
# MAGIC **TROUBLESHOOTING:** If one flow shows 0 rows, check that the pipeline configuration parameter points to the correct volume path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Silver Layer — Expectations and Stream-Static Join
# MAGIC
# MAGIC The Silver layer will clean and validate our data using expectations, then enrich it with product catalog information through a stream-static join.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Understand the Data Quality Expectations
# MAGIC
# MAGIC Data quality expectations ensure that only valid data flows through our pipeline. Here are the constraints we'll implement:
# MAGIC
# MAGIC | Constraint Name | Expectation | Action | Why |
# MAGIC |----------------|-------------|--------|-----|
# MAGIC | `valid_order_id` | `order_id IS NOT NULL` | **FAIL UPDATE** | Primary key — NULL breaks all downstream joins |
# MAGIC | `valid_sku` | `sku IS NOT NULL` | **DROP ROW** | No product reference = unusable for analytics |
# MAGIC | `positive_quantity` | `quantity >= 1` | **DROP ROW** | Quantity of zero or less is not a valid sale |
# MAGIC | `positive_unit_price` | `unit_price > 0` | **WARN** (default) | Flags pricing anomalies without blocking the pipeline |
# MAGIC | `valid_total_amount` | `total_amount >= 0` | **WARN** (default) | Flags negative totals for review |

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Create the Silver SQL File
# MAGIC
# MAGIC 1. On your **ecommerce_pipeline** folder select the kebab menu and select **Create File**
# MAGIC
# MAGIC 2. Select the language as **SQL**
# MAGIC
# MAGIC 3. Name the file `silver_transformation.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Create the Silver Table
# MAGIC
# MAGIC **Instructions:**
# MAGIC - Create a Silver streaming table named **orders_clean** in the **lab_2_silver** schema.
# MAGIC - Use `CREATE OR REFRESH STREAMING TABLE` and select from `STREAM lab_1_bronze.combined_orders_raw`.
# MAGIC - Add data quality constraints using `CONSTRAINT` and `EXPECT` for key columns.
# MAGIC - Use `CLUSTER BY AUTO` for automatic clustering.
# MAGIC - Cast raw fields to correct types with `TRY_CAST`.
# MAGIC - Add a table comment describing the table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - **Table:** **orders_clean**
# MAGIC - **Columns:** Cast fields like **order_date** (`TIMESTAMP`), **quantity** (`INT`), **unit_price**, **discount_amount**, **total_amount** (`DOUBLE`), **ship_date** (`DATE`).
# MAGIC - **Constraints:**
# MAGIC   - `valid_order_id`: **order_id** IS NOT NULL, `ON VIOLATION FAIL UPDATE`
# MAGIC   - `valid_sku`: **sku** IS NOT NULL, `ON VIOLATION DROP ROW`
# MAGIC   - `positive_quantity`: **quantity** >= 1, `ON VIOLATION DROP ROW`
# MAGIC   - `positive_unit_price`: **unit_price** > 0 (WARN)
# MAGIC   - `valid_total_amount`: **total_amount** >= 0 (WARN)
# MAGIC
# MAGIC **To Do:**
# MAGIC Using these requirements, write the full code for the Silver table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### CODE ANSWER - Silver Table With Expectations and Liquid Clustering Enabled
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- STEP 1: Silver clean orders
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE lab_2_silver.orders_clean
# MAGIC (
# MAGIC   -- FAIL: order_id is the primary key
# MAGIC   CONSTRAINT valid_order_id
# MAGIC     EXPECT (order_id IS NOT NULL)
# MAGIC     ON VIOLATION FAIL UPDATE,
# MAGIC
# MAGIC   -- DROP: rows without a SKU are unusable for analytics
# MAGIC   CONSTRAINT valid_sku
# MAGIC     EXPECT (sku IS NOT NULL)
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC
# MAGIC   -- DROP: quantity less than 1 is not a valid sale
# MAGIC   CONSTRAINT positive_quantity
# MAGIC     EXPECT (quantity >= 1)
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC
# MAGIC   -- WARN: flags pricing anomalies; row passes through
# MAGIC   CONSTRAINT positive_unit_price
# MAGIC     EXPECT (unit_price > 0),
# MAGIC
# MAGIC   -- WARN: flags negative totals; row passes through
# MAGIC   CONSTRAINT valid_total_amount
# MAGIC     EXPECT (total_amount >= 0)
# MAGIC )
# MAGIC COMMENT "Silver clean orders - type-cast, quality-validated, liquid-clustered."
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   TRY_CAST(order_date      AS TIMESTAMP) AS order_date,
# MAGIC   customer_id,
# MAGIC   sku,
# MAGIC   model,
# MAGIC   category,
# MAGIC   order_type,
# MAGIC   channel,
# MAGIC   TRY_CAST(quantity        AS INT)       AS quantity,
# MAGIC   TRY_CAST(unit_price      AS DOUBLE)    AS unit_price,
# MAGIC   discount_code,
# MAGIC   TRY_CAST(discount_amount AS DOUBLE)    AS discount_amount,
# MAGIC   TRY_CAST(total_amount    AS DOUBLE)    AS total_amount,
# MAGIC   payment_method,
# MAGIC   order_status,
# MAGIC   city,
# MAGIC   region,
# MAGIC   TRY_CAST(ship_date       AS DATE)      AS ship_date,
# MAGIC   browser,
# MAGIC   os,
# MAGIC   session_id,
# MAGIC   app_version,
# MAGIC   device_model,
# MAGIC   source_file,
# MAGIC   ingestion_time
# MAGIC FROM STREAM lab_1_bronze.combined_orders_raw;
# MAGIC </code></pre>
# MAGIC
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
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Ingesting Product Catalog
# MAGIC
# MAGIC Create a **materialized view** in the Silver layer to hold the static product catalog reference.
# MAGIC
# MAGIC **Requirements:**
# MAGIC
# MAGIC - Name the view as **lab_2_silver.product_catalog_ref**.   
# MAGIC - Read the `product_catalog.csv` file using `read_files` with:
# MAGIC   - `format => 'csv'`  
# MAGIC   - `header => true`  
# MAGIC - Select and type-cast columns:
# MAGIC   - **sku**  
# MAGIC   - **brand**  
# MAGIC   - **category** as **catalog_category**  
# MAGIC   - **list_price** cast to `DOUBLE` as **list_price**  
# MAGIC   - **is_active** cast to `BOOLEAN` as **is_active**  
# MAGIC - Add a table comment indicating it is a product catalog reference with brand, category, and list price per SKU.
# MAGIC
# MAGIC **To Do:**
# MAGIC Using these requirements, write the full statement/code for the view.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### CODE ANSWER - Creating Product Catalog Reference as Materialized View
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- STEP 2: Product catalog as a materialized view (static reference)
# MAGIC -- Used as the static side of the stream-static join below.
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW lab_2_silver.product_catalog_ref
# MAGIC COMMENT "Product catalog reference - brand, category, and list price per SKU."
# MAGIC AS
# MAGIC SELECT
# MAGIC   sku,
# MAGIC   brand,
# MAGIC   category                    AS catalog_category,
# MAGIC   CAST(list_price AS DOUBLE)  AS list_price,
# MAGIC   CAST(is_active  AS BOOLEAN) AS is_active
# MAGIC FROM read_files(
# MAGIC   '${product_catalog_source}',
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC );
# MAGIC </code></pre>
# MAGIC
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
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E5. Join Orders Clean Table with Product Catalog
# MAGIC
# MAGIC We will enrich the **orders_clean** table with product catalog attributes using a stream-static join.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Use a stream-static **LEFT JOIN** to retain all orders, even if the **SKU** is missing in the catalog.
# MAGIC - Add product attributes from the catalog:
# MAGIC   - **brand**
# MAGIC   - **catalog_category**
# MAGIC   - **list_price**
# MAGIC   - **is_active** (as **sku_is_active**)
# MAGIC - Calculate **price_vs_catalog** as the difference between the order's **unit_price** and the catalog's **list_price**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- STEP 3: Enriched orders — stream-static join
# MAGIC -- Streaming side : orders_clean
# MAGIC -- Static side    : product_catalog_ref (re-read on every trigger)
# MAGIC -- LEFT JOIN retains all orders even if the SKU is not in catalog.
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE lab_2_silver.orders_enriched
# MAGIC COMMENT "Enriched Silver orders - stream-static join adds brand, catalog category, and list price."
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   DATE(o.order_date)                     AS order_date,
# MAGIC   o.customer_id,
# MAGIC   o.sku,
# MAGIC   o.model,
# MAGIC   o.order_type,
# MAGIC   o.channel,
# MAGIC   o.quantity,
# MAGIC   o.unit_price,
# MAGIC   o.discount_code,
# MAGIC   o.discount_amount,
# MAGIC   o.total_amount,
# MAGIC   o.payment_method,
# MAGIC   o.order_status,
# MAGIC   o.city,
# MAGIC   o.region,
# MAGIC   o.ship_date,
# MAGIC   o.source_file,
# MAGIC   p.brand,
# MAGIC   p.catalog_category,
# MAGIC   p.list_price,
# MAGIC   ROUND(o.unit_price - p.list_price, 2)  AS price_vs_catalog,
# MAGIC   p.is_active                             AS sku_is_active
# MAGIC FROM STREAM lab_2_silver.orders_clean AS o
# MAGIC LEFT JOIN lab_2_silver.product_catalog_ref AS p
# MAGIC   ON o.sku = p.sku;
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
# MAGIC ### E6. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it completes successfully.
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines Editor, review your pipeline run:
# MAGIC
# MAGIC    - Confirm that **0** new records were ingested into **combined_orders_raw**.
# MAGIC
# MAGIC    - **orders_clean** should show **192** records, with 2 expectations met and 3 unmet.
# MAGIC
# MAGIC    - Hover over **orders_clean** in the pipeline graph to see **8** records dropped and **8** records with warnings. Click on "Expectations" for details:
# MAGIC
# MAGIC      - **8** records dropped due to the `positive_quantity` constraint.
# MAGIC      - **10** records triggered warnings for the `positive_unit_price` constraint.
# MAGIC      -  **2** records failed both constraints, so the graph shows **8** failed for `positive_unit_price`, but actually **10** failed.
# MAGIC
# MAGIC    - **product_catalog_ref** should have **50** records.
# MAGIC
# MAGIC    - **orders_enriched** should have joined columns from **product_catalog_ref** and also show **192** records.
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, check that your volumes and configuration parameters are set correctly.
# MAGIC
# MAGIC #### Checkpoint - Silver Layer
# MAGIC
# MAGIC <img src="./Includes/images/lab/lab_checkpoint_1.png" alt="Silver Layer Checkpoint" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### E7. Confirming Transformation of Records
# MAGIC
# MAGIC Let's verify that our Silver layer transformations are working correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders_clean'    AS table_name, COUNT(*) AS rows FROM lab_2_silver.orders_clean
# MAGIC UNION ALL
# MAGIC SELECT 'orders_enriched' AS table_name, COUNT(*) AS rows FROM lab_2_silver.orders_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify enrichment - brand and list_price populated for known SKUs
# MAGIC SELECT
# MAGIC   order_id, sku, brand, catalog_category,
# MAGIC   unit_price, list_price, price_vs_catalog, sku_is_active
# MAGIC FROM lab_2_silver.orders_enriched
# MAGIC WHERE brand IS NOT NULL
# MAGIC ORDER BY price_vs_catalog DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint — Silver Layer
# MAGIC | Check | Expected |
# MAGIC |-------|----------|
# MAGIC | **orders_clean**  | Bronze rows minus any DROP violations |
# MAGIC | **orders_enriched**  | Equal to orders_clean — LEFT JOIN retains all rows |
# MAGIC | **Expectations tab (pipeline UI)** | 5 constraints shown |
# MAGIC | **Enrichment** | brand, catalog_category, list_price populated for known SKUs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Gold Layer — Business Analytics Materialized Views
# MAGIC
# MAGIC The Gold layer provides business-ready analytics tables optimized for reporting and dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create the Gold Analytics SQL File
# MAGIC
# MAGIC 1. On your **ecommerce_pipeline** folder select the kebab menu and select **Create File**
# MAGIC
# MAGIC 2. Select the language as **SQL**
# MAGIC
# MAGIC 3. Name the file `gold_analytics.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Daily Revenue by Category Gold View
# MAGIC
# MAGIC Create a **Gold-layer materialized view** named **weekly_revenue_by_category** that provides a daily revenue snapshot by category for business stakeholders.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Aggregate sales at the daily level across key business dimensions: date, product category, brand, sales channel, and region.
# MAGIC
# MAGIC - Expose core commercial KPIs:
# MAGIC   - **total_orders**: Count of unique orders per group
# MAGIC   - **units_sold**: Total quantity sold
# MAGIC   - **gross_revenue**: Sum of total_amount
# MAGIC   - **total_discounts**: Sum of discount_amount
# MAGIC   - **net_revenue**: Gross revenue minus total discounts
# MAGIC   - **avg_order_value**: Average order value per group
# MAGIC   - **discounted_orders**: Count of orders with a discount code
# MAGIC   - **last_refreshed**: Timestamp of materialized view refresh (`current_timestamp()`)
# MAGIC
# MAGIC **To Do:**
# MAGIC Using these requirements, write the full code for the Gold analytics view.

# COMMAND ----------

## Create the Gold layer analytics materialized view
## Follow the requirements specified above

%sql
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### CODE ANSWER - Gold Analytics View
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- GOLD VIEW: Daily Revenue by Category
# MAGIC -- Audience: Executive dashboards, category managers
# MAGIC -- ----------------------------------------------------------------
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW lab_3_gold.weekly_revenue_by_category
# MAGIC COMMENT "Gold MV: Daily gross revenue, units, and order count by category and channel."
# MAGIC AS
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     catalog_category                                        AS category,
# MAGIC     brand,
# MAGIC     order_type                                              AS channel,
# MAGIC     region,
# MAGIC     COUNT(DISTINCT order_id)                                AS total_orders,
# MAGIC     SUM(quantity)                                           AS units_sold,
# MAGIC     ROUND(SUM(total_amount), 2)                             AS gross_revenue,
# MAGIC     ROUND(SUM(discount_amount), 2)                         AS total_discounts,
# MAGIC     ROUND(SUM(total_amount) - SUM(discount_amount), 2)     AS net_revenue,
# MAGIC     ROUND(AVG(total_amount), 2)                             AS avg_order_value,
# MAGIC     COUNT(CASE WHEN discount_code IS NOT NULL THEN 1 END)  AS discounted_orders,
# MAGIC     current_timestamp()                                     AS last_refreshed
# MAGIC
# MAGIC FROM lab_2_silver.orders_enriched
# MAGIC WHERE order_date IS NOT NULL AND catalog_category  IS NOT NULL
# MAGIC GROUP BY
# MAGIC order_date,catalog_category,brand,order_type,region;
# MAGIC </code></pre>
# MAGIC
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
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. Run the Full Pipeline and Validate Gold Layer
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it completes successfully.
# MAGIC 2. In the Lakeflow Pipelines Editor, review your pipeline run:
# MAGIC
# MAGIC    - Confirm that **0** new records were ingested into any tables of Bronze and Silver layers.
# MAGIC    - Materialized view **weekly_revenue_by_category** should have **192** records
# MAGIC
# MAGIC #### Checkpoint - Gold Layer
# MAGIC
# MAGIC <img src="./Includes/images/lab/lab_checkpoint_2.png" alt="Gold Layer Checkpoint" width="1200">

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top revenue categories
# MAGIC SELECT
# MAGIC   category,
# MAGIC   SUM(total_orders)            AS total_orders,
# MAGIC   SUM(units_sold)              AS units_sold,
# MAGIC   ROUND(SUM(gross_revenue), 2) AS gross_revenue,
# MAGIC   ROUND(SUM(net_revenue), 2)   AS net_revenue
# MAGIC FROM lab_3_gold.weekly_revenue_by_category
# MAGIC GROUP BY category
# MAGIC ORDER BY gross_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Incremental Processing
# MAGIC
# MAGIC Now we'll simulate incremental data processing by landing additional files and observing how our pipeline handles new data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Land Additional Files
# MAGIC
# MAGIC Run the function below to automatically land new files into the source locations for both web and app orders.

# COMMAND ----------

copy_second_file()

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Listing Available Files
# MAGIC
# MAGIC Let's verify that the new files have been added to our source volumes.

# COMMAND ----------

# List files in web_orders volume
display(spark.sql(f"LIST '{my_vol_path}/web_orders'"))

# COMMAND ----------

# List files in app_orders volume
display(spark.sql(f"LIST '{my_vol_path}/app_orders'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it completes successfully.
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines Editor, review your pipeline run:
# MAGIC
# MAGIC    - Confirm that **200** new records were ingested into **combined_orders_raw**.
# MAGIC
# MAGIC    - **orders_clean** should have **191** records, with 3 expectations met and 2 unmet.
# MAGIC
# MAGIC    - Hover over **orders_clean** in the pipeline graph to see **9** records dropped and **7** records with warnings. Click on "Expectations" for details:
# MAGIC
# MAGIC      - A total of **9** records were dropped: **8** due to the `positive_quantity` constraint and **1** due to the `valid_sku` constraint.
# MAGIC      - **7** records triggered warnings for the `positive_unit_price` constraint.
# MAGIC
# MAGIC    - **product_catalog_ref** should have **50** records.
# MAGIC
# MAGIC    - **orders_enriched** should have joined columns from **product_catalog_ref** and also show **191** records.
# MAGIC
# MAGIC    - **weekly_revenue_by_category** should now show a total of **380** records.
# MAGIC
# MAGIC #### Checkpoint - Incremental Processing
# MAGIC
# MAGIC <img src="./Includes/images/lab/lab_checkpoint_3.png" alt="Incremental Processing Checkpoint" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### G4. Validating the Pipeline
# MAGIC
# MAGIC Let's validate that our incremental processing is working correctly by examining the data at each layer.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Verify Bronze Cumulative Totals
# MAGIC
# MAGIC After both runs, the Bronze layer should contain all records from Run 1 and Run 2 combined.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, order_type, COUNT(*) AS rows
# MAGIC FROM lab_1_bronze.combined_orders_raw
# MAGIC GROUP BY source_file, order_type
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Verify Data Quality Impact
# MAGIC
# MAGIC In the first run, **8** records failed. In the second run, **9** records failed. The difference between Bronze and Silver layers is the total dropped records: `8 + 9 = 17`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, order_id, unit_price, sku, quantity FROM lab_1_bronze.combined_orders_raw
# MAGIC MINUS
# MAGIC SELECT source_file, order_id, unit_price, sku, quantity FROM lab_2_silver.orders_clean
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Data Quality Pass Rate Analysis
# MAGIC
# MAGIC Calculate the overall data quality pass rate from Bronze raw to Silver clean.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM lab_1_bronze.combined_orders_raw)   AS bronze_raw_rows,
# MAGIC   (SELECT COUNT(*) FROM lab_2_silver.orders_clean) AS silver_clean_rows,
# MAGIC   ROUND(
# MAGIC     (SELECT COUNT(*) FROM lab_2_silver.orders_clean) * 100.0 /
# MAGIC     NULLIF((SELECT COUNT(*) FROM lab_1_bronze.combined_orders_raw), 0),
# MAGIC   2) AS pass_rate_pct;

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>