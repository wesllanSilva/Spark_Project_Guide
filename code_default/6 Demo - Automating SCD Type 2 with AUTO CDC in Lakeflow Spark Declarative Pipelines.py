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
# MAGIC # Demo - Automating SCD Type 2 with AUTO CDC in Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC ## Overview 
# MAGIC
# MAGIC This demonstration showcases how to implement automated Change Data Capture (CDC) for Slowly Changing Dimension (SCD) Type 2 patterns using Lakeflow Spark Declarative Pipelines. 
# MAGIC
# MAGIC Learners will learn to build an end-to-end streaming pipeline that automatically processes customer data changes (inserts, updates, and deletes) while maintaining complete historical records. The demo uses real-world retail customer data from Databricks Marketplace and demonstrates how AUTO CDC INTO simplifies complex CDC operations that traditionally required manual coding. 
# MAGIC
# MAGIC Learners will create a multi-layered architecture (Bronze → Silver → Gold) with automated data quality checks, incremental processing, and materialized views for both current and historical customer analytics.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC
# MAGIC - **Configure and implement AUTO CDC INTO** for automated Change Data Capture processing in Lakeflow Spark Declarative Pipelines with SCD Type 2 patterns
# MAGIC
# MAGIC - **Design multi-layered data architectures** using Bronze (raw ingestion), Silver (CDC processing), and Gold (analytics-ready) layers with streaming tables and materialized views
# MAGIC
# MAGIC - **Apply comprehensive data quality constraints** using pipeline expectations with WARN, DROP, and FAIL actions to ensure data integrity across CDC operations
# MAGIC
# MAGIC - **Process streaming JSON files incrementally** using Auto Loader and manage customer lifecycle events (INSERT, UPDATE, DELETE) while preserving historical versions
# MAGIC
# MAGIC - **Create analytics ready materialized views** that automatically maintain current customer states and track removed customers for downstream reporting and business intelligence
# MAGIC
# MAGIC
# MAGIC ### CDC Pipeline Overview
# MAGIC
# MAGIC In this demonstration, you'll build a Lakeflow Spark Declarative Pipeline that performs the full medallion flow from raw ingestion to curated analytics:
# MAGIC
# MAGIC 1. **Ingest JSON source files** from cloud storage into a **Bronze raw streaming table** using Auto Loader.  
# MAGIC 2. **Apply data quality expectations** to the raw table to produce a **clean Bronze table** ready for downstream processing.  
# MAGIC 3. **Implement CDC SCD Type 2** logic with `AUTO CDC INTO` to maintain an up-to-date **Silver customer table** with full change history.  
# MAGIC 4. **Create Gold materialized views** for **current customers** and **deleted customers**, enabling simplified analytics and reporting.
# MAGIC
# MAGIC ![CDC Pipeline Overview](./Includes/images/cdc_lecture/cdc_pipeline_overview.png)

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
# MAGIC ## A. REQUIRED - Classroom Setup
# MAGIC Follow the cells below to set up the notebook for your specific Workspace.

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
# MAGIC 1. Run the cell below to initialize your environment. This setup step does the following:
# MAGIC
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks provided Vocareum workspace
# MAGIC
# MAGIC     - Creates a catalog named **labuser_user_name** when running outside of a Databricks Vocareum provided workspace  
# MAGIC       - Uses **labuser_idnumber** when running in Vocareum
# MAGIC
# MAGIC     - Create three schemas in your specified catalog:  
# MAGIC       - **sdp_cdc_1_bronze**
# MAGIC       - **sdp_cdc_2_silver**
# MAGIC       - **sdp_cdc_3_gold**  
# MAGIC     - Creates a volume named **YOUR_LABUSER_CATALOG.sdp_cdc_1_bronze.customer_source_files** and adds a single JSON file into the volume.
# MAGIC     - Checks your specified compute
# MAGIC
# MAGIC     This ensures that all schemas, tables and objects are created in your catalog.

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
# MAGIC This function is defined in the notebook: `./Includes/Classroom-Setup-auto-cdc-demo`
# MAGIC
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-auto-cdc-demo

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Confirm the variable `my_vol_path` created in the classroom setup points to **your-catalog.sdp_cdc_1_bronze.customer_source_files** volume path.
# MAGIC
# MAGIC     **Example:** `/Volumes/YOUR_LABUSER_CATALOG/sdp_cdc_1_bronze/customer_source_files`

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Customer Data Source File(s) in Your Volume

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to programmatically view the files in your `/Volumes/your-catalog/sdp_cdc_1_bronze/customer_source_files/` volume **path**.
# MAGIC   
# MAGIC     Confirm you only see one file for customers (**00.json**). 

# COMMAND ----------

spark.sql(f'LIST "{my_vol_path}"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the query below to explore the customers **00.json** file located in your **customer_source_files** volume within **your-catalog.sdp_cdc_1_bronze** schema. 
# MAGIC
# MAGIC     Note the following:
# MAGIC
# MAGIC       a. The file contains **939 customers** (remember this number).
# MAGIC
# MAGIC       b. It includes general customer information such as **email**, **name**, and **address**.
# MAGIC
# MAGIC       c. The **timestamp** column specifies the logical order (sequence) of customer events in the source data as a UNIX timestamp.
# MAGIC
# MAGIC       d. The **operation** column indicates whether the entry is for a new customer, a deletion, or an update.
# MAGIC
# MAGIC       - **NOTE:** Since this is the first JSON file, all rows will be considered new customers.
# MAGIC
# MAGIC   **NOTE:** The `my_vol_path` SQL variable was created for you in the classroom setup.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   my_vol_path || '/00.json',  -- my_vol_path is the path to your source JSON files in the volume your-catalog.sdp_cdc_1_bronze.customer_source_files
# MAGIC   format => "JSON"
# MAGIC )
# MAGIC ORDER BY operation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question
# MAGIC How can we ingest new raw JSON source files with customer updates into our pipeline to update a **silver table** when inserts, updates, or deletes occur, while also maintaining historical records (SCD Type 2)?
# MAGIC
# MAGIC ### Answer
# MAGIC Use `AUTO CDC INTO` with Spark Declarative Pipelines!
# MAGIC
# MAGIC View the Databricks documentation [Processing a change data feed: Keep only the latest data vs. keep historical versions of data](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture#processing-a-change-data-feed-keep-only-the-latest-data-vs-keep-historical-versions-of-data) for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Building the CDC Spark Declarative Pipeline with `AUTO CDC INTO`

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Enable the Lakeflow Pipelines Editor
# MAGIC
# MAGIC Complete the following steps to confirm or enable the **Lakeflow Pipelines Editor**:
# MAGIC
# MAGIC 1. In the top-right corner of the workspace, select your **account icon** ![Account Icon](./Includes/images/account_icon.png) (*Your letter will differ*).  
# MAGIC
# MAGIC 2. Right-click **Settings** and choose **Open link in new tab**.  
# MAGIC 3. In the left sidebar, select **Developer** under **User**.  
# MAGIC 4. In the **Experimental features** section, locate **Lakeflow Pipelines Editor** and toggle it **on**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Create a Lakeflow Spark Declarative Pipeline using the Lakeflow Pipelines Editor
# MAGIC Complete the following steps to create your Spark Declarative Pipeline:
# MAGIC
# MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open link in New Tab**.  
# MAGIC
# MAGIC 2. In the new tab, select **Create → ETL Pipeline**.  
# MAGIC
# MAGIC    **NOTE:** If prompted to **Try the new Lakeflow Pipelines Editor**, choose **Enable Lakeflow Pipelines Editor**. This appears only if you did not complete the previous step.  
# MAGIC
# MAGIC 3. At the top, complete the following:
# MAGIC    - Name your pipeline `demo_auto_cdc_pipeline_yourname`
# MAGIC    - Select your default **catalog** and **schema**:  
# MAGIC         - **Catalog:** The catalog specified in the setup script (example: `labuser_xxx`) 
# MAGIC         - **Schema:** **sdp_cdc_1_bronze**  
# MAGIC       **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. Rename the `transformations` folder to `my_pipeline`.
# MAGIC
# MAGIC 5. Rename the `my_transformations.sql` file to `cdc_pipeline.sql`.
# MAGIC
# MAGIC 6. Leave the **Lakeflow Pipelines Editor** page open.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC ![Create SDP Checkpoint](./Includes/images/cdc_lecture/cdc_create_initial_pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Configure Pipeline Parameter
# MAGIC
# MAGIC 1. Run the cell below to get the path to your raw data source JSON files in your **customer_source_files** volume within your **labuser.sdp_cdc_1_bronze** schema.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the path above and add it as a configuration parameter in your **Spark Declarative Pipeline**. This will reference your **sdp_cdc_1_bronze.customer_source_files** volume that contains your raw JSON file(s) that you explored earlier:
# MAGIC
# MAGIC    a. Select **Settings** in your pipeline tab.
# MAGIC
# MAGIC    b. Under **Configuration** select **Add configuration**.
# MAGIC
# MAGIC    c. For **Key** add `source`
# MAGIC
# MAGIC    d. For **Value** add the `path to your volume` from above.
# MAGIC
# MAGIC    e. Select **Save**.
# MAGIC
# MAGIC    **NOTE:** For more information on configuration parameters, check out the Databricks documentation [Use parameters with Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)
# MAGIC
# MAGIC #### Checkpoint (your path will vary)
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_config_parameter_value.png" alt="Config Parameter Checkpoint" width="600">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Create Bronze Layer with Auto Loader for Incremental Ingestion
# MAGIC
# MAGIC 1. In this step, you'll define the **Bronze raw** layer of your **Spark Declarative Pipeline** to create the table: **sdp_cdc_1_bronze.customers_bronze_raw_demo**.  
# MAGIC
# MAGIC    This layer will incrementally ingest raw **JSON** files from cloud storage into a **streaming table** using **Auto Loader**.  
# MAGIC    
# MAGIC    Copy the SQL code below and paste it into your `cdc_pipeline.sql` file. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ------------------------------------------------------
# MAGIC -- STEP 1: JSON -> Bronze Ingestion
# MAGIC ------------------------------------------------------
# MAGIC -- Ingest the JSON files from cloud storage into a streaming table using Auto Loader
# MAGIC CREATE OR REFRESH STREAMING TABLE sdp_cdc_1_bronze.customers_bronze_raw_demo
# MAGIC   COMMENT "Raw data from customers CDC feed"
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() processing_time,  -- Obtain the ingestion processing time for the rows
# MAGIC   _metadata.file_name as source_file    -- Obtain the file name of the record
# MAGIC FROM STREAM read_files(
# MAGIC   "${source}",  -- References your SDP parameter that points to your demonstration volume containing your JSON data source files
# MAGIC   format => "json");
# MAGIC <!-------------------END SOLUTION CODE------------------->
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
# MAGIC **Review the code:**
# MAGIC
# MAGIC - `CREATE OR REFRESH STREAMING TABLE` - Creates a managed streaming table that automatically updates as new data arrives.  
# MAGIC - `COMMENT` - Describes the purpose of the table for easier discovery and documentation.  
# MAGIC - `current_timestamp()` - Captures the ingestion timestamp for data lineage and auditing.  
# MAGIC - `_metadata.file_name` - Adds the source file name for traceability and debugging.  
# MAGIC - `FROM STREAM read_files()` - Uses Auto Loader to incrementally read new JSON files from the specified (triggered or continuous)  `${source}` path you added in the previous step. 

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Select **Dry Run** to verify that your pipeline and SQL code are configured correctly.  
# MAGIC    
# MAGIC    This will validate your settings without actually running the full pipeline.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC > **TROUBLESHOOTING:** If the dry run did not work, make sure you configured your `source` pipeline parameter correctly in the **Configure Pipeline Parameter** step above.
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_dry_run_ingest.png" alt="JSON Ingest" width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC ### C5. Apply Data Quality Rules to the Bronze Table
# MAGIC
# MAGIC With the raw JSON CDC data now set up to stream into **sdp_cdc_1_bronze.customers_bronze_raw_demo**, we'll add another **bronze** quality layer before moving to **Silver**.  
# MAGIC
# MAGIC You'll create an intermediate table **sdp_cdc_1_bronze.customers_bronze_clean** and apply **data quality constraints** to filter and flag invalid records while preserving expected `NULL` values for `DELETE` operations.  
# MAGIC
# MAGIC In this step, you'll:  
# MAGIC - Apply **multiple constraints** to check key fields.  
# MAGIC - Use **WARN**, **DROP**, and **FAIL** actions to define how violations are handled.  
# MAGIC - Add **conditional logic** and **regex validation** inside constraints.  
# MAGIC
# MAGIC For details, see [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### C5.1 - About the Data Source
# MAGIC
# MAGIC    - The CDC feed contains `INSERT`, `UPDATE`, and `DELETE` operations for customer records. 
# MAGIC
# MAGIC    - `INSERT` and `UPDATE` events include valid values for all key columns.  
# MAGIC
# MAGIC    - `DELETE` events have `NULL` values for all non-key fields such as **name**, **email**, **address**, **city**, and **state**.  
# MAGIC
# MAGIC    **Example of a Dropped Record:**
# MAGIC
# MAGIC    | **address** | **city** | **customer_id** | **email** | **name** | **operation** | **state** |
# MAGIC    |--------------|-----------|------------------|-------------|------------|----------------|-------------|
# MAGIC    | `NULL` | `NULL` | `23617` | `NULL` | `NULL` | `DELETE` | `NULL` |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### C5.2 - Create the Clean Bronze Table Using Pipeline Data Quality Expectations
# MAGIC
# MAGIC 1. Now that we have a general understanding of the data, let's define the following **data quality constraints** that enforce those rules in a secondary Bronze table named **sdp_cdc_1_bronze.customers_bronze_clean_demo** before the data moves to the Silver stage.
# MAGIC
# MAGIC Below is a table of the data quality constraints:
# MAGIC
# MAGIC   | **Comment ID** | **Constraint** | **Rule / Expectation** | **Violation Action** | **Purpose** |
# MAGIC   |:------:|----------------|------------------------|----------------------|--------------|
# MAGIC   | **A** | **`valid_id`** | **customer_id** `IS NOT NULL` | **FAIL** | Fails the transaction if a record has a missing **customer_id**. *Will required manual intervention to fix it.* |
# MAGIC   | **B** | **`valid_operation`** | **operation** `IS NOT NULL` | **DROP ROW** | Drops any record missing an **operation** type. |
# MAGIC   | **C** | **`valid_name`** | **name** `IS NOT NULL OR operation = "DELETE"` | **WARN** (default) | Flags missing **name** values unless it's a **DELETE** operation. |
# MAGIC   | **D** | **`valid_address`** | All address fields (**address**, **city**, **state**, **zip_code**) must be `non-null` unless it's a **DELETE** | **WARN** (default) | Ensures complete address information for **INSERT** and **UPDATE** events. |
# MAGIC   | **E** | **`valid_email`** | **email** must match a valid format unless it's a **DELETE** | **DROP ROW** | Uses regex to validate the **email** format and drops invalid records. |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the SQL code below and append it into your `cdc_pipeline.sql` file.  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC  ------------------------------------------------------
# MAGIC -- STEP 2: Bronze Raw -> Bronze Clean
# MAGIC ------------------------------------------------------
# MAGIC CREATE STREAMING TABLE sdp_cdc_1_bronze.customers_bronze_clean_demo
# MAGIC   (
# MAGIC
# MAGIC     -- A. Require a valid customer_id, fail the transaction if missing
# MAGIC     CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) 
# MAGIC       ON VIOLATION FAIL UPDATE,
# MAGIC
# MAGIC     -- B. Require a valid operation, drop any record with NULL operation
# MAGIC     CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) 
# MAGIC       ON VIOLATION DROP ROW,
# MAGIC
# MAGIC     -- C. Require name to be present unless the operation is DELETE
# MAGIC     CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE"),
# MAGIC
# MAGIC     -- D. Require full address fields unless operation is DELETE
# MAGIC     CONSTRAINT valid_address EXPECT (
# MAGIC       (address IS NOT NULL 
# MAGIC        AND city IS NOT NULL 
# MAGIC        AND state IS NOT NULL 
# MAGIC        AND zip_code IS NOT NULL)
# MAGIC        OR operation = "DELETE"),
# MAGIC
# MAGIC     -- E. Require valid email format (regex), skip check for DELETE, drop invalid rows
# MAGIC     CONSTRAINT valid_email EXPECT (
# MAGIC       rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') 
# MAGIC       OR operation = "DELETE") 
# MAGIC       ON VIOLATION DROP ROW
# MAGIC   )
# MAGIC   COMMENT "Clean raw bronze data and apply quality constraints"
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime -- Convert unix timestamp
# MAGIC FROM STREAM sdp_cdc_1_bronze.customers_bronze_raw_demo;
# MAGIC <!-------------------END SOLUTION CODE------------------->
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
# MAGIC **Review the code:**
# MAGIC
# MAGIC   - This query creates the **sdp_cdc_1_bronze.customers_bronze_clean_demo** table from the **sdp_cdc_1_bronze.customers_bronze_raw_demo** table.  
# MAGIC
# MAGIC   - It applies the **data quality constraints** defined above to ensure only valid CDC records progress to the next stage before reaching Silver.
# MAGIC
# MAGIC   - Also **Converts UNIX timestamp to readable timestamp** using `CAST(from_unixtime(timestamp) AS timestamp)` as `timestamp_datetime`.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Select **Dry Run** to verify that your pipeline and SQL code are configured correctly.  
# MAGIC    
# MAGIC    This will validate your settings without actually running the full pipeline.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_dry_run_bronze_clean.png" alt="Bronze Clean" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### C6.Implement `AUTO CDC INTO` for the Silver Table
# MAGIC
# MAGIC Now that the **Bronze** layer (**sdp_cdc_1_bronze.customers_bronze_raw_demo -> sdp_cdc_1_bronze.customers_bronze_clean_demo**) data is clean, we'll create a **Silver** table that automatically applies inserts, updates, and deletes as new changes arrive.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### The problem, manual CDC logic is brittle
# MAGIC In many pipelines, Change Data Capture is implemented manually.
# MAGIC
# MAGIC That usually means writing and maintaining a `MERGE INTO` statement that must correctly handle:
# MAGIC
# MAGIC - Inserts, updates, and deletes  
# MAGIC - Duplicate events and replays  
# MAGIC - Late arriving changes  
# MAGIC - Correct keys and match conditions  
# MAGIC
# MAGIC Even when the SQL looks simple, the logic becomes easy to break as requirements change.
# MAGIC
# MAGIC #### Traditional approach, manual `MERGE INTO` example
# MAGIC
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO target_table AS t
# MAGIC USING source_stream AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND s.operation = 'UPDATE'
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND s.operation = 'DELETE'
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND s.operation = 'INSERT'
# MAGIC   THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC **Why this is a problem**
# MAGIC
# MAGIC - You must maintain the merge logic yourself  
# MAGIC - You must trust incoming operation values and handle edge cases  
# MAGIC - As schemas evolve and rules change, the merge often grows into a large block of SQL that is difficult to test and easy to get wrong  
# MAGIC
# MAGIC Next, we will replace this pattern with `AUTO CDC INTO`, which allows the pipeline to apply CDC changes automatically with far less code.

# COMMAND ----------

# MAGIC %md
# MAGIC #### SOLUTION
# MAGIC
# MAGIC `AUTO CDC INTO` in **Spark Declarative Pipelines** simplifies this process by automatically managing inserts, updates, and deletes in streaming data, reducing boilerplate code and improving reliability.
# MAGIC
# MAGIC `AUTO CDC INTO` provides the following guarantees and requirements:
# MAGIC
# MAGIC - Performs incremental and streaming ingestion of CDC data  
# MAGIC - Lets you define one or more primary key fields for identifying records  
# MAGIC - Assumes rows contain inserts and updates by default  
# MAGIC - **Optionally** applies deletes when defined  
# MAGIC - Orders late-arriving records using a **sequencing key**  
# MAGIC - Allows excluding columns with the **`EXCEPT`** keyword  
# MAGIC - Defaults to **SCD Type 1**, though we will use **SCD Type 2** in this demonstration  
# MAGIC
# MAGIC We will complete this in two steps:
# MAGIC
# MAGIC 1. **Create an empty Silver target table** to store historical customer data using the SCD Type 2 pattern  
# MAGIC 2. **Use `AUTO CDC INTO`** to automatically apply inserts, updates, and deletes as new changes arrive  
# MAGIC
# MAGIC **NOTE:** For more details, see the official documentation: [**The AUTO CDC APIs: Simplify change data capture with Lakeflow Spark Declarative Pipelines**](https://docs.databricks.com/aws/en/ldp/cdc)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### C6.1 - Create an Empty Silver Target Table
# MAGIC Before we can use AUTO CDC INTO, we need to create the target table that will store our customer data with historical tracking.
# MAGIC
# MAGIC 1. The SQL command below creates or refreshes a **streaming Silver table** named **sdp_cdc_2_silver.customers_silver_scd2_demo**.  
# MAGIC
# MAGIC    This table will continuously receive incremental updates from the **customers_bronze_clean_demo** table as new change data arrives.  
# MAGIC
# MAGIC    Copy and paste the code below into your `cdc_pipeline.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC -- STEP 3: Processing CDC Data with AUTO CDC INTO
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- a. Create the streaming target table if it's not already created
# MAGIC CREATE OR REFRESH STREAMING TABLE sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC   COMMENT 'SCD Type 2 Historical Customer Data';
# MAGIC <!-------------------END SOLUTION CODE------------------->
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
# MAGIC **Review the code**
# MAGIC
# MAGIC - `CREATE OR REFRESH STREAMING TABLE` - Creates the table **sdp_cdc_2_silver.customers_silver_scd2_demo** if it doesn't exist, or refreshes it if it does, ensuring it's ready for streaming updates.
# MAGIC
# MAGIC - `COMMENT` - Adds a description to document the table's purpose, identifying it as an **SCD Type 2** table used to track historical customer changes over time.
# MAGIC
# MAGIC Once the Silver target table exists, we can automatically propagate changes from the cleaned Bronze table using Lakeflow Spark Declarative Pipelines with the `AUTO CDC INTO` command.

# COMMAND ----------

# MAGIC %md
# MAGIC #### C6.2 - Perform SCD Type 2 with `AUTO CDC INTO`
# MAGIC
# MAGIC
# MAGIC 1. Before we run `AUTO CDC INTO`, let's take a closer look at the key columns in our cleaned Bronze table (**sdp_cdc_1_bronze.customers_bronze_clean_demo**). 
# MAGIC
# MAGIC     Understanding these columns helps explain how **Spark Declarative Pipelines** detects and applies changes across `INSERT`, `UPDATE`, and `DELETE` operations.
# MAGIC
# MAGIC     Below is a simplified preview of the data. We've included only the columns that are important for **Change Data Capture (CDC)**.
# MAGIC
# MAGIC | **customer_id** (*primary key*) | **name** | **email** | **operation** (*type of change*) | **timestamp_datetime** (*sequence column*) | **source_file** | ... |
# MAGIC |------------------|-----------|------------|----------------|------------------------|------------------|-----|
# MAGIC | 23056 | Brent Chavez | nelsonjoy@example.com | `NEW` | 2021-09-23T17:26:21.000+00:00 | 00.json | ... |
# MAGIC | 23057 | James Cruz | perkinsdeborah@example.net | `UPDATE` | 2021-09-23T18:21:45.000+00:00 | 00.json | ... |
# MAGIC | 23058 | Jennifer Christensen | jmccullough@example.net | `DELETE` | 2021-09-23T00:19:44.000+00:00 | 00.json | ... |
# MAGIC | ... | ... | ... | ... | ... | ... | ... |
# MAGIC
# MAGIC **Key Columns**
# MAGIC
# MAGIC - **customer_id**- Serves as the **primary key** used to identify each unique customer record. CDC logic depends on this column to determine which row should be `updated` or `deleted` in the target table.
# MAGIC
# MAGIC - **operation** - Indicates the type of change from the source system (`NEW`, `UPDATE`, or `DELETE`).  
# MAGIC   - `DELETE` operations remove the corresponding record from the target table.  
# MAGIC   - `NEW` and `UPDATE` operations are automatically processed and merged by `AUTO CDC` using the **customer_id** primary key column.
# MAGIC
# MAGIC - **timestamp_datetime** - Defines the **order of operations**. This ensures `updates` are applied in the correct sequence when multiple changes occur for the same record.
# MAGIC
# MAGIC - **source_file** - Helps trace which file each record came from, useful for debugging and validation during pipeline runs.
# MAGIC
# MAGIC Next, we'll use these columns to perform `AUTO CDC INTO`, allowing Spark Declarative Pipelines **to automatically manage incremental updates between Bronze and Silver**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. The `AUTO CDC INTO` command defines a **Spark Declarative Pipeline flow** that applies **SCD Type 2** Change Data Capture logic to keep the **Silver** table continuously up to date.  
# MAGIC
# MAGIC    It automatically manages how `INSERTS`, `UPDATES`, and `DELETES` are applied between the following:  
# MAGIC
# MAGIC    - `Source:` **sdp_cdc_1_bronze.customers_bronze_clean_demo** - The streaming Bronze table containing cleaned CDC records.  
# MAGIC
# MAGIC    - `Target:` **sdp_cdc_2_silver.customers_silver_scd2_demo** - The Silver table that stores the current and historical customer data.
# MAGIC
# MAGIC
# MAGIC     Copy and paste the code below into your `cdc_pipeline.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC -- b. Perform SCD Type 2 into the silver table
# MAGIC CREATE FLOW customers_scd_type_2_flow AS 
# MAGIC AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2_demo  -- Target: Where processed records are stored
# MAGIC FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean_demo   -- Source: Clean CDC records from Bronze layer
# MAGIC   KEYS (customer_id)                                       -- Primary key: Used to match records for updates/deletes
# MAGIC   APPLY AS DELETE WHEN operation = "DELETE"                -- Delete logic: Remove records marked as DELETE
# MAGIC   SEQUENCE BY timestamp_datetime                           -- Ordering: Ensures changes are applied in correct sequence
# MAGIC   COLUMNS * EXCEPT (timestamp, _rescued_data, operation)   -- Column selection: Include all except metadata fields
# MAGIC   STORED AS SCD TYPE 2;                                    -- SCD Type 2: Maintains historical versions with __START_AT and __END_AT
# MAGIC <!-------------------END SOLUTION CODE------------------->
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
# MAGIC **Review the code**
# MAGIC
# MAGIC - `CREATE FLOW customers_scd_type_2_flow AS` - Creates a named flow (`customers_scd_type_2_flow`) that defines how CDC changes will be processed.  
# MAGIC - `AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2_demo` - Applies the CDC logic to the **target** Silver table.  
# MAGIC - `FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean_demo` - Reads the streaming **source data** that includes new inserts, updates, and deletes.  
# MAGIC - `KEYS (customer_id)` - Identifies the unique customer record by its primary key.  
# MAGIC - `APPLY AS DELETE WHEN operation = "DELETE"` - Ensures records with a delete operation are removed from the target.  
# MAGIC - `SEQUENCE BY timestamp_datetime` - Orders incoming records so late-arriving data is processed correctly.  
# MAGIC - `COLUMNS * EXCEPT (timestamp, _rescued_data, operation)` - Selects all columns from the source except metadata or system fields.  
# MAGIC - `STORED AS SCD TYPE 2` - Specifies the **Slowly Changing Dimension Type 2** method, which updates records in place, keeping historical versions.
# MAGIC
# MAGIC - **NOTE:** For more information view the Databricks documentation [AUTO CDC INTO (Lakeflow Spark Declarative Pipelines)](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-apply-changes-into).

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Select **Dry Run** to verify that your pipeline and SQL code are configured correctly.  
# MAGIC    
# MAGIC    This will validate your settings without actually running the full pipeline.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_dry_run_auto_cdc_run1.png" alt="AUTO CDC" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Pipeline Execution and Analysis (Run with 1 JSON File)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Run the Spark Declarative Pipeline
# MAGIC
# MAGIC 1. In the **Lakeflow Pipelines Editor**, run your **Spark Declarative Pipeline**. The first run may take a few minutes.  
# MAGIC
# MAGIC 2. Once the pipeline finishes, verify it looks similar to the example below.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_01_pipeline-run.png" alt="Pipeline Run 1" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Quick Review: AUTO CDC INTO (SCD Type 2 Behavior)
# MAGIC
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/02-scd-type-2-01-review-slide.png" alt="SCD Type 2 Review" width="1200">
# MAGIC
# MAGIC Recall `AUTO CDC INTO` in Spark Declarative Pipelines automatically tracks historical changes without manual merge logic. With **SCD Type 2** it creates two new columns:
# MAGIC
# MAGIC - **__START_AT** - Timestamp when the record becomes active. 
# MAGIC
# MAGIC - **__END_AT** - Timestamp when the record is closed (after an update or delete).  
# MAGIC
# MAGIC When a row changes, the old version gets an **__END_AT** timestamp, and a new version starts with a fresh **__START_AT**. 
# MAGIC
# MAGIC This keeps a full SCD Type 2 history automatically.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Explore the Spark Declarative Pipeline Results
# MAGIC
# MAGIC After the pipeline completes, review the outputs to confirm data flow and CDC behavior on the first run with **one** JSON file.
# MAGIC
# MAGIC 1. In the pipeline graph:
# MAGIC    - **939** rows were streamed through all layers, passing Bronze data quality checks and upserting into **sdp_cdc_2_silver.customers_silver_scd2_demo**.
# MAGIC
# MAGIC 2. In the **Lakeflow Pipeline Editor** table window:
# MAGIC    - View the **Upserted records** column in **sdp_cdc_2_silver.customers_silver_scd2_demo** and confirm all **939** rows were inserted as new customers.
# MAGIC
# MAGIC 3. In the **Tables** view, select the **customers_silver_scd2_demo** table:
# MAGIC    - Preview the table to inspect the data.
# MAGIC    - Scroll right to see the **__START_AT** and **__END_AT** columns  were automatically added by the `AUTO CDC INTO` SCD Type 2 process indicating active, inactive and removed data (right now all rows are active - **__END_AT** = `null`).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Explore the Pipeline CDC Silver Streaming Table

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to confirm that `current_catalog()` references your catalog and that your settings are still active (they might have been cleared).  
# MAGIC
# MAGIC     If they were cleared, reset your default catalog using: `USE CATALOG your_catalog_name;`
# MAGIC
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS `Your Current Catalog`

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the query below to view the **sdp_cdc_2_silver.customers_silver_scd2_demo** CDC streaming table (SCD Type 2).  
# MAGIC
# MAGIC Observe the following:
# MAGIC
# MAGIC - The table contains all **939 rows** from the **00.json** file, as all customers are new.  
# MAGIC
# MAGIC - Scroll to the right to see that `AUTO CDC INTO (SCD Type 2)` added two columns:  
# MAGIC   - **__START_AT**: Timestamp showing when the current version of each record became active. This matches the sequence column **timestamp_datetime**.  
# MAGIC   - **__END_AT**: 
# MAGIC     - Timestamp showing when the record became inactive due to a **DELETE** or **UPDATE**. 
# MAGIC     - Filtering where **__END_AT** is `NULL` returns all active customers.  
# MAGIC
# MAGIC Since this is the first ingestion from **00.json**, all records are active and **__END_AT** contains only `NULL` values.
# MAGIC
# MAGIC **NOTES:**  
# MAGIC - These columns implement **Slowly Changing Dimension (SCD) Type 2** tracking.  
# MAGIC - `AUTO CDC INTO` automatically manages **__START_AT** and **__END_AT** to record the valid time range of each version.  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, email, timestamp_datetime, processing_time, source_file, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to query the CDC table for **all current customers** by filtering where **__END_AT** is `NULL`. 
# MAGIC
# MAGIC    Recall a `NULL` value in the **__END_AT** column indicates all active (current) records.
# MAGIC
# MAGIC    Review the results and confirm that on the first run all **939** records are active.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, email, timestamp_datetime, processing_time, source_file, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE __END_AT IS NULL;    -- Find all current customers

# COMMAND ----------

# MAGIC %md
# MAGIC 4. To find all **deleted customer records** in an SCD Type 2 table created with `AUTO CDC INTO`, you need to locate the **most recent version of each record** and check if the **__END_AT** `IS NOT NULL` (contains a value).  
# MAGIC
# MAGIC     - **NOTE:** Recall a non-null **__END_AT** value indicates that the record has been marked as deleted.  
# MAGIC
# MAGIC     The example query below demonstrates one way to find these records. Run the cell and confirm that **0** results are returned, since no deletions have occurred yet in the **sdp_cdc_2_silver.customers_silver_scd2_demo** CDC table. 
# MAGIC
# MAGIC
# MAGIC **Code details**
# MAGIC - This identifies **removed customers** by grouping all versions, selecting each customer's latest record, and keeping only those marked as deleted, showing their final details before removal.
# MAGIC
# MAGIC     - **Use MAX_BY to find the latest record** - The function `MAX_BY(value, __START_AT)` returns the value from the row with the most recent `__START_AT` timestamp.  
# MAGIC         - `MAX_BY(name, __START_AT)` - most recent name  
# MAGIC         - `MAX_BY(address, __START_AT)` - most recent address  
# MAGIC         - `MAX_BY(__END_AT, __START_AT)` - most recent deletion marker from the most recent record
# MAGIC         - etc.
# MAGIC
# MAGIC     - `GROUP BY customer_id` - Collects all historical versions of each customer into a single group so we can analyze changes over time.
# MAGIC
# MAGIC     - `HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL` - Condition keeps only customers whose latest record has a value for `__END_AT`, meaning they have been removed.
# MAGIC
# MAGIC     - [max_by aggregate function](https://docs.databricks.com/aws/en/sql/language-manual/functions/max_by)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Currently this query will return 0 results since no customers were marked as deleted
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   MAX_BY(name, __START_AT)      AS name,
# MAGIC   MAX_BY(address, __START_AT)   AS address,
# MAGIC   MAX_BY(city, __START_AT) AS city,
# MAGIC   MAX_BY(state, __START_AT)   AS state,
# MAGIC   MAX_BY(zip_code, __START_AT)   AS zip_code,
# MAGIC   MAX_BY(__START_AT, __START_AT) AS __START_AT,
# MAGIC   MAX_BY(__END_AT, __START_AT)  AS __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC GROUP BY customer_id
# MAGIC HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Create Gold Materialized Views
# MAGIC
# MAGIC Rather than having users manually query the CDC table to **find current or deleted customers**, you can create **Gold materialized views** that surface this information automatically as an object.  
# MAGIC
# MAGIC These views simplify access for business users and can be added directly to your **Spark Declarative Pipeline** for continuous updates.
# MAGIC
# MAGIC In this step we'll create two views:
# MAGIC
# MAGIC    - a. **current_customers_gold_demo** - Returns only the most recent, active customer records (where `__END_AT IS NULL`).  
# MAGIC
# MAGIC    - b. **removed_customers_gold_demo** - Creates a **Gold Materialized View** that lists all customers who have been **removed (deleted)** from the SCD Type 2 table.  (It uses `MAX_BY` on multiple columns like `name`, `address`, `city`, `state`, and `zip_code` to return the most recent record per customer where the latest `__END_AT` value is not `NULL`.)
# MAGIC
# MAGIC **NOTES:**
# MAGIC
# MAGIC - Materialized views in **Spark Declarative Pipelines** provide an automatically updated layer for reporting and dashboards. They make it easy to query only the **current customer state** or identify **removed customers** without scanning the entire SCD history.
# MAGIC
# MAGIC - When possible, **materialized views** are incrementally updated. For more information, see the [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh).
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Create the Current Customers Materialized View
# MAGIC
# MAGIC 1. Copy and paste the code below into your `cdc_pipeline.sql` file to create your gold materialized views. Explore the code.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC -- STEP 4: Create Materialized View for Current (Active) Customers
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- a. Create Gold Materialized View for Current Customers
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.current_customers_gold_demo
# MAGIC COMMENT "Current updated list of active customers"
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   * EXCEPT (processing_time),
# MAGIC   current_timestamp() updated_at
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE `__END_AT` IS NULL;      -- Filter for only rows that contain a null value for __END_AT, which indicates the current version of the record
# MAGIC <!-------------------END SOLUTION CODE------------------->
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
# MAGIC ### E2. Create the Deleted Customers Materialized View
# MAGIC
# MAGIC 1. Copy and paste the code below into your `cdc_pipeline.sql` file to create your gold materialized views. Explore the code.
# MAGIC
# MAGIC - **NOTE:** Because this uses `GROUP BY` with aggregates instead of window functions, Lakeflow can update the view incrementally as new CDC data arrives instead of recomputing everything. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC -- b. Create Gold Materialized View that lists all customers who have been removed (deleted) from the SCD Type 2 table.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.removed_customers_gold_demo AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   MAX_BY(name, __START_AT)      AS name,
# MAGIC   MAX_BY(address, __START_AT)   AS address,
# MAGIC   MAX_BY(city, __START_AT) AS city,
# MAGIC   MAX_BY(state, __START_AT)   AS state,
# MAGIC   MAX_BY(zip_code, __START_AT)   AS zip_code,
# MAGIC   MAX_BY(__START_AT, __START_AT) AS __START_AT,
# MAGIC   MAX_BY(__END_AT, __START_AT)  AS __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC GROUP BY customer_id
# MAGIC HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL;  -- Find the latest records __END_AT value and only return if not null
# MAGIC <!-------------------END SOLUTION CODE------------------->
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
# MAGIC ### E3. Run the Final Spark Declarative Pipeline with the Materialized Views
# MAGIC
# MAGIC 1. **REQUIRED:** Since you're developing the pipeline, run it using **Run pipeline with full table refresh** to remove all checkpoints, truncate the tables, and run the pipeline from scratch.
# MAGIC     - Select the **Run Pipeline** dropdown arrow > **Run pipeline with full table refresh**.  
# MAGIC
# MAGIC    This action will:  
# MAGIC    - Remove all existing checkpoints  
# MAGIC    - Drop previously created tables  
# MAGIC    - Restart the pipeline from scratch 
# MAGIC
# MAGIC 2. While the pipeline is running, update the bottom table view to show the **Incrementalization** column.  
# MAGIC    - **NOTE:** The **Incrementalization** column shows whether each materialized view was processed as a **Full recompute** or updated **incrementally**: [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh).  
# MAGIC    
# MAGIC    <img src="./Includes/images/cdc_lecture/incremental_mvs_column2.png" alt="MV Incremental Column" width="1000"> 
# MAGIC <br></br>
# MAGIC 3. After the run completes, verify the pipeline results match the example below.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC
# MAGIC > **TROUBLESHOOTING:** If you see zero rows processed in your streaming tables, you likely ran a normal pipeline run instead of **Run pipeline with full table refresh**. A standard run will not reprocess data that was already ingested and processed.
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_02_pipeline-run-with-mvs-one-file.png" alt="Pipeline Run 2 - MVs" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Explore the Spark Declarative Pipeline Run

# COMMAND ----------

# MAGIC %md
# MAGIC After the pipeline completes, review the outputs to confirm data flow and CDC behavior.
# MAGIC
# MAGIC 1. In the pipeline graph:
# MAGIC    - **939** rows were streamed through all streaming tables, passing Bronze data quality checks and upserting into **sdp_cdc_2_silver.customers_silver_scd2_demo**.
# MAGIC    - **sdp_cdc_3_gold.current_customers_gold_demo** shows **939** active customers, as no updates or deletes have occurred yet and all customers are active.
# MAGIC    - **sdp_cdc_3_gold.removed_customers_gold_demo** shows **0** rows since no customers have been removed.
# MAGIC
# MAGIC 2. In the **Lakeflow Pipeline Editor** table window:
# MAGIC    - The **Upserted records** column in **sdp_cdc_2_silver.customers_silver_scd2_demo** confirms all **939** rows were inserted as new customers.
# MAGIC    - Both Gold materialized views display `Full recompute` in the **Incrementalization** column because this was their first computation.
# MAGIC
# MAGIC 3. In the **Tables** view, select **customers_silver_scd2_demo**:
# MAGIC    - Preview the table to inspect the data.
# MAGIC    - Scroll right to see the **__START_AT** and **__END_AT** columns automatically added by the `AUTO CDC INTO` SCD Type 2 process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Incremental Data Processing
# MAGIC The final pipeline is built, let's add another file to cloud storage!

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Land Additional Data
# MAGIC 1. Before landing additional data to cloud storage, let's query the **sdp_cdc_2_silver.customers_silver_scd2_demo** streaming table for customers *23225* and *23617*.  
# MAGIC
# MAGIC    Review the output and notice the following:
# MAGIC
# MAGIC    - **customer_id = 23225**  
# MAGIC      - **Address:** `76814 Jacqueline Mountains Suite 815`  
# MAGIC      - **State:** `TX`  
# MAGIC      - **__END_AT:** `null`, indicating this is the current active record for the customer.
# MAGIC
# MAGIC    - **customer_id = 23617**  
# MAGIC      - This record currently exists in the table and is active (**__END_AT** is `null`).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE customer_id IN (23225, 23617);

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to land a new JSON file (**01.json**) **in** your **customer_source_files** volume to simulate new files being added to your cloud storage location.
# MAGIC
# MAGIC     Confirm that your **customer_source_files** volume contains two JSON files (**00.json and 01.json**).

# COMMAND ----------

## Copy a second JSON file into the user's volume
copy_files(
    copy_from = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/staging/customers', 
    copy_to = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/customer_source_files', 
    n = 2
)

## List files in your volume (confirm two files exist)
spark.sql(f'LIST "{my_vol_path}"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to view the raw data in the **01.json** file before ingesting it into your pipeline.  
# MAGIC
# MAGIC    Key points to notice:
# MAGIC
# MAGIC    - The **01.json** file contains **23** rows.  
# MAGIC    
# MAGIC    - The **operation** column includes **UPDATE**, **DELETE**, and **NEW** values:
# MAGIC      - 12 customers marked as **UPDATE**  
# MAGIC      - 1 customer marked as **DELETE**  
# MAGIC      - 10 customers marked as **NEW**  
# MAGIC
# MAGIC    - For **customer_id = 23225** (Sandy Adams):  
# MAGIC      - Original address (from **00.json**): `76814 Jacqueline Mountains Suite 815`, `TX`  
# MAGIC      - Updated address (in this file): `512 John Stravenue Suite 239`, `TN`  
# MAGIC
# MAGIC    - For **customer_id = 23617**:  
# MAGIC      - The **operation** is **DELETE**.  
# MAGIC      - All other columns contain `NULL` values.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   my_vol_path || '/01.json',
# MAGIC   format => "JSON"
# MAGIC )
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Process Updates and Deletes

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Return to your **Spark Declarative Pipeline** and select **Run pipeline** to incrementally process the new JSON file (**01.json**) and apply SCD Type 2 logic to your Silver table.  
# MAGIC
# MAGIC 2. Once complete, verify the results match the example below.
# MAGIC
# MAGIC ### Checkpoint
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/cdc_run_01json.png" alt="Pipeline Run 2" width="1200">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. After running the pipeline, review the **Pipeline graph**:
# MAGIC
# MAGIC    - **23 rows** were incrementally ingested from the new **01.json** file into both **customers_bronze_raw_demo** and **customers_bronze_clean_demo** , passing all data quality checks. 
# MAGIC
# MAGIC    - In the **customers_silver_scd2_demo** streaming table (SCD Type 2), **35 rows** were processed and **upserted**.  
# MAGIC      - Breakdown of the **35** upserts from the **01.json** file:  
# MAGIC         - **12 updates** - each update adds a new version(row) and marks the old one inactive (**24 total upserts**)  
# MAGIC         - **1 delete** - sets a value in the **__END_AT** column  
# MAGIC         - **10 new** customer inserts 
# MAGIC
# MAGIC      **Total changes:** 24 + 1 + 10 = **35**
# MAGIC
# MAGIC    - The **current_customers_gold_demo** materialized view contains **948** active customers.
# MAGIC
# MAGIC    - The **removed_customers_gold_demo** materialized view contains **1** deleted customer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Pipeline Object Analysis and Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Query the CDC Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Query the data in the **sdp_cdc_2_silver.customers_silver_scd2_demo** streaming table with SCD Type 2 and observe the following:
# MAGIC
# MAGIC    a. The table contains **961 rows**
# MAGIC
# MAGIC       - **initial 939 customers**  
# MAGIC       - \+ **12 updates** to existing customers 
# MAGIC       - \+ **10 new customers**
# MAGIC
# MAGIC    b. Scroll to the right and locate the **__END_AT** column. Then scroll down to rows **82** and **83**. Notice there are two rows for customer **22668**, the original record and the updated record.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC ORDER BY customer_id, __END_AT;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the query on the **sdp_cdc_2_silver.customers_silver_scd2_demo** streaming table for all rows where **__END_AT** `IS NOT NULL` to view all rows where those customers rows are now inactive.
# MAGIC
# MAGIC Notice the following:
# MAGIC   - **13 rows** are returned (**12 UPDATES** + **1 DELETE**)
# MAGIC   - The **__END_AT** column indicates the date and time that the row was either updated or marked as removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE __END_AT IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Query the **sdp_cdc_2_silver.customers_silver_scd2_demo** table for the **customer_id** *23225* (one of the customers that was updated). 
# MAGIC
# MAGIC Notice the following:
# MAGIC
# MAGIC - There are **two records** for that customer in the table.
# MAGIC - The original record from the **00.json** file now has a value in the **__END_AT** column, indicating that it is now inactive.
# MAGIC - The new record from the **01.json** file is now the active row and contains a `null` value in the **__END_AT** column.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, state, source_file, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE customer_id = 23225;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. In the **01.json** file, recall **customer_id** *23617* was marked as deleted. 
# MAGIC
# MAGIC     Let's query the **sdp_cdc_2_silver.customers_silver_scd2_demo** table for that customer and view the results. 
# MAGIC
# MAGIC     Notice that when a customer is marked as deleted, the **__END_AT** column contains the value of when that customer was deleted and became inactive, but the customer records STILL EXISTS in the **sdp_cdc_2_silver.customers_silver_scd2_demo** with SCD Type 2.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE customer_id = 23617

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Query the Current Customers Materialized View
# MAGIC
# MAGIC 1. To view your organization's most up-to-date customer data, you can query the materialized view **sdp_cdc_3_gold.current_customers_gold_demo**. 
# MAGIC
# MAGIC     Remember, the query to create the materialized view filters for all **__END_AT** values that are `null` (active rows).
# MAGIC
# MAGIC     Run the cell and view the results. Notice the following:
# MAGIC    - The current updated list of customers contains **948 rows**:
# MAGIC      - **939** from the initial file (**00.json**)
# MAGIC      - **+10** new customers from the update file (**01.json**)
# MAGIC      - **-1** deleted customer from the update file (**01.json**)
# MAGIC      - The table also contains the updated records from the **01.json** file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT, source_file
# MAGIC FROM sdp_cdc_3_gold.current_customers_gold_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Query the Removed Customers Materialized View
# MAGIC 1. To view your organization's deleted customers, you can query the materialized view **sdp_cdc_3_gold.removed_customers_gold_demo**. 
# MAGIC
# MAGIC     Run the cell and view the results. Notice that there has been **1** customer marked as deleted (**customer_id** = *23617*) in the CDC silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_cdc_3_gold.removed_customers_gold_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. (Optional) Continue Incrementally Processing New Data
# MAGIC
# MAGIC If you'd like to continue practicing, use the demonstration function `copy_files` to dynamically add another JSON file to your cloud storage location. 
# MAGIC
# MAGIC Simply remove the `%skip` and run the cell to add a new file into your source volume.
# MAGIC
# MAGIC **NOTE:** Ensure the variables you defined at the start of this lab: `your_marketplace_share_catalog_name`, `my_catalog` are still active for the function to work properly.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC ## Copy a another JSON file into the user's volume
# MAGIC copy_files(
# MAGIC     copy_from = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/staging/customers', 
# MAGIC     copy_to = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/customer_source_files', 
# MAGIC     n = 3 # <-- This value determines how many files should be in the volume. This will add the third JSON file (02.json) to the volume. You can continue using this until you run out of files.
# MAGIC )
# MAGIC
# MAGIC ## List files in your volume
# MAGIC spark.sql(f'LIST "{my_vol_path}"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Lab Clean Up
# MAGIC 1. Feel free to delete the schemas you create in this demonstration by running cell below and confirming the delete (**Y**).

# COMMAND ----------

delete_schemas(
    catalog = my_catalog, ## <--- Your catalog name using the variable you set earlier
    schemas = ['sdp_cdc_1_bronze','sdp_cdc_2_silver','sdp_cdc_3_gold']
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Delete your Spark Declarative Pipeline through the **Jobs & Pipelines** UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Summary and Key Takeaways
# MAGIC
# MAGIC You have successfully implemented an end-to-end **AUTO CDC for SCD Type 2** pipeline using **Lakeflow Spark Declarative Pipelines**. This demonstration showcased how modern data engineering can dramatically simplify complex Change Data Capture operations that traditionally required extensive manual coding.
# MAGIC
# MAGIC **Simplified CDC Implementation:**
# MAGIC - Replaced complex manual `MERGE` operations with declarative `AUTO CDC INTO` syntax
# MAGIC - Automatic handling of late-arriving data using sequence columns
# MAGIC - Built-in SCD Type 2 logic without manual custom coding
# MAGIC
# MAGIC **Robust Data Quality:**
# MAGIC - Multi-layered validation with conditional constraints for DELETE operations
# MAGIC - Flexible violation handling (WARN, DROP, FAIL) based on business requirements
# MAGIC - Comprehensive email validation using regex patterns
# MAGIC
# MAGIC **Incremental Processing Efficiency:**
# MAGIC - Auto Loader for incremental file ingestion (**triggered or continuous**) from cloud storage
# MAGIC - Incremental materialized view updates where possible
# MAGIC - Streaming architecture that processes only new changes

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>