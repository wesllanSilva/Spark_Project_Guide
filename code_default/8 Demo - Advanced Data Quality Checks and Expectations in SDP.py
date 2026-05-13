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
# MAGIC # Demo - Advanced Data Quality Checks and Expectations in Spark Declarative Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This demonstration showcases enterprise-grade data quality management using Spark Declarative Pipelines (Lakeflow) with advanced quality expectations and quarantine patterns. You will build a comprehensive medallion architecture that implements three critical data engineering patterns: range-based validation, automatic schema evolution, and zero-loss quarantine processing.
# MAGIC
# MAGIC The demo simulates a real-world scenario where order data arrives with varying quality levels and evolving schemas. Using official Databricks patterns, you will process this data through bronze, silver, and gold layers while implementing robust quality controls that catch data anomalies, handle schema changes seamlessly, and ensure zero data loss through intelligent quarantine mechanisms.
# MAGIC
# MAGIC This demonstration uses a two-run approach: Run 1 establishes a clean baseline with 157 perfect records to validate the quality framework, while Run 2 introduces 60 records containing intentional quality issues and schema evolution to demonstrate the system's resilience and monitoring capabilities.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC - **Implement comprehensive data validation** using 6 quality expectations covering NOT NULL checks, numeric ranges, and date validations with proper constraint syntax
# MAGIC - **Handle automatic schema evolution** without pipeline code changes using flexible bronze layer design and schema hints
# MAGIC - **Build quarantine patterns** using official Databricks inverse logic to capture invalid records while maintaining zero data loss and detailed failure tracking
# MAGIC - **Monitor data quality metrics** and analyze expectation performance to identify specific data quality issues and develop remediation strategies

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Required - Select a Compute Environment
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
# MAGIC **NOTE:** This notebook was **developed and tested using Serverless V4**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Architecture Overview
# MAGIC
# MAGIC This demo demonstrates how to build a robust data pipeline using Databricks medallion architecture, focusing on data quality enforcement and schema flexibility.
# MAGIC
# MAGIC ### Key Patterns Demonstrated
# MAGIC
# MAGIC **1. Advanced Data Quality Checks**
# MAGIC - Enforce comprehensive data quality rules: NOT NULL for business keys, numeric range validations, and temporal checks
# MAGIC - Invalid rows are systematically quarantined with detailed failure analysis for remediation workflows
# MAGIC
# MAGIC **2. Seamless Schema Evolution**
# MAGIC - Handles new columns automatically—no pipeline code changes required
# MAGIC - Bronze layer stores all columns as STRING for maximum flexibility
# MAGIC - Silver layer performs safe type conversion with validation
# MAGIC
# MAGIC **3. Zero-Loss Quarantine Pattern**
# MAGIC - Invalid records are captured in a dedicated quarantine table ensuring no data loss
# MAGIC - Tracks which specific rules failed for each record with detailed failure reasons
# MAGIC - Enables parallel processing of valid and invalid data streams
# MAGIC
# MAGIC ### Demo Pipeline Architecture
# MAGIC
# MAGIC <img src="./Includes/images/dq/dq_pipeline_overview.png" alt="Complete Pipeline Run 1" width="1200">
# MAGIC
# MAGIC ### Demo Execution Flow
# MAGIC
# MAGIC - **Run 1:** Clean baseline data (157 records) — all pass quality checks, establishes 100% quality baseline
# MAGIC - **Run 2:** Data with quality issues (60 records, 2 new columns) — demonstrates issue detection and schema evolution

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
# MAGIC Run the cell below to initialize your environment. This setup step performs the following actions:
# MAGIC
# MAGIC - **Creates or validates catalog access** when running outside of a Databricks provided Vocareum workspace
# MAGIC - **Creates four schemas** in your specified catalog:
# MAGIC     - **dq_1_bronze** - Raw data ingestion layer
# MAGIC     - **dq_2_silver** - Validated and transformed data layer  
# MAGIC     - **dq_3_gold** - Analytics-ready aggregated data layer
# MAGIC - **Creates volume storage** for source data processing:
# MAGIC     - `sales` volume: Active processing location for pipeline consumption
# MAGIC     - `ops` volume: Staging location containing multiple data files
# MAGIC - **Verifies compute environment** compatibility with Lakeflow pipelines
# MAGIC
# MAGIC This ensures that all schemas, tables, and objects are created in your designated catalog with proper isolation.
# MAGIC
# MAGIC **Important:** You must have permission to create catalogs in your own non-Vocareum workspace. If you do not have the required permissions, this step will fail. Review the troubleshooting note below before continuing.

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
# MAGIC This function is defined in the notebook: `./Includes/Classroom-Setup-dq`
# MAGIC
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-dq

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Verify Volume Path Configuration
# MAGIC
# MAGIC Run the cell below to view the value of the `my_vol_path` variable and confirm that it references your **your-catalog.dq_1_bronze** path. This path will be used to dynamically reference your source volumes throughout this demonstration.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Source Data Characteristics
# MAGIC
# MAGIC Before building the pipeline, examine the source data to understand its structure, quality characteristics, and schema evolution patterns. This analysis will inform our quality expectation design.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Examine Available Data Files
# MAGIC
# MAGIC View all files available in your **ops** volume. Note that it contains multiple files that will be incrementally moved to the `sales` volume for processing, simulating real-world data arrival patterns.

# COMMAND ----------

# List files in volume
display(spark.sql(f"LIST '{my_vol_path}/ops'"))

# COMMAND ----------

# MAGIC %md
# MAGIC View the current files available for processing in the **sales** volume location. The `sales` volume currently contains only one file; additional files will be added incrementally from the `ops` location to demonstrate schema evolution and quality issue detection.

# COMMAND ----------

# List files in volume
display(spark.sql(f"LIST '{my_vol_path}/sales'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Analyze First File - Clean Baseline Data
# MAGIC
# MAGIC Explore the first file which contains clean, high-quality data that will establish our quality baseline and validate the pipeline framework.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/ops/sales_1.csv'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Firt File Characteristics (sales_1.csv):**
# MAGIC - **157 clean records** from a single sales subsidiary with perfect data quality
# MAGIC - **16 columns**: Standard order fields including order_id, customer_id, qty, unit_price, discount_pct, total_amount, order_date
# MAGIC - **Quality Profile**: No missing business keys, valid discount percentages (0-100%), recent order dates
# MAGIC - **Purpose**: Establishes 100% quality baseline to validate expectation framework

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Analyze Second File - Quality Issues and Schema Evolution
# MAGIC
# MAGIC Explore the second file which contains intentional quality issues and demonstrates schema evolution with new business columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/ops/sales_2.csv'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Second File Characteristics (sales_2.csv):**
# MAGIC
# MAGIC **Schema Evolution (New Business Columns):**
# MAGIC - **19 columns** (16 original + 2 new business fields)
# MAGIC - **New Column**: `order_status` (confirmed, pending, shipped, delivered, cancelled)
# MAGIC - **New Column**: `shipping_cost` (numeric values for shipping fees)
# MAGIC
# MAGIC **Intentional Quality Issues for Testing:**
# MAGIC - **Invalid discount percentages**: discount_pct = 120 (violates discount <= 100% rule)
# MAGIC - **Negative discounts**: discount_pct = -10.73 (violates discount >= 0 rule)
# MAGIC - **Historical dates**: order_date = 1930-12-31 (violates recent date range requirement)
# MAGIC - **Missing business keys**: NULL order_id or customer_id values
# MAGIC - **Invalid shipping costs**: shipping_cost > 100 or shipping_cost < 0
# MAGIC
# MAGIC **Expected Quality Results:**
# MAGIC - Approximately 85% of records will pass all quality checks
# MAGIC - 15% will be quarantined with detailed failure reasons for remediation

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Analysis Checkpoint:**
# MAGIC - **sales_1.csv**: 157 rows (clean baseline for framework validation)
# MAGIC - **sales_2.csv**: 60 rows (quality issues + schema evolution demonstration)
# MAGIC - **Total Expected**: 217 records across both files with varying quality profiles

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create the Spark Declarative Pipeline
# MAGIC
# MAGIC Now that we understand the data characteristics and quality challenges, create the Spark Declarative Pipeline with comprehensive data quality expectations and quarantine handling.

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
# MAGIC    - **Pipeline Name**: `demo_dq_yourname`
# MAGIC    - **Default Catalog**: `YOUR_LABUSER_CATALOG`
# MAGIC    - **Default Schema**: `dq_1_bronze`  
# MAGIC    **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. **Rename pipeline components**:
# MAGIC    - Rename the **transformations** folder to `dq_pipeline`
# MAGIC    - Rename **my_transformations.sql** file to `bronze_ingestion.sql`
# MAGIC
# MAGIC 5. Leave the **Lakeflow Pipelines Editor** page open for the next steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Configure Pipeline Parameters
# MAGIC
# MAGIC 1. Run the cell below to retrieve the key-value pairs needed to set your pipeline configuration parameters for the **source volume**.

# COMMAND ----------

config_parameters = [
    ('source', f'{my_vol_path}/sales')
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
# MAGIC ## D. Create Bronze Layer - Raw Ingestion with Schema Evolution
# MAGIC
# MAGIC The bronze layer implements schema evolution by using the `read_files` function with flexible column handling and schema hints for future column compatibility.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Create Bronze Table with Schema Evolution Support
# MAGIC
# MAGIC Copy the code below into your `bronze_ingestion.sql` file. This implementation uses a STRING-based approach for maximum schema flexibility and includes schema hints for future columns.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------
# MAGIC -- BRONZE LAYER: RAW DATA INGESTION
# MAGIC ------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_1_bronze.sales_bronze_raw_demo
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- Core business fields (all as STRING for schema flexibility)
# MAGIC   CAST(subsidiary_id AS STRING) AS subsidiary_id,
# MAGIC   CAST(order_id AS STRING) AS order_id,
# MAGIC   CAST(order_timestamp AS STRING) AS order_timestamp,
# MAGIC   CAST(customer_id AS STRING) AS customer_id,
# MAGIC   CAST(region AS STRING) AS region,
# MAGIC   CAST(country AS STRING) AS country,
# MAGIC   CAST(city AS STRING) AS city,
# MAGIC   CAST(channel AS STRING) AS channel,
# MAGIC   CAST(sku AS STRING) AS sku,
# MAGIC   CAST(category AS STRING) AS category,
# MAGIC   CAST(qty AS STRING) AS qty,
# MAGIC   CAST(unit_price AS STRING) AS unit_price,
# MAGIC   CAST(discount_pct AS STRING) AS discount_pct,
# MAGIC   CAST(coupon_code AS STRING) AS coupon_code,
# MAGIC   CAST(total_amount AS STRING) AS total_amount,
# MAGIC   CAST(order_date AS STRING) AS order_date,
# MAGIC
# MAGIC   -- New columns for schema evolution (will be null initially)
# MAGIC   CAST(order_status AS STRING) AS order_status,
# MAGIC   CAST(shipping_cost AS STRING) AS shipping_cost,
# MAGIC
# MAGIC   -- Rescued data column for parsing issues
# MAGIC   CAST(_rescued_data AS STRING) AS _rescued_data,
# MAGIC
# MAGIC   -- Metadata columns for lineage tracking
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${source}',
# MAGIC   format => 'csv',
# MAGIC   schemaHints => 'order_status STRING, shipping_cost STRING'
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
# MAGIC **Review the Bronze Layer Design Patterns:**
# MAGIC
# MAGIC **Schema Evolution Strategy:**
# MAGIC - **STRING storage**: All business columns stored as STRING for maximum flexibility and compatibility
# MAGIC - **Pre-defined schema**: Includes both original and two future columns for seamless evolution
# MAGIC - **Schema hints**: Declares expected new columns (`order_status`, `shipping_cost`) even when not yet present in data
# MAGIC - **Null handling**: New columns will be NULL for existing files, populated for future files
# MAGIC
# MAGIC **Data Lineage and Protection:**
# MAGIC - **Auto Loader**: `read_files` function enables incremental processing with checkpoint management
# MAGIC - **Metadata capture**: Source file names and modification timestamps for complete data lineage
# MAGIC - **Rescued data**: `_rescued_data` column captures any parsing issues for investigation and recovery
# MAGIC - **Parameter reference**: `${source}` enables dynamic path configuration
# MAGIC
# MAGIC **Technical Benefits:**
# MAGIC - No pipeline code changes required when new columns arrive
# MAGIC - Backward compatibility with existing data files
# MAGIC - Forward compatibility with evolving business requirements
# MAGIC
# MAGIC For more information about schema hints functionality, see the [Databricks documentation on schema hints](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#override-schema-inference-with-schema-hints).

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Create Clean Bronze Table
# MAGIC
# MAGIC Create a streaming intermediate Bronze table that performs safe type casting from the raw ingested data, without applying any quality checks. This step ensures clean type conversion before data validation.
# MAGIC
# MAGIC - Creates a clean streaming Bronze table from the source data stream.
# MAGIC - Uses `TRY_CAST` to safely convert data types (dates, numbers) without failing on bad data.
# MAGIC - Passes through all columns without applying any data quality filters yet.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- BRONZE LAYER: INTERMEDIATE TRANSFORMATION TABLE
# MAGIC -- Purpose: Clean type casting from Bronze Raw (no quality checks yet)
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_1_bronze.sales_bronze_clean_demo
# MAGIC COMMENT "Intermediate table - type casting only, no quality checks"
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Direct pass-through and safe type casting
# MAGIC   subsidiary_id,
# MAGIC   order_id,
# MAGIC   TRY_CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
# MAGIC   TRY_CAST(order_date AS DATE) AS order_date,
# MAGIC   customer_id,
# MAGIC   region,
# MAGIC   country,
# MAGIC   city,
# MAGIC   channel,
# MAGIC   sku,
# MAGIC   category,
# MAGIC   TRY_CAST(qty AS INT) AS qty,
# MAGIC   TRY_CAST(unit_price AS DOUBLE) AS unit_price,
# MAGIC   TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
# MAGIC   TRY_CAST(total_amount AS DOUBLE) AS total_amount,
# MAGIC   coupon_code,
# MAGIC   order_status,
# MAGIC   TRY_CAST(shipping_cost AS DOUBLE) AS shipping_cost,
# MAGIC   source_file
# MAGIC
# MAGIC FROM STREAM dq_1_bronze.sales_bronze_raw_demo;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Run and Validate the Bronze Layer
# MAGIC
# MAGIC 1. **Run the pipeline** and confirm that it completes successfully with no errors.
# MAGIC
# MAGIC 2. **Validate ingestion results** in the Lakeflow Pipelines editor:
# MAGIC    - Verify that **157 records** were ingested into the sales_bronze_raw_demo and table from the first file
# MAGIC    - Select the **sales_bronze_raw_demo** table and open the **Data** tab to preview the ingested sales data
# MAGIC    - Confirm that new columns (`order_status`, `shipping_cost`) exist but contain NULL values
# MAGIC
# MAGIC **Troubleshooting:** If your pipeline fails, verify that:
# MAGIC - Volume paths are correctly configured in pipeline parameters
# MAGIC - The `source` configuration parameter matches your volume path
# MAGIC - Serverless compute is selected and running
# MAGIC
# MAGIC **Expected Checkpoint Results:**
# MAGIC
# MAGIC <img src="./Includes/images/dq/checkpoint_bronze.png" alt="Bronze Layer Results" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Query and Analyze Bronze Layer
# MAGIC
# MAGIC Run these queries to validate the bronze layer tables:
# MAGIC
# MAGIC - Confirm that all records are ingested into the raw bronze table
# MAGIC - Check that `schema_hints` worked successfully

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify record ingestion by source file (should show only sales_1.csv)
# MAGIC SELECT 
# MAGIC   source_file,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   'Clean baseline data' AS data_profile
# MAGIC FROM dq_1_bronze.sales_bronze_raw_demo
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schema evolution readiness
# MAGIC -- Total Column should be 21
# MAGIC DESCRIBE TABLE dq_1_bronze.sales_bronze_raw_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Create Silver Layer with Data Quality Expectations and Quarantine Logic
# MAGIC
# MAGIC - In this layer, we will first type-cast columns to their appropriate data formats.
# MAGIC - Next, we will apply data quality expectations.
# MAGIC - Finally, based on these expectations, we will separate the data into two tables: one for clean records and one for invalid records, using inverse logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Understanding Data Quality Expectations Framework
# MAGIC
# MAGIC Before creating tables with expectations, understand the expectation framework and why each validation is critical for enterprise data quality.
# MAGIC
# MAGIC **Expectation Syntax:**
# MAGIC ```sql
# MAGIC CONSTRAINT <constraint_name> 
# MAGIC   EXPECT (<condition>) 
# MAGIC   ON VIOLATION <action>
# MAGIC ```
# MAGIC
# MAGIC **Available Violation Actions:**
# MAGIC - **WARN** (default): Invalid records included but violation logged for monitoring
# MAGIC - **DROP ROW**: Invalid records excluded from target table 
# MAGIC - **FAIL UPDATE**: Pipeline stops and requires manual intervention
# MAGIC
# MAGIC **Our Strategy:** We use **WARN** (default) for all expectations to ensure the pipeline continues running and all records are retained for quarantine analysis.
# MAGIC
# MAGIC **Our 6 Comprehensive Quality Expectations:**
# MAGIC
# MAGIC **Category 1: Business Key Validation (Critical for Data Integrity)**
# MAGIC 1. **`check_subsidiary_id`**: Every order must have a subsidiary identifier for proper attribution
# MAGIC 2. **`check_customer_id`**: Must identify which customer placed the order for analytics and compliance
# MAGIC 3. **`check_sku`**: Must identify what product was ordered for inventory and revenue tracking
# MAGIC
# MAGIC **Category 2: Range-Based Validations (Business Rule Enforcement)**
# MAGIC
# MAGIC 4. **`valid_discount_range`**: Discount must be 0%-100% (catches negative discounts and impossible values)
# MAGIC 5. **`valid_date_range`**: Order dates within last 4 years  (catches archived data and future dates)
# MAGIC
# MAGIC **NOTE:** Since our data is static, we are checking the constraint **`valid_date_range`** with respect to **01-01-2026**.
# MAGIC
# MAGIC **Category 3: Schema Evolution Validation (New Column Quality)**
# MAGIC
# MAGIC 6. **`valid_shipping_cost`**: Shipping cost must be between $0 and $100 (validates new business column)
# MAGIC
# MAGIC **Note on Schema Evolution:** The `valid_shipping_cost` expectation applies to a column not present in initial data but expected in future files. Once the column appears, the expectation automatically enforces validation. For details, see [Schema Evolution Pattern](https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=SQL#schema-evolution-pattern).

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Understanding Quarantine Records Pattern
# MAGIC
# MAGIC **Quarantine records** in Spark Declarative Pipelines are records that violate data quality expectations and are routed to a separate quarantine table for analysis and remediation. This enterprise pattern ensures:
# MAGIC
# MAGIC **Benefits:**
# MAGIC - **Zero data loss**: Invalid data is isolated for review, not dropped or causing pipeline failures
# MAGIC - **Detailed tracking**: Metadata captures which specific expectations failed for each record
# MAGIC - **Flexible workflows**: Enables remediation processes, reprocessing, and quality improvement
# MAGIC - **Audit compliance**: Complete record of data quality issues for regulatory requirements
# MAGIC
# MAGIC **Implementation Pattern:**
# MAGIC - **Primary table**: Contains expectations with WARN action (logs violations, retains records)
# MAGIC - **Quarantine logic**: Uses inverse logic (NOT condition) to identify failed records
# MAGIC - **Parallel processing**: Valid and invalid records flow to separate tables simultaneously
# MAGIC
# MAGIC > **Learn More:**  
# MAGIC > 💡 For comprehensive information on quarantine frameworks and best practices, see the [Databricks documentation on expectation patterns and quarantining invalid records](https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=Python%20Module#quarantine-invalid-records).

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Create a New SQL File in your Pipeline
# MAGIC
# MAGIC 1. Click the kebab menu next to your `dq_pipeline` folder and select **Create file**.
# MAGIC 2. Select the language as **SQL**.
# MAGIC 3. Name the file `silver_transformation.sql`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Create Quarantine Table with Quality Expectations
# MAGIC
# MAGIC
# MAGIC Add the following code to your `silver_transformations.sql` file. This creates the main quarantine table with all 6 data quality expectations defined.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- SILVER LAYER - STEP 1: QUARANTINE TABLE WITH EXPECTATIONS
# MAGIC -- Purpose: Define table structure and 6 data quality expectations
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_dq_demo
# MAGIC (
# MAGIC   -- Business columns with proper data types
# MAGIC   subsidiary_id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_timestamp TIMESTAMP,
# MAGIC   order_date DATE,
# MAGIC   customer_id STRING,
# MAGIC   region STRING,
# MAGIC   country STRING,
# MAGIC   city STRING,
# MAGIC   channel STRING,
# MAGIC   sku STRING,
# MAGIC   category STRING,
# MAGIC   qty INT,
# MAGIC   unit_price DOUBLE,
# MAGIC   discount_pct DOUBLE,
# MAGIC   total_amount DOUBLE,
# MAGIC   coupon_code STRING,
# MAGIC   order_status STRING,
# MAGIC   shipping_cost DOUBLE,
# MAGIC   source_file STRING,
# MAGIC
# MAGIC   -- Quality tracking columns for quarantine analysis
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   quarantine_reason STRING,
# MAGIC
# MAGIC   --========================================================================
# MAGIC   -- 6 DATA QUALITY EXPECTATIONS (WARN ACTION FOR QUARANTINE PATTERN)
# MAGIC   --========================================================================
# MAGIC
# MAGIC   CONSTRAINT check_subsidiary_id 
# MAGIC     EXPECT (subsidiary_id IS NOT NULL),
# MAGIC
# MAGIC   CONSTRAINT check_customer_id 
# MAGIC     EXPECT (customer_id IS NOT NULL),
# MAGIC
# MAGIC   CONSTRAINT check_sku 
# MAGIC     EXPECT (sku IS NOT NULL),
# MAGIC
# MAGIC   CONSTRAINT valid_discount_range
# MAGIC     EXPECT (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)),
# MAGIC
# MAGIC   -- Since the data is static, we check that order_date falls between 01-01-2026 and 4 years prior, instead of using the current date.
# MAGIC   CONSTRAINT valid_date_range 
# MAGIC     EXPECT (order_date IS NULL OR 
# MAGIC         (order_date >= DATE_SUB(DATE '2026-01-01', 1460) AND 
# MAGIC           order_date <= DATE '2026-01-01')),
# MAGIC
# MAGIC   CONSTRAINT valid_shipping_cost EXPECT (
# MAGIC     CASE WHEN shipping_cost IS NOT NULL THEN shipping_cost > 0 AND shipping_cost < 100 ELSE TRUE END
# MAGIC   )
# MAGIC )
# MAGIC COMMENT "Quarantine table with 6 expectations - supports inverse logic pattern"
# MAGIC TBLPROPERTIES (
# MAGIC   'quality.layer' = 'silver_quarantine',
# MAGIC   'quality.pattern' = 'inverse_logic'
# MAGIC )
# MAGIC PARTITIONED BY (is_quarantined);
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the Expectation Framework:**
# MAGIC
# MAGIC **Expectations Summary:**
# MAGIC
# MAGIC | # | Expectation Name | Validates | Range/Rule | Violation Example |
# MAGIC |---|-----------------|-----------|------------|---------------------|
# MAGIC | 1 | check_subsidiary_id | Business Key | NOT NULL | Missing subsidiary IDs |
# MAGIC | 2 | check_customer_id | Business Key | NOT NULL | Missing customer IDs |
# MAGIC | 3 | check_sku | Business Key | NOT NULL | Missing SKUs |
# MAGIC | 4 | valid_discount_range | Discount | 0%-100% | discount = -10.73% or 120% |
# MAGIC | 5 | valid_date_range | Date | Last 4 years | date = 1930-12-31 |
# MAGIC | 6 | valid_shipping_cost | Shipping | $0-$100 | shipping_cost = -5 or 150 |
# MAGIC
# MAGIC **Quality Tracking Design:**
# MAGIC - **is_quarantined**: Boolean flag indicating whether record failed any expectation
# MAGIC - **quarantine_reason**: Detailed string listing all failed validations for remediation
# MAGIC - **Partitioning**: Separates quarantined and valid data for performance optimization
# MAGIC
# MAGIC **Table Properties:**
# MAGIC - Metadata tags identify this as a quarantine pattern implementation
# MAGIC - Enables monitoring and governance tools to recognize the quality framework

# COMMAND ----------

# MAGIC %md
# MAGIC ### E5. Create Inverse Logic Flow for Quarantine Population
# MAGIC
# MAGIC Add the inverse logic flow to populate the quarantine table with quality tracking information. This flow implements the official Databricks quarantine pattern by using inverse logic to identify and flag records that fail any quality expectations.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- SILVER LAYER - STEP 2: FLOW TO APPLY INVERSE LOGIC
# MAGIC -- Purpose: Calculate is_quarantined flag and populate quarantine table
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE FLOW apply_inverse_logic_flow
# MAGIC AS
# MAGIC INSERT INTO dq_2_silver.sales_silver_dq_demo BY NAME
# MAGIC SELECT
# MAGIC   -- Pass through all business columns
# MAGIC   subsidiary_id,
# MAGIC   order_id,
# MAGIC   order_timestamp,
# MAGIC   order_date,
# MAGIC   customer_id,
# MAGIC   region,
# MAGIC   country,
# MAGIC   city,
# MAGIC   channel,
# MAGIC   sku,
# MAGIC   category,
# MAGIC   qty,
# MAGIC   unit_price,
# MAGIC   discount_pct,
# MAGIC   total_amount,
# MAGIC   coupon_code,
# MAGIC   order_status,
# MAGIC   shipping_cost,
# MAGIC   source_file,
# MAGIC
# MAGIC   --========================================================================
# MAGIC   -- INVERSE LOGIC: Mark as quarantined if ANY expectation fails
# MAGIC   --========================================================================
# MAGIC   NOT (
# MAGIC     (subsidiary_id IS NOT NULL) AND
# MAGIC     (customer_id IS NOT NULL) AND
# MAGIC     (sku IS NOT NULL) AND
# MAGIC     (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)) AND
# MAGIC     (order_date IS NULL OR 
# MAGIC      (order_date >= DATE_SUB(DATE '2026-01-01', 1460) AND 
# MAGIC       order_date <= DATE '2026-01-01')) AND
# MAGIC     (shipping_cost IS NULL OR (shipping_cost > 0 AND shipping_cost < 100))
# MAGIC   ) AS is_quarantined,
# MAGIC
# MAGIC   --========================================================================
# MAGIC   -- BUILD DETAILED QUARANTINE REASON STRING
# MAGIC   --========================================================================
# MAGIC   CONCAT_WS('; ',
# MAGIC     CASE WHEN subsidiary_id IS NULL 
# MAGIC          THEN 'Missing subsidiary_id' END,
# MAGIC     CASE WHEN customer_id IS NULL 
# MAGIC          THEN 'Missing customer_id' END,
# MAGIC     CASE WHEN sku IS NULL 
# MAGIC          THEN 'Missing sku' END,
# MAGIC     CASE WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100) 
# MAGIC          THEN 'Invalid discount_pct (must be 0-100)' END,
# MAGIC     CASE WHEN order_date IS NOT NULL AND 
# MAGIC               (order_date < DATE_SUB(DATE '2026-01-01', 1460) OR order_date > DATE '2026-01-01') 
# MAGIC          THEN 'Invalid order_date (outside 4-year range)' END,
# MAGIC     CASE WHEN shipping_cost IS NOT NULL AND (shipping_cost <= 0 OR shipping_cost >= 100)
# MAGIC          THEN 'Invalid shipping_cost (must be 0-100)' END
# MAGIC   ) AS quarantine_reason
# MAGIC
# MAGIC FROM STREAM dq_1_bronze.sales_bronze_clean_demo;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the Inverse Logic Implementation:**
# MAGIC
# MAGIC **Quarantine Flag Logic:**
# MAGIC - **NOT()** wrapper around all expectations combined with AND logic
# MAGIC - If ANY expectation fails, the record is marked `is_quarantined = TRUE`
# MAGIC - If ALL expectations pass, the record is marked `is_quarantined = FALSE`
# MAGIC
# MAGIC **Detailed Failure Tracking:**
# MAGIC - **CONCAT_WS()** builds semicolon-separated failure reasons
# MAGIC - Each CASE statement checks for specific violation types
# MAGIC - Provides actionable information for data remediation teams
# MAGIC - Empty string for records that pass all validations
# MAGIC
# MAGIC **Streaming Flow Benefits:**
# MAGIC - Processes records continuously as they arrive
# MAGIC - Maintains low latency for real-time quality monitoring
# MAGIC - Enables immediate quarantine identification for operational workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### E6. Create Separate Tables for Valid and Quarantined Records
# MAGIC
# MAGIC Create separate tables for valid and quarantined records to enable zero data loss and parallel processing workflows. Add this code to your `silver_transformations.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- SILVER LAYER - STEP 3: SEPARATE VALID AND INVALID DATA PATHS
# MAGIC --========================================================================
# MAGIC
# MAGIC --========================================================================
# MAGIC -- VALID RECORDS PATH - Clean data for analytics
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_valid_demo
# MAGIC COMMENT "Clean records passing all 6 quality checks - ready for analytics"
# MAGIC AS
# MAGIC SELECT * EXCEPT (is_quarantined, quarantine_reason)
# MAGIC FROM STREAM dq_2_silver.sales_silver_dq_demo
# MAGIC WHERE is_quarantined = FALSE;
# MAGIC
# MAGIC --========================================================================
# MAGIC -- QUARANTINED RECORDS PATH - Invalid data for remediation
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_quarantined_demo
# MAGIC COMMENT "Invalid records with quality violations - requires remediation"
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM STREAM dq_2_silver.sales_silver_dq_demo
# MAGIC WHERE is_quarantined = TRUE;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the Official Quarantine Pattern Implementation:**
# MAGIC
# MAGIC **Zero Data Loss Architecture:**
# MAGIC - **Source table**: Contains expectations with WARN action (logs violations, retains all records)
# MAGIC - **Valid path**: Records where `is_quarantined = FALSE` (passed all quality checks)
# MAGIC - **Quarantine path**: Records where `is_quarantined = TRUE` (failed one or more checks)
# MAGIC - **Mathematical guarantee**: Total Records = Valid Records + Quarantined Records
# MAGIC
# MAGIC **Parallel Processing Benefits:**
# MAGIC - **Analytics workflow**: Consumes only validated, clean data from `sales_silver_valid_demo`
# MAGIC - **Remediation workflow**: Processes quarantined data with detailed failure information
# MAGIC - **Monitoring workflow**: Tracks quality metrics across both streams
# MAGIC - **Performance optimization**: Separate tables enable independent scaling and optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Create Gold Layer - Production-Ready Analytics Data
# MAGIC
# MAGIC The gold layer contains pre-aggregated analytics from validated silver data, providing fast query performance for business intelligence and reporting applications.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create Gold Layer Analytics Table
# MAGIC
# MAGIC 1. In your **dq_pipeline** folder, select the kebab menu and select **Create File**
# MAGIC 2. Select the language as **SQL**
# MAGIC 3. Name the file `gold_analytics.sql`
# MAGIC 4. Copy and paste the code below

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- GOLD LAYER: MATERIALIZED VIEW FOR BUSINESS ANALYTICS
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dq_3_gold.sales_analytics_demo
# MAGIC COMMENT "Business metrics aggregated from validated sales data"
# MAGIC AS
# MAGIC SELECT
# MAGIC   region,
# MAGIC   country,
# MAGIC   category,
# MAGIC   COUNT(DISTINCT order_id) AS total_orders,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   SUM(qty) AS total_quantity_sold,
# MAGIC   ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC   ROUND(AVG(total_amount), 2) AS avg_order_value,
# MAGIC   ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
# MAGIC   MIN(order_date) AS earliest_order_date,
# MAGIC   MAX(order_date) AS latest_order_date,
# MAGIC   CURRENT_TIMESTAMP() AS last_refreshed_at
# MAGIC FROM dq_2_silver.sales_silver_valid_demo
# MAGIC GROUP BY region, country, category;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review Gold Layer Design for Analytics:**
# MAGIC
# MAGIC **Why Materialized Views for Gold Layer?**
# MAGIC - **Pre-computed aggregations**: Eliminates expensive GROUP BY operations at query time
# MAGIC - **Automatic refresh**: Updates when underlying silver data changes
# MAGIC - **Delta Lake storage**: Provides ACID guarantees and time travel capabilities
# MAGIC - **BI tool optimization**: Perfect for dashboards requiring fast response times
# MAGIC
# MAGIC **Business Metrics Provided:**
# MAGIC - **Revenue analytics**: Total revenue, average order value by region and category
# MAGIC - **Customer insights**: Unique customer counts and purchasing patterns
# MAGIC - **Product performance**: Quantity sold and discount effectiveness by category
# MAGIC - **Temporal analysis**: Order date ranges for trend analysis
# MAGIC - **Data freshness**: Last refresh timestamp for monitoring
# MAGIC
# MAGIC **Analytics Workflow Support:**
# MAGIC - **BI dashboards**: Query pre-aggregated gold views for fast performance
# MAGIC - **Data science**: Access detailed, validated silver tables for modeling
# MAGIC - **Operations teams**: Monitor quarantine tables for data quality remediation
# MAGIC - **Executive reporting**: Use gold metrics for strategic decision making

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Run and Analyze Pipeline Results - First Run (Clean Baseline)
# MAGIC
# MAGIC Execute the complete pipeline and analyze the results to validate that the quality framework correctly processes clean baseline data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Execute the Complete Pipeline
# MAGIC
# MAGIC 1. **Run the pipeline** and confirm that it completes successfully with no errors
# MAGIC 2. **Observe the pipeline graph** to see data flowing through all layers: Bronze → Silver → Gold
# MAGIC 3. **Monitor execution time** and resource utilization in the pipeline UI
# MAGIC
# MAGIC **Troubleshooting:** If your pipeline fails, verify that:
# MAGIC - All SQL files are properly created in the `dq_pipeline` folder
# MAGIC - Configuration parameters are correctly set
# MAGIC - Serverless compute is selected and running
# MAGIC - Volume paths are accessible and contain data files
# MAGIC
# MAGIC **Expected Pipeline Graph:**
# MAGIC
# MAGIC <img src="./Includes/images/dq/checkpoint_run_1.png" alt="Complete Pipeline Run 1" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Validate First Run Results - Clean Baseline
# MAGIC
# MAGIC Review the pipeline execution to confirm that all data quality checks passed and the pipeline established a perfect baseline. This run demonstrates that the pipeline framework correctly ingests, transforms, and validates clean data.
# MAGIC
# MAGIC **Expected Results for Run 1 (Clean Baseline):**
# MAGIC
# MAGIC | Table | Expected Records | Quality Status | Notes |
# MAGIC |-------|------------------|----------------|-------|
# MAGIC | Bronze (sales_bronze_raw_demo) | 157 | All ingested | Raw data from sales_1.csv |
# MAGIC | Silver (sales_bronze_clean_demo) | 157 | Type-cast success | All conversions successful |
# MAGIC | Silver (sales_silver_dq_demo) | 157 | All tracked | All records with is_quarantined = FALSE |
# MAGIC | Silver (sales_silver_valid_demo) | 157 | 100% quality | Perfect baseline established |
# MAGIC | Silver (sales_silver_quarantined_demo) | 0 | No violations | Quality framework validated |
# MAGIC | Gold (sales_analytics_demo) | ~55 | Aggregated | Business metrics by region/category |
# MAGIC
# MAGIC **Validation Steps in Pipeline UI:**
# MAGIC 1. Select the **sales_silver_dq_demo** table and open the **Expectations** tab to verify "6 constraints tracked"
# MAGIC 2. Open the **Data** tab to confirm all records have `is_quarantined = FALSE`
# MAGIC 3. Check the **sales_silver_quarantined_demo** table to ensure it contains 0 records
# MAGIC 4. Verify the **sales_analytics_demo** gold table contains aggregated business metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Query and Analyze Results
# MAGIC
# MAGIC Execute the following queries to verify silver and gold layers result:
# MAGIC - Records in **sales_silver_quarantined_demo** should be zero
# MAGIC - Quality Score should be **100%**
# MAGIC - Querying on **sales_analytics_demo** MV for insight

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify quarantine is empty for Run 1 (should return 0 - perfect quality baseline)
# MAGIC SELECT 
# MAGIC   COUNT(*) AS quarantined_count,
# MAGIC   'Expected: 0 records for clean baseline' AS validation_note
# MAGIC FROM dq_2_silver.sales_silver_quarantined_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all records passed quality checks
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_records,
# MAGIC   SUM(CASE WHEN is_quarantined = FALSE THEN 1 ELSE 0 END) AS valid_records,
# MAGIC   SUM(CASE WHEN is_quarantined = TRUE THEN 1 ELSE 0 END) AS quarantined_records,
# MAGIC   ROUND(SUM(CASE WHEN is_quarantined = FALSE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS quality_score_pct
# MAGIC FROM dq_2_silver.sales_silver_dq_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Querying on sales_analytics materailized view
# MAGIC SELECT * FROM dq_3_gold.sales_analytics_demo

# COMMAND ----------

# MAGIC %md
# MAGIC **Run 1 Validation Checkpoint - Expected Results:**
# MAGIC - **Bronze Layer**: 157 records from sales_1.csv successfully ingested
# MAGIC - **Silver Transformation**: 157 records type-cast successfully with no conversion errors
# MAGIC - **Quality Validation**: 157 records pass all 6 expectations (100% quality score)
# MAGIC - **Quarantine Status**: 0 records quarantined (perfect baseline established)
# MAGIC - **Gold Analytics**: Business metrics aggregated should have 55 validated records

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Prepare and Execute Second Run - Quality Issues and Schema Evolution
# MAGIC
# MAGIC Introduce the second data file containing quality issues and new schema columns to demonstrate the pipeline's resilience and monitoring capabilities.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. Copy Additional Files for Processing
# MAGIC
# MAGIC Copy the second file from the operations location to the sales processing location to trigger incremental processing with quality issues and schema evolution.

# COMMAND ----------

ops_path = f'/Volumes/{my_catalog}/dq_1_bronze/ops'
sales_path = f'/Volumes/{my_catalog}/dq_1_bronze/sales'

# Copy files from ops location to user's source volume (now including sales_2.csv)
copy_files(copy_from=ops_path, copy_to=sales_path, n=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### H2. Verify File Availability for Processing
# MAGIC
# MAGIC Confirm that `sales_2.csv` has been successfully moved to the processing source location and is ready for pipeline consumption.

# COMMAND ----------

spark.sql(f"LIST '{sales_path}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3. Execute Pipeline Run 2 - Quality Issues and Schema Evolution
# MAGIC
# MAGIC 1. **Run the pipeline again** and confirm that it completes successfully despite quality issues
# MAGIC 2. **Observe incremental processing** as the pipeline detects and processes the new file
# MAGIC 3. **Monitor quality metrics** in the pipeline UI to see expectation violations being tracked
# MAGIC
# MAGIC **Expected Behavior:**
# MAGIC - Pipeline continues running despite quality violations (WARN action)
# MAGIC - New columns are automatically incorporated (schema evolution)
# MAGIC - Invalid records are systematically quarantined with detailed failure reasons
# MAGIC - Valid records continue to flow to analytics tables
# MAGIC
# MAGIC **Expected Pipeline Results:**
# MAGIC
# MAGIC <img src="./Includes/images/dq/checkpoint_run_2.png" alt="Pipeline Run 2 with Quality Issues" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### H4. Analyze Run 2 Results - Quality Issues Detected
# MAGIC
# MAGIC Review the pipeline execution to see how quality issues and schema evolution were successfully handled by the quarantine framework.
# MAGIC
# MAGIC **Expected Results for Run 2 (Incremental with Issues):**
# MAGIC
# MAGIC | Table | New Records | Total Records | Quality Notes |
# MAGIC |-------|-------------|---------------|---------------|
# MAGIC | Bronze (sales_bronze_raw_demo) | +60 | 217 | Schema evolved! (2 new columns populated) |
# MAGIC | Bronze (sales_bronze_clean_demo) | +60 | 217 | Type casting successful for all records |
# MAGIC | Silver (sales_silver_dq_demo) | +60 | 217 | All records tracked with quality flags |
# MAGIC | Silver (sales_silver_valid_demo) | +51 | 208 | 9 records failed expectations |
# MAGIC | Silver (sales_silver_quarantined_demo) | +9 | 9 | Invalid records captured with failure details |
# MAGIC | Gold (sales_analytics_demo) | Updated | ~60 | Metrics updated with new valid data |
# MAGIC
# MAGIC **Key Validation Points:**
# MAGIC 1. **Zero Data Loss**: Bronze (217) = Valid (208) + Quarantined (9)
# MAGIC 2. **Schema Evolution**: New columns populated automatically
# MAGIC 3. **Quality Detection**: Specific violations identified and quarantined
# MAGIC 4. **Pipeline Resilience**: Continues processing despite quality issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### H5. Comprehensive Quality Analysis
# MAGIC
# MAGIC Execute detailed analysis queries to understand the quality patterns and validate the quarantine framework effectiveness.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall quality metrics and zero data loss verification
# MAGIC WITH quality_stats AS (
# MAGIC   SELECT 
# MAGIC     (SELECT COUNT(*) FROM dq_1_bronze.sales_bronze_raw_demo) AS total_ingested,
# MAGIC     (SELECT COUNT(*) FROM dq_2_silver.sales_silver_valid_demo) AS total_valid,
# MAGIC     (SELECT COUNT(*) FROM dq_2_silver.sales_silver_quarantined_demo) AS total_quarantined
# MAGIC )
# MAGIC SELECT 
# MAGIC   total_ingested,
# MAGIC   total_valid,
# MAGIC   total_quarantined,
# MAGIC   ROUND(total_valid * 100.0 / total_ingested, 2) AS quality_score_pct,
# MAGIC   ROUND(total_quarantined * 100.0 / total_ingested, 2) AS failure_rate_pct,
# MAGIC   CASE WHEN total_ingested = total_valid + total_quarantined 
# MAGIC        THEN '✅ ZERO DATA LOSS VERIFIED' 
# MAGIC        ELSE '❌ DATA LOSS DETECTED' 
# MAGIC   END AS data_loss_check
# MAGIC FROM quality_stats;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze specific quarantined records with detailed failure reasons
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   discount_pct,
# MAGIC   order_date,
# MAGIC   shipping_cost,
# MAGIC   source_file,
# MAGIC   quarantine_reason
# MAGIC FROM dq_2_silver.sales_silver_quarantined_demo
# MAGIC ORDER BY quarantine_reason, order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **Quality Analysis Insights:**
# MAGIC - **Zero Data Loss Achieved**: Mathematical verification that no records were lost
# MAGIC - **High Quality Score**: Typically 94-96% overall quality across both files
# MAGIC - **Detailed Failure Tracking**: Each quarantined record includes specific violation reasons
# MAGIC - **Actionable Intelligence**: Operations teams can prioritize remediation based on failure types
# MAGIC - **Range Validation Effectiveness**: Successfully caught discount > 100%, negative values, and historical dates
# MAGIC - **Business Key Protection**: Identified and quarantined records with missing critical identifiers

# COMMAND ----------

# MAGIC %md
# MAGIC ### H6. Validate Schema Evolution Success
# MAGIC
# MAGIC Verify that schema evolution worked seamlessly without any pipeline code changes. Note that records from the first file will have null values for the new columns(**shipping_cost** and **order_status**), while records from the second file will have populated values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dq_2_silver.sales_silver_valid_demo

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Evolution Success Validation:**
# MAGIC - **Seamless Evolution**: Schema grew from 16 to 18 core business columns without any pipeline code changes
# MAGIC - **Backward Compatibility**: File 1 records maintain NULL values for new columns
# MAGIC - **Forward Compatibility**: File 2 records populate new columns with business values
# MAGIC - **No Pipeline Disruption**: Schema hints enabled graceful column addition
# MAGIC - **Business Value Addition**: New columns provide order status tracking and shipping cost analysis
# MAGIC - **Quality Framework Extension**: New columns automatically included in quality validations

# COMMAND ----------

# MAGIC %md
# MAGIC ### H7. Quality Performance by Source File
# MAGIC
# MAGIC Analyze quality performance differences between the clean baseline file and the file with intentional quality issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality score analysis by source file
# MAGIC SELECT 
# MAGIC   COALESCE(b.source_file, q.source_file) AS source_file,
# MAGIC   COUNT(DISTINCT COALESCE(b.order_id, q.order_id)) AS total_records,
# MAGIC   COUNT(DISTINCT s.order_id) AS valid_records,
# MAGIC   COUNT(DISTINCT q.order_id) AS quarantined_records,
# MAGIC   ROUND(COUNT(DISTINCT s.order_id) * 100.0 / 
# MAGIC         NULLIF(COUNT(DISTINCT COALESCE(b.order_id, q.order_id)), 0), 2) AS quality_score_pct,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(b.source_file, q.source_file) LIKE '%sales_1%' THEN 'Clean baseline data'
# MAGIC     WHEN COALESCE(b.source_file, q.source_file) LIKE '%sales_2%' THEN 'Quality issues + schema evolution'
# MAGIC     ELSE 'Unknown file pattern'
# MAGIC   END AS file_profile
# MAGIC FROM dq_1_bronze.sales_bronze_raw_demo b
# MAGIC FULL OUTER JOIN dq_2_silver.sales_silver_valid_demo s 
# MAGIC   ON b.order_id = s.order_id
# MAGIC FULL OUTER JOIN dq_2_silver.sales_silver_quarantined_demo q 
# MAGIC   ON COALESCE(b.order_id, s.order_id) = q.order_id
# MAGIC GROUP BY COALESCE(b.source_file, q.source_file)
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC **File-Level Quality Performance Insights:**
# MAGIC - **File 1 (Baseline)**: 100% quality score validates framework correctness
# MAGIC - **File 2 (Issues)**: 85% quality score demonstrates issue detection capability
# MAGIC - **Overall Performance**: 95.85% combined quality score shows enterprise-grade data quality
# MAGIC - **Framework Validation**: Clean data passes completely, problematic data is systematically identified
# MAGIC - **Production Readiness**: Quality framework successfully handles both perfect and imperfect data scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Summary and Key Takeaways
# MAGIC
# MAGIC **Data Quality Management:**
# MAGIC - Successfully implemented 6 comprehensive data quality expectations covering business keys, numeric ranges, and temporal validations
# MAGIC - Achieved 100% quality score on clean baseline data (Run 1: 157/157 records)
# MAGIC - Detected and quarantined 9 quality violations in problematic data (Run 2: 51/60 records passed)
# MAGIC - Overall pipeline quality score: 95.85% (208/217 total records processed)
# MAGIC
# MAGIC **Schema Evolution:**
# MAGIC - Seamlessly handled schema evolution from 16 to 18 core business columns from source without pipeline code changes
# MAGIC - Bronze layer STRING strategy enabled backward compatibility and forward flexibility
# MAGIC - New business columns (**order_status, shipping_cost**) automatically integrated
# MAGIC
# MAGIC **Zero Data Loss Quarantine:**
# MAGIC - Implemented official Databricks inverse logic pattern for quarantine processing
# MAGIC - Achieved mathematical zero data loss: Bronze Records = Valid Sales Records + Quarantine Sales Records
# MAGIC - Provided detailed failure analysis and remediation insights for each quarantined record
# MAGIC
# MAGIC ### Production Readiness
# MAGIC
# MAGIC This demonstration showcased enterprise-grade patterns that provide:
# MAGIC - **Robust data quality controls** that catch anomalies before they reach analytics
# MAGIC - **Flexible schema handling** that adapts to evolving business requirements
# MAGIC - **Complete data lineage** with zero loss and full audit capabilities
# MAGIC - **Actionable quality insights** for continuous data improvement

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>