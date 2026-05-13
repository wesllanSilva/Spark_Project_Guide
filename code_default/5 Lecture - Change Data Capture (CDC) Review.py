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
# MAGIC # Lecture - Change Data Capture (CDC) Review
# MAGIC
# MAGIC This lecture provides a comprehensive review of Change Data Capture (CDC) concepts and implementation patterns in the Lakehouse. You'll explore how CDC enables real-time data synchronization and learn about different approaches to handling changing data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Define** Change Data Capture (CDC) and explain its role in data synchronization
# MAGIC 2. **Distinguish** between SCD Type 1 and SCD Type 2 patterns and their use cases
# MAGIC 3. **Analyze** how SCD Type 1 overwrites existing data while SCD Type 2 preserves history
# MAGIC 4. **Identify** when to use each SCD type based on business requirements
# MAGIC 5. **Recognize** how `AUTO CDC INTO` simplifies CDC implementation in Lakeflow Spark Declarative Pipelines(SDP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. What is Change Data Capture?

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="./Includes/images/cdc_lecture/01-cdcoverview-review.png" alt="CDC Overview" width="1100">
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC Let's review **Change Data Capture (CDC)**, a foundational concept for keeping data synchronized across systems.
# MAGIC
# MAGIC #### CDC Definition and Purpose
# MAGIC
# MAGIC Change Data Capture is a technique used to track and capture changes in data sources like databases, Lakehouses, or data warehouses, and then apply those changes to a target table to ensure it reflects the latest state of the source.
# MAGIC
# MAGIC #### Slowly Changing Dimensions (SCDs)
# MAGIC
# MAGIC CDC is closely tied to the concept of **Slowly Changing Dimensions (SCDs)**, which describe how historical data changes are handled in your target system.
# MAGIC
# MAGIC We'll focus on two main types:
# MAGIC - **SCD Type 1** - Overwrites existing data with new values (no history tracking)  
# MAGIC - **SCD Type 2** - Preserves history by storing previous versions of records
# MAGIC
# MAGIC #### Real-World Example
# MAGIC
# MAGIC Imagine a **customer** table where new customers are added, existing customer information is updated, or some customers are deleted. CDC ensures those changes flow into the target table using either SCD Type 1 or Type 2 logic, keeping the target table continuously up to date.
# MAGIC
# MAGIC **Think about it:** What types of data changes occur frequently in your organization that would benefit from automated CDC processing?
# MAGIC
# MAGIC #### Documentation Resources
# MAGIC - [What is change data capture (CDC)?](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture)
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Slowly Changing Dimensions (SCD) Type 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. SCD Type 1 - Overview
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/02-scd-type-1-02-review-slide.png" alt="SCD Type 1 Example" width="1100">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC
# MAGIC SCD Type 1 updates the **target table** by **overwriting existing rows** with the latest values. No version history is maintained - only the current state matters.
# MAGIC
# MAGIC #### Key Characteristics:
# MAGIC - **Updates:** When a record is updated by its key(s), the existing row is replaced with new values
# MAGIC - **Deletes:** When a record is deleted by its key(s), it is removed from the target table  
# MAGIC - **Current State Only:** Only the most recent version of each record is stored
# MAGIC - **No History:** Previous changes and historical versions are not retained
# MAGIC
# MAGIC #### Our Scenario Setup
# MAGIC
# MAGIC **Target Table (customers)** - Current customer data:
# MAGIC
# MAGIC | CustomerID | Name   | Address | ProcessDate |
# MAGIC |------------|--------|---------|-------------|
# MAGIC | 1 | Peter | 1 Blue Rd. | 5/1/2025 |
# MAGIC | 2 | Samarth | 22 Front St | 5/1/2025 |
# MAGIC
# MAGIC **Source Updates** - Incoming changes:
# MAGIC
# MAGIC | Change Type | Details |
# MAGIC |-------------|---------|
# MAGIC | **Update** | Peter has two address updates (5/15 and 5/20) for `customer_id = 1` |
# MAGIC | **Delete** | Samarth requests account removal (`customer_id = 2`) |
# MAGIC | **Insert** | New customer Kostas joins (`customer_id = 3`) |
# MAGIC
# MAGIC **Goal:** Apply updates so the target table reflects the latest state of all customers.
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. SCD Type 1 - Implementation Example
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/02-scd-type-1-01-review-slide.png" alt="SCD Type 1 Overview" width="1100">
# MAGIC
# MAGIC > **NOTE:** When ingesting a `DELETE` row using `AUTO CDC INTO`, non key columns may contain `NULL` values. The required key columns must always be populated, as they are used to identify the row to delete.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC When we apply **SCD Type 1**, the target table is updated with the latest customer information using the **CustomerID** (key column) and **ProcessDate** (sequence column) to determine the most recent changes.
# MAGIC
# MAGIC #### Processing Logic:
# MAGIC
# MAGIC 1. **Peter (CustomerID 1):** 
# MAGIC    - Multiple address updates exist (5/15 and 5/20)
# MAGIC    - Only the latest update (5/20) with address *123 Main St.* is applied
# MAGIC    - Previous address history is lost
# MAGIC
# MAGIC 2. **Samarth (CustomerID 2):** 
# MAGIC    - Marked for deletion
# MAGIC    - Entire row is removed from the target table
# MAGIC    - No trace of the customer remains
# MAGIC
# MAGIC 3. **Kostas (CustomerID 3):** 
# MAGIC    - New customer record
# MAGIC    - Inserted as a new row in the target table
# MAGIC
# MAGIC #### Final Result:
# MAGIC The customers table contains only the most current snapshot of active customers. This approach is ideal when:
# MAGIC - Historical data is not required for business operations
# MAGIC - Storage efficiency is prioritized
# MAGIC - Regulatory compliance doesn't require audit trails
# MAGIC
# MAGIC **Use SCD Type 1 when:** You only need the most up-to-date and accurate information without historical context.
# MAGIC
# MAGIC #### Documentation
# MAGIC - [Use SCD Type 1 to keep only the latest data](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture#step-2-use-scd-type-1-to-keep-only-the-latest-data)
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Slowly Changing Dimensions (SCD) Type 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. SCD Type 2 - Scenario
# MAGIC
# MAGIC <img src="./Includes/images/cdc_lecture/03-auto-cdc-examplescenario.png" alt="SCD Type 2 - Scenario" width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. SCD Type 2 - Implementation
# MAGIC <img src="./Includes/images/cdc_lecture/02-scd-type-2-01-review-slide.png" alt="SCD Type 2" width="1100">
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC **Slowly Changing Dimensions Type 2 (SCD Type 2)** introduces **historical tracking and versioning** of records, preserving a complete audit trail of all changes over time.
# MAGIC
# MAGIC #### Core Principles:
# MAGIC
# MAGIC When a record changes:
# MAGIC - **Historical Preservation:** The old record is preserved with metadata columns showing its validity period
# MAGIC - **New Version Creation:** A new record is inserted with the updated information
# MAGIC - **Soft Deletes:** Deleted records remain in the table but are flagged as inactive
# MAGIC
# MAGIC #### Metadata Columns Added:
# MAGIC - **__START_AT** - Timestamp when the row became active  
# MAGIC - **__END_AT** - Timestamp when the row became inactive  
# MAGIC   - `NULL` **value** = currently active record
# MAGIC   - **Non** `NULL` **value** = inactive/historical record
# MAGIC
# MAGIC #### Detailed Example Breakdown:
# MAGIC
# MAGIC **Customer ID 1 - Peter:**  
# MAGIC - **Two records exist** for Peter in the final table
# MAGIC - **Active record:** Shows Peter's current address with `__END_AT = NULL`
# MAGIC - **Historical record:** Preserves his previous address with `__END_AT` populated
# MAGIC
# MAGIC **Customer ID 2 - Samarth:**  
# MAGIC - **Account deletion** processed as a soft delete
# MAGIC - **Original record remains** but `__END_AT` is populated with deletion timestamp
# MAGIC - **No new record created** since this was a deletion operation
# MAGIC
# MAGIC **Customer ID 3 - Kostas:**  
# MAGIC - **New customer insertion** creates active record
# MAGIC - **__START_AT** marks when he joined, **__END_AT** remains NULL
# MAGIC
# MAGIC #### Business Value:
# MAGIC SCD Type 2 enables complete historical analysis, allowing you to:
# MAGIC - Track how customer attributes evolved over time
# MAGIC - Perform point-in-time analysis for any historical date
# MAGIC - Maintain compliance with audit requirements
# MAGIC - Support advanced analytics on changing dimensions
# MAGIC
# MAGIC **Use SCD Type 2 when:** Historical data tracking is essential for business intelligence, compliance, or analytical requirements.
# MAGIC
# MAGIC #### Documentation
# MAGIC - [Use SCD Type 2 to keep historical data](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture#step-3-use-scd-type-2-to-keep-historical-data)
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Implementing CDC with `AUTO CDC INTO` in Spark Declarative Pipelines (SCD Type 1 Example)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="./Includes/images/cdc_lecture/03-auto-cdc-example.png" alt="AUTO CDC Example" width="1100">
# MAGIC
# MAGIC > **NOTE:** The `AUTO CDC INTO` key can be defined using a single column or a composite key. A composite key simply means using multiple columns together to uniquely identify a record, for example (`CustomerID`, `OrderID`).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC Now that we've covered CDC concepts and both SCD patterns, let's explore how **Lakeflow Spark Declarative Pipelines** simplifies CDC implementation with the `AUTO CDC INTO` statement.
# MAGIC
# MAGIC **NOTE:** The `AUTO CDC` APIs were previously known as `APPLY CHANGES INTO`, but the syntax and functionality remain identical.
# MAGIC
# MAGIC #### AUTO CDC INTO Syntax Breakdown
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers;
# MAGIC
# MAGIC CREATE FLOW scd_type_1_flow AS
# MAGIC AUTO CDC INTO customers 
# MAGIC  FROM STREAM updates
# MAGIC  KEYS (CustomerID)                              
# MAGIC  APPLY AS DELETE WHEN operation = "DELETE"     
# MAGIC  SEQUENCE BY ProcessDate                 
# MAGIC  COLUMNS * EXCEPT (operation)  
# MAGIC  STORED AS SCD TYPE 1;
# MAGIC ```
# MAGIC
# MAGIC - **`AUTO CDC INTO customers`** - Specifies the target table for CDC operations
# MAGIC - **`FROM STREAM updates`** - Defines the source stream containing CDC events
# MAGIC - **`KEYS (CustomerID)`** - Establishes unique key(s) for matching source and target records
# MAGIC - **`APPLY AS DELETE WHEN operation = "DELETE"`** - Defines deletion logic based on operation column
# MAGIC - **`SEQUENCE BY ProcessDate`** - Ensures events are processed in chronological order
# MAGIC - **`COLUMNS * EXCEPT (operation)`** - Includes all columns except operational metadata
# MAGIC - **`STORED AS SCD TYPE 1`** - Specifies SCD Type 1 pattern (default is SCD Type 1 default)
# MAGIC
# MAGIC #### Key Advantages
# MAGIC
# MAGIC `AUTO CDC INTO` provides significant benefits over traditional approaches:
# MAGIC
# MAGIC 1. **Simplified Implementation:** Eliminates complex `MERGE INTO` logic
# MAGIC 2. **Automatic Ordering:** Handles event sequencing automatically
# MAGIC 3. **Built-in SCD Support:** Native support for both Type 1 and Type 2 patterns
# MAGIC 4. **Streaming Integration:** Works seamlessly with both streaming and batch sources
# MAGIC 5. **Error Handling:** Includes robust error handling and recovery mechanisms
# MAGIC
# MAGIC **Reflection Question:** How might `AUTO CDC INTO` simplify your current data pipeline maintenance compared to custom merge logic?
# MAGIC
# MAGIC </details>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Documentation and Next Steps
# MAGIC
# MAGIC #### Key Resources:
# MAGIC - [The AUTO CDC APIs: Simplify change data capture with Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/cdc)
# MAGIC - [AUTO CDC INTO (Lakeflow Spark Declarative Pipelines)](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-apply-changes-into)
# MAGIC
# MAGIC ### Coming Up Next: Automating SCD Type 2 with AUTO CDC in Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC In the next notebook, you will apply what you just learned by building a pipeline that implements CDC with SCD Type 2 using `AUTO CDC` in Lakeflow Spark Declarative Pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Covered:
# MAGIC
# MAGIC 1. **Change Data Capture (CDC)** enables automated synchronization of data changes between systems
# MAGIC 2. **SCD Type 1** overwrites existing data, maintaining only current state (no history)
# MAGIC 3. **SCD Type 2** preserves complete historical versions using metadata columns
# MAGIC 4. **`AUTO CDC INTO`** provides declarative, simplified CDC implementation in Lakeflow pipelines
# MAGIC
# MAGIC ### Decision Framework:
# MAGIC
# MAGIC **Choose SCD Type 1 when:**
# MAGIC - Only current data state is needed
# MAGIC - Storage efficiency is prioritized
# MAGIC - Historical tracking is not required
# MAGIC
# MAGIC **Choose SCD Type 2 when:**
# MAGIC - Historical analysis is essential
# MAGIC - Audit trails are required for compliance
# MAGIC - Point-in-time reporting is needed

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>