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
# MAGIC # Advanced Techniques with Spark Declarative Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course provides a hands-on exploration of **Lakeflow Spark Declarative Pipelines (SDP)** —Databricks modern framework for building production-grade streaming pipelines. You will learn advanced pipeline design patterns, data quality enforcement, and cross-platform integration essential for real-world lakehouse engineering.
# MAGIC
# MAGIC The course starts with **Multi-Flow pipelines**, ingesting from multiple sources into a single target table using explicit flows. You will then explore **Liquid Clustering** for adaptive data layout optimization and **Data Quality Expectations** for enforcing business rules at every pipeline stage.
# MAGIC
# MAGIC Next, you will learn the **Multiplex Streaming pattern** for processing mixed-schema event streams, connecting with **Delta Sinks** and **Iceberg Reads via Delta UniForm** for cross-platform access to live streaming data.
# MAGIC
# MAGIC The course also covers **Change Data Capture (CDC)** —including **SCD Type 1** (current state) and **SCD Type 2** (full history tracking)—automated with `AUTO CDC INTO` in Lakeflow SDP.
# MAGIC
# MAGIC Finally, you will implement advanced data quality techniques, including range-based constraints, NULL-tolerant expressions, schema evolution handling, and the **Quarantine Pattern** for zero-data-loss pipelines with full audit trails.
# MAGIC
# MAGIC Through lectures and hands-on demos, you will:
# MAGIC
# MAGIC - Build **multi-flow pipelines** ingesting data from multiple retail subsidiaries into a single Bronze streaming table
# MAGIC - Apply **Liquid Clustering** and **Data Quality Expectations** to Silver and Gold tables
# MAGIC - Implement the **Multiplex pattern** with Delta Sinks and Iceberg UniForm for cross-platform access
# MAGIC - Automate **SCD Type 2 history tracking** using `AUTO CDC INTO`
# MAGIC - Design **quarantine-based quality pipelines** that preserve invalid records for audit and remediation
# MAGIC
# MAGIC By the end of this course, you will be equipped to design and build enterprise-grade streaming pipelines on Databricks that are reliable, observable, and production-ready.
# MAGIC
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Build **multi-flow Bronze streaming tables** consolidating CSV and JSON data from multiple sources using explicit **`CREATE FLOW`** definitions
# MAGIC
# MAGIC - Create **incremental Gold materialized views** and apply **Unity Catalog tags** to Bronze, Silver, and Gold objects for governance and discoverability
# MAGIC
# MAGIC - Implement the **Multiplex pattern** to ingest a mixed-schema event stream into a single Bronze table using the **`VARIANT`** data type and fan out into domain-specific Silver tables
# MAGIC
# MAGIC - Build a **Delta Sink** using **`dp.create_sink`** and **`@dp.append_flow`**, then enable **Iceberg reads via Delta UniForm** for cross-platform analytics access
# MAGIC
# MAGIC - Automate a **CDC pipeline** using **`AUTO CDC INTO`** with **SCD Type 2** to track customer INSERT, UPDATE, and DELETE events with full history, surfaced via Gold materialized views
# MAGIC
# MAGIC - Implement **advanced data quality expectations** covering NOT NULL, numeric range, and NULL-tolerant constraints, and handle **schema evolution** in Bronze using **`schemaHints`** and **`_rescued_data`**
# MAGIC
# MAGIC - Apply the **Quarantine Pattern** using **inverse logic** to route invalid records without data loss, and monitor per-constraint violation metrics in the **Pipelines UI**

