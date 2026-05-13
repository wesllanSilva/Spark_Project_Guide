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
# MAGIC # Lecture - Summary and Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. What You Accomplished
# MAGIC
# MAGIC Congratulations on completing **Advanced Techniques with Spark Declarative Pipelines**! Throughout this course, you built real, production-relevant pipelines and applied patterns that directly transfer to enterprise data engineering work.
# MAGIC
# MAGIC  Area | What You Can Now Do |
# MAGIC ------|---------------------|
# MAGIC  **Multi-Flow Pipelines** | Design pipelines that merge multiple source streams into a single Bronze target using explicit `CREATE FLOW` and `@dp.append_flow` definitions. |
# MAGIC  **Liquid Clustering** | Configure `CLUSTER BY AUTO` and manual clustering keys on streaming tables, and understand when each approach is appropriate. |
# MAGIC  **Data Quality Expectations** | Apply WARN, DROP ROW, and FAIL UPDATE violation modes, and build layered quality strategies across Bronze, Silver, and Gold layers. |
# MAGIC  **Multiplex Pattern** | Ingest mixed-schema event streams through a single pipeline and fan out to per-entity Silver tables. |
# MAGIC  **Delta Sinks and Iceberg Reads** | Write streaming output to external Delta tables using `dp.create_sink`, and enable Delta UniForm for cross-platform Iceberg access. |
# MAGIC  **Change Data Capture** | Distinguish SCD Type 1 vs. Type 2, and implement CDC pipelines using `AUTO CDC INTO` for both patterns. |
# MAGIC  **Advanced Data Quality** | Write range-based and NULL-tolerant constraints, handle schema evolution in Bronze, and implement the Quarantine Pattern for zero-data-loss pipelines. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Additional Resources
# MAGIC
# MAGIC Explore the following resources to deepen your understanding and stay current with platform updates.
# MAGIC
# MAGIC ### B1. Core Documentation
# MAGIC
# MAGIC - [Lakeflow Spark Declarative Pipelines Overview](https://docs.databricks.com/aws/en/dlt/index.html) — Comprehensive reference for pipeline concepts, syntax, and configuration options.
# MAGIC
# MAGIC - [AUTO CDC APIs — Simplify Change Data Capture](https://docs.databricks.com/aws/en/ldp/cdc) — Full reference for `AUTO CDC INTO`, including SCD Type 1 and Type 2 configuration options.
# MAGIC
# MAGIC - [Delta UniForm Documentation](https://docs.databricks.com/aws/en/delta/uniform.html) — Learn about enabling Iceberg reads on Delta tables, supported configurations, and known limitations.
# MAGIC
# MAGIC - [Data Quality Expectations in Pipelines](https://docs.databricks.com/aws/en/dlt/expectations.html) — Reference for defining constraints, violation modes, and monitoring quality metrics in the pipeline UI.
# MAGIC
# MAGIC - [Liquid Clustering for Delta Tables](https://docs.databricks.com/aws/en/delta/clustering.html) — Guidance on configuring, monitoring, and optimizing Liquid Clustering.
# MAGIC
# MAGIC ### B2. Blogs and Announcements
# MAGIC
# MAGIC - [Databricks Release Notes](https://docs.databricks.com/aws/en/release-notes/) — Stay informed about new features, improvements, and platform updates.
# MAGIC
# MAGIC - [Announcing Lakeflow Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines) — Overview of SDP's evolution and production-ready features.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### C1. Continue Your Learning
# MAGIC
# MAGIC Expand your data and AI knowledge through Databricks self-paced and instructor-led training. These courses help you deepen your technical skills and gain hands-on experience with the Databricks platform.
# MAGIC
# MAGIC Visit the [Databricks Training and Certification](https://www.databricks.com/learn/training/home)
# MAGIC
# MAGIC ### C2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit the [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>