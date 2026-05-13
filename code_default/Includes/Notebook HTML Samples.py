# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Information
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC In this training, we use <strong>Lakebase Provisioned</strong>. 
# MAGIC For future use, we recommend <strong>Lakebase Autoscaling</strong>, which supports additional features beyond the core concepts covered here.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Warning
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC PostgreSQL does not support cross-database queries, so you can only query the database selected as your default.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Error</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     This is an error message. Use this style to highlight critical issues, errors, or anti-patterns that should be avoided.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #4caf50;
# MAGIC   background: #e8f5e9;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Success</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     This is an error message. Use this style to highlight critical issues, errors, or anti-patterns that should be avoided.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #7b1fa2;
# MAGIC   background: #f3e5f5;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.1em;">Notes</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     This is a note. Use this style to highlight important remarks or observations that the reader should keep in mind.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC