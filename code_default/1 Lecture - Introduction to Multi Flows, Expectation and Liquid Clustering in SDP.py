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
# MAGIC # Lecture - Introduction to Multi Flows, Expectation and Liquid Clustering in SDP
# MAGIC
# MAGIC This lecture introduces **Flows** in Spark Declarative Pipelines as the primary mechanism for loading and processing data incrementally — including how to use multiple flows to write to a single target table. It then explores how **Liquid Clustering** and **Data Quality Expectations** are applied specifically within a declarative pipeline context.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - Explain what a **flow** is and how it relates to a streaming table or materialized view
# MAGIC - Describe the difference between a **default flow** and an **explicit flow**
# MAGIC - Apply the **multi-flow pattern** to write from multiple sources into a single target table
# MAGIC - Explain why **multi-flow is preferred over UNION** for incremental pipelines
# MAGIC - Recall how **Liquid Clustering** and **Data Quality Expectations** are configured in Spark Declarative Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Understanding Flows in Spark Declarative Pipelines

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. What is Flow(Recap...)
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Flow = Query + Target
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     A["<b>Query</b><br/>SQL · Filters · Joins"] --> C["<b>FLOW</b><br/>Execution Unit"]
# MAGIC     B["<b>Target</b><br/>Streaming Table or MV"] --> C
# MAGIC     C --> D["<b>Output</b><br/>Processed Data"]
# MAGIC     style A fill:#E3F2FD,stroke:#1976d2,stroke-width:2px,color:#000
# MAGIC     style B fill:#E8F5E9,stroke:#388E3C,stroke-width:2px,color:#000
# MAGIC     style C fill:#FFF3E0,stroke:#F57C00,stroke-width:3px,color:#000
# MAGIC     style D fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#000
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Flow Processing Modes
# MAGIC
# MAGIC <div style="display:flex;gap:48px;margin:24px 0;align-items:flex-start;">
# MAGIC
# MAGIC   <!-- Incremental -->
# MAGIC   <div style="flex:1;max-width:480px;">
# MAGIC     <div style="font-weight:600;color:#1B5E20;font-size:20px;margin-bottom:4px;">
# MAGIC       Incremental Mode
# MAGIC     </div>
# MAGIC     <div class="mermaid">
# MAGIC flowchart TB
# MAGIC     I1[Source Data] --> I2[<b>Read Checkpoint</b>]
# MAGIC     I2 --> I3[New Records Only]
# MAGIC     I3 --> I4[Write to Target]
# MAGIC     I4 --> I5[Update Checkpoint]
# MAGIC     I5 -.->|Next Run| I2
# MAGIC     style I1 fill:#C8E6C9,stroke:#2E7D32,color:#000
# MAGIC     style I2 fill:#A5D6A7,stroke:#1B5E20,color:#000
# MAGIC     style I3 fill:#81C784,stroke:#1B5E20,color:#000
# MAGIC     style I4 fill:#66BB6A,stroke:#1B5E20,color:#000
# MAGIC     style I5 fill:#66BB6A,stroke:#1B5E20,color:#000
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Full Refresh -->
# MAGIC   <div style="flex:1;max-width:480px;">
# MAGIC     <div style="font-weight:600;color:#0D47A1;font-size:20px;margin-bottom:4px;">
# MAGIC       Full Refresh Mode
# MAGIC     </div>
# MAGIC     <div class="mermaid">
# MAGIC flowchart TB
# MAGIC     F1[Source Data] --> F2[<b>Discard Checkpoint</b>]
# MAGIC     F2 --> F3[Read ALL Records]
# MAGIC     F3 --> F4[Process Everything]
# MAGIC     F4 --> F5[Overwrite Target]
# MAGIC     style F1 fill:#BBDEFB,stroke:#1565C0,color:#000
# MAGIC     style F2 fill:#90CAF9,stroke:#0D47A1,color:#000
# MAGIC     style F3 fill:#64B5F6,stroke:#0D47A1,color:#000
# MAGIC     style F4 fill:#42A5F5,stroke:#0D47A1,color:#000
# MAGIC     style F5 fill:#1E88E5,stroke:#0D47A1,color:#fff
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default", themeVariables: {edgeLabelBackground: "#FFFFFF"}});
# MAGIC </script>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Click For Additional Notes
# MAGIC <details> 
# MAGIC
# MAGIC ### Flow 
# MAGIC
# MAGIC A **flow** is the basic unit of work in a Spark Declarative Pipeline. Each flow independently reads, processes, and writes data, enabling modular and reliable pipeline updates.
# MAGIC
# MAGIC ##### Query
# MAGIC
# MAGIC The **Query** defines *what data to read* and how to transform it — this is your SQL or DataFrame logic, including filtering, joining, and aggregating.
# MAGIC
# MAGIC ##### Target
# MAGIC The **Target** defines *where the results land* — either a **Streaming Table** for incremental ingestion or a **Materialized View** for computed or aggregated results.
# MAGIC
# MAGIC ---
# MAGIC ### Processing Modes
# MAGIC ##### Incremental Mode
# MAGIC
# MAGIC Processes **only new records** since the last pipeline run using a **checkpoint** stored per flow. This is highly efficient for large datasets and streaming sources — on restart, the flow resumes exactly where it left off without reprocessing historical data.
# MAGIC
# MAGIC **When to use:** Ongoing ingestion from streaming sources like Kafka, Auto Loader, or any append-only table.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##### Full Refresh Mode
# MAGIC
# MAGIC Reprocesses **all records from the source from scratch** — the existing checkpoint is discarded and the target table is fully overwritten. This is more expensive but necessary when business logic changes require recomputing historical data.
# MAGIC
# MAGIC **When to use:** After modifying transformation logic, fixing data quality issues, or when a clean slate is required.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Flow Lifecycle and Checkpoint Management
# MAGIC
# MAGIC <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;">
# MAGIC   <div style="font-size:15px;color:#555;line-height:1.6;">
# MAGIC     Each flow maintains its own <strong>checkpoint</strong> that tracks exactly how far it has read from its source — enabling reliable restarts, independent progress, and failure isolation.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### How a Flow Checkpoint Works
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     A[Flow Starts] --> B[Read Checkpoint]
# MAGIC     B --> C{Checkpoint Exists?}
# MAGIC     C -->|Yes| D[Resume from Last Position]
# MAGIC     C -->|No| E[Start from Beginning]
# MAGIC     D --> F[Process New Records]
# MAGIC     E --> F
# MAGIC     F --> G[Write to Target]
# MAGIC     G --> H[Update Checkpoint]
# MAGIC     H -.->|Next Run| B
# MAGIC     style A fill:#E3F2FD,stroke:#1976d2,stroke-width:2px,color:#000
# MAGIC     style B fill:#E8F5E9,stroke:#388E3C,stroke-width:2px,color:#000
# MAGIC     style C fill:#FFF3E0,stroke:#F57C00,stroke-width:2px,color:#000
# MAGIC     style D fill:#E8F5E9,stroke:#388E3C,color:#000
# MAGIC     style E fill:#FFEBEE,stroke:#C62828,color:#000
# MAGIC     style F fill:#F3E5F5,stroke:#7B1FA2,color:#000
# MAGIC     style G fill:#E8F5E9,stroke:#388E3C,color:#000
# MAGIC     style H fill:#E3F2FD,stroke:#1976d2,color:#000
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Four Key Rules of Checkpoint Management
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="background:#F3F4F6;border-left:4px solid #607D8B;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#111;margin-bottom:6px;">Flow Name = Checkpoint Identity</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">The flow name directly maps to its checkpoint location on storage. Each flow's progress is tracked and stored independently under its name.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#FFF3E0;border-left:4px solid #F57C00;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#E65100;margin-bottom:6px;">Renaming Resets Progress</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">Changing a flow's name creates a brand new checkpoint and abandons the old one. The flow reprocesses data from the beginning as if it has never run before.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#E8F5E9;border-left:4px solid #388E3C;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#1B5E20;margin-bottom:6px;">Independent Progression</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">Each flow processes data at its own pace. A slow or lagging flow has no effect on other flows within the same pipeline — they advance independently.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#E3F2FD;border-left:4px solid #1976d2;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#0D47A1;margin-bottom:6px;">Failure Isolation</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">If one flow fails, remaining flows continue processing normally. Failures are scoped to the individual flow — not the entire pipeline.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#FFF9C4;border-left:4px solid #F9A825;padding:14px 18px;margin:20px 0;border-radius:4px;">
# MAGIC   <div style="display:flex;align-items:start;gap:10px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div style="font-size:16px;color:#444;line-height:1.7;">
# MAGIC       <strong>Summary:</strong> A flow's checkpoint acts like a bookmark, tracking progress independently. Renaming a flow resets its checkpoint, starting from the beginning and discarding previous progress.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Default Flow vs. Explicit Flow
# MAGIC
# MAGIC Most of the time, a flow is created **automatically and implicitly** when you define a streaming table or materialized view. This is the **default flow** — it shares the name of its target table.
# MAGIC
# MAGIC You can also create **explicit flows** separately from the table definition. This is required when you need to write to an existing table from a new source, or when multiple sources need to converge into one target.
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:16px 0;">
# MAGIC <div style="flex:1;background:#F3F4F6;border:1px solid #E0E0E0;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#333;font-size:16px;margin-bottom:1px;">Default Flow (implicit)</div>
# MAGIC <div style="font-size:16px;color:#555;margin-bottom:8px;">Table and flow created in one step. The flow takes the name of the table.</div>
# MAGIC <pre style="background:#fff;border:1px solid #ddd;border-radius:4px;padding:10px;font-size:15px;margin:0;">
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE target_table
# MAGIC AS SELECT *
# MAGIC FROM STREAM source_table;
# MAGIC </pre>
# MAGIC </div>
# MAGIC <div style="flex:1;background:#F3F4F6;border:1px solid #E0E0E0;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#333;font-size:16px;margin-bottom:10px;">Explicit Flow (separate definition)</div>
# MAGIC <div style="font-size:16px;color:#555;margin-bottom:8px;">Table defined first. Flow defined separately and attached to the target by name.</div>
# MAGIC <pre style="background:#fff;border:1px solid #ddd;border-radius:4px;padding:10px;font-size:15px;margin:0;">
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE target_table;
# MAGIC
# MAGIC CREATE FLOW my_flow
# MAGIC AS INSERT INTO target_table BY NAME
# MAGIC SELECT * FROM STREAM source_table;
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var code = block.textContent.trim();
# MAGIC         var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC         block.innerHTML = 
# MAGIC             '<div style="position:relative;margin:16px 0;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:15px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:16px;"></code></pre>' +
# MAGIC             '</div>';
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>
# MAGIC </pre>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. The Multi-Flow Pattern

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Writing Multiple Sources into One Target
# MAGIC
# MAGIC The power of explicit flows is that you can define **multiple flows targeting the same table**. Each flow independently reads from a different source and appends its records to the shared target.
# MAGIC
# MAGIC This is the **multi-flow pattern** — it is the recommended approach when you need to consolidate data from several sources into a single unified table.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     S1("<div style='text-align:center;font-size:15px;'>
# MAGIC <img src='./Includes/images/icons/csv_file_icon.png' width='28' style='display:block;margin:0 auto 4px;'/>
# MAGIC <b>Source A</b><br/>CSV file
# MAGIC </div>")
# MAGIC     S2("<div style='text-align:center;font-size:15px;'>
# MAGIC <img src='./Includes/images/icons/json_file_icon.png' width='28' style='display:block;margin:0 auto 4px;'/>
# MAGIC <b>Source B</b><br/>JSON file
# MAGIC </div>")
# MAGIC     S3("<div style='text-align:center;font-size:15px;'>
# MAGIC <img src='./Includes/images/icons/csv_file_icon.png' width='28' style='display:block;margin:0 auto 4px;'/>
# MAGIC <b>Source C</b><br/>CSV file
# MAGIC </div>")
# MAGIC     F1["Flow A<br/>source_a_flow"]
# MAGIC     F2["Flow B<br/>source_b_flow"]
# MAGIC     F3["Flow C<br/>source_c_flow"]
# MAGIC     T[" Single Target<br/>Streaming Table<br/>INSERT INTO ... BY NAME"]
# MAGIC     S1 -----> F1
# MAGIC     S2 -----> F2
# MAGIC     S3 -----> F3
# MAGIC     F1 -----> T
# MAGIC     F2 -----> T
# MAGIC     F3 -----> T
# MAGIC     style F1 fill:#1976D2,color:#fff,stroke:#1565C0
# MAGIC     style F2 fill:#1976D2,color:#fff,stroke:#1565C0
# MAGIC     style F3 fill:#1976D2,color:#fff,stroke:#1565C0
# MAGIC     style T fill:#FF5722,color:#fff,stroke:#E64A19
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>
# MAGIC
# MAGIC <div style="background:#E8F5E9;border-left:4px solid #388E3C;padding:14px 18px;margin:20px 0;border-radius:4px;">
# MAGIC   <div style="font-size:16px;color:#1B5E20;line-height:1.7;">
# MAGIC     💡 Each flow has its <strong>own independent checkpoint</strong> — if a new source is added later, only that flow needs to backfill. The other flows are completely unaffected.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Multi-Flow vs. UNION — Why It Matters
# MAGIC
# MAGIC A common alternative to multi-flow is combining sources with a `UNION` clause inside a single streaming table definition. For incremental pipelines, this creates critical limitations.
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:16px 0;">
# MAGIC <div style="flex:1;background:#FFEBEE;border:1px solid #FFCDD2;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#C62828;font-size:16px;margin-bottom:8px;">❌ UNION Approach</div>
# MAGIC <ul style="font-size:16px;color:#333;margin:0;padding-left:16px;">
# MAGIC <li>All sources share a <strong>single checkpoint</strong></li>
# MAGIC <li>Adding a new source <strong>requires a full refresh</strong> to reprocess everything</li>
# MAGIC <li>A failure in one source can block all others</li>
# MAGIC <li>Harder to track lineage per source</li>
# MAGIC <li>Complex error handling across multiple data sources</li>
# MAGIC <li>Limited scalability as source count increases</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC <div style="flex:1;background:#E8F5E9;border:1px solid #C8E6C9;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:8px;">✅ Multi-Flow Approach</div>
# MAGIC <ul style="font-size:16px;color:#333;margin:0;padding-left:16px;">
# MAGIC <li>Each flow has its <strong>own independent checkpoint</strong></li>
# MAGIC <li>New sources can be added <strong>without a full refresh</strong></li>
# MAGIC <li>Flows are isolated — one source failure does not affect others</li>
# MAGIC <li>Clear per-source lineage and monitoring</li>
# MAGIC <li>Independent error handling and recovery per source</li>
# MAGIC <li>Better scalability and maintainability</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Data Quality Expectations
# MAGIC
# MAGIC Data quality means making sure your pipeline only stores valid and reliable records. In Spark Declarative Pipelines, you use **expectations** to set simple rules directly on streaming tables. If a record breaks a rule, you can choose to warn, drop, or fail the update.
# MAGIC
# MAGIC All flows writing to a table follow the same expectations, making it easy to monitor and enforce data quality.
# MAGIC
# MAGIC > Refer to [Data Quality Expectations in Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/expectations) documentation for more details.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Defining Expectations on Streaming Tables
# MAGIC
# MAGIC In Spark Declarative Pipelines, constraints are defined inline in the table's column definition block using `CONSTRAINT ... EXPECT`. There are three violation modes that control what happens when a record fails a rule:
# MAGIC
# MAGIC <div style="display:flex;flex-direction:column;gap:10px;margin:16px 0;">
# MAGIC <div style="display:flex;align-items:flex-start;gap:12px;background:#E8F5E9;border:1px solid #C8E6C9;border-radius:6px;padding:12px 14px;">
# MAGIC   <div style="font-size:20px;min-width:32px;">🟢</div>
# MAGIC   <div>
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#1B5E20;">WARN (default — no ON VIOLATION clause)</div>
# MAGIC     <div style="font-size:16px;color:#555;margin-top:2px;">Invalid rows are <strong>kept</strong> in the table. A warning metric is logged in the pipeline event log. Use for monitoring without blocking data.</div>
# MAGIC     <code style="font-size:15px;">CONSTRAINT valid_field EXPECT (field IS NOT NULL)</code>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="display:flex;align-items:flex-start;gap:12px;background:#FFF3E0;border:1px solid #FFE0B2;border-radius:6px;padding:12px 14px;">
# MAGIC   <div style="font-size:20px;min-width:32px;">🟡</div>
# MAGIC   <div>
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#E65100;">DROP ROW</div>
# MAGIC     <div style="font-size:16px;color:#555;margin-top:2px;">Invalid rows are <strong>removed</strong> from the table. Dropped records are counted in metrics but do not appear in the final dataset.</div>
# MAGIC     <code style="font-size:15px;">CONSTRAINT valid_qty EXPECT (qty > 0) ON VIOLATION DROP ROW</code>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="display:flex;align-items:flex-start;gap:12px;background:#FFEBEE;border:1px solid #FFCDD2;border-radius:6px;padding:12px 14px;">
# MAGIC   <div style="font-size:20px;min-width:32px;">🔴</div>
# MAGIC   <div>
# MAGIC     <div style="font-weight:bold;font-size:16px;color:#C62828;">FAIL UPDATE</div>
# MAGIC     <div style="font-size:16px;color:#555;margin-top:2px;">The entire pipeline update is <strong>halted</strong> when even one record violates the rule. Use for critical fields where any violation indicates a serious upstream issue.</div>
# MAGIC     <code style="font-size:15px;">CONSTRAINT not_null_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE</code>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #009688; background: #e0f2f1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">💡</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #00695c; font-size: 1.1em;">Expectation Strategy</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">Use a layered approach: <code>FAIL UPDATE</code> for critical business keys, <code>DROP ROW</code> for data quality issues that can be filtered out, and <code>WARN</code> for monitoring unusual but potentially valid data patterns.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Expectation Implementation Example
# MAGIC
# MAGIC Here's a comprehensive example showing how to implement data quality expectations on a streaming table:
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE streaming_table
# MAGIC   (
# MAGIC     CONSTRAINT valid_qty        EXPECT (qty >= 0)                     ON VIOLATION DROP ROW,
# MAGIC     CONSTRAINT valid_amount     EXPECT (total_amount >= 0)            ON VIOLATION DROP ROW,
# MAGIC     CONSTRAINT not_null_ts      EXPECT (order_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC     CONSTRAINT valid_email      EXPECT (customer_email RLIKE '^[^@]+@[^@]+\\.[^@]+$'),
# MAGIC     CONSTRAINT reasonable_qty   EXPECT (qty <= 1000)                 ON VIOLATION WARN
# MAGIC   )
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var code = block.textContent.trim();
# MAGIC         var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC         block.innerHTML = 
# MAGIC             '<div style="position:relative;margin:16px 0;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:15px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:16px;"></code></pre>' +
# MAGIC             '</div>';
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">⚠️</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #e65100; font-size: 1.1em;">Data Quality Expectations Cannot Be Defined on a Flow</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">When using the multi-flow pattern, <strong>quality constraints must be defined on the target table</strong> — not inside the <code>CREATE FLOW</code> or <code>@dp.append_flow</code> definition. The table enforces quality rules centrally for all flows that write to it.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Monitoring and Observability
# MAGIC
# MAGIC Data quality expectations automatically generate metrics accessible through two channels 
# MAGIC - **Pipeline UI** for at-a-glance health 
# MAGIC - **System tables** for deep programmatic analysis
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="background:#F3F4F6;border:1px solid #E0E0E0;border-radius:8px;padding:16px 18px;">
# MAGIC     <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
# MAGIC       <div style="font-size:28px;"></div>
# MAGIC       <div style="font-weight:bold;font-size:16px;color:#111;">Records Processed</div>
# MAGIC     </div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.7;">Total rows read and written per pipeline update — gives you a baseline throughput view for each flow.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#FFEBEE;border:1px solid #FFCDD2;border-radius:8px;padding:16px 18px;">
# MAGIC     <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
# MAGIC       <div style="font-size:28px;"></div>
# MAGIC       <div style="font-weight:bold;font-size:16px;color:#C62828;">Records Failing Each Constraint</div>
# MAGIC     </div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.7;">Per-constraint violation counts — identifies exactly which rule is failing and how many records are affected.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#FFF3E0;border:1px solid #FFE0B2;border-radius:8px;padding:16px 18px;">
# MAGIC     <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
# MAGIC       <div style="font-size:28px;"></div>
# MAGIC       <div style="font-weight:bold;font-size:16px;color:#E65100;">Violation Percentage</div>
# MAGIC     </div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.7;">Proportion of records violating each expectation — helps distinguish a minor blip from a systemic quality issue.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#E8F5E9;border:1px solid #C8E6C9;border-radius:8px;padding:16px 18px;">
# MAGIC     <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
# MAGIC       <div style="font-size:28px;"></div>
# MAGIC       <div style="font-weight:bold;font-size:16px;color:#1B5E20;">Historical Trends</div>
# MAGIC     </div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.7;">Quality metrics over time — surfaces gradual data drift or regressions introduced by upstream schema changes.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#E3F2FD;border-left:4px solid #1976D2;border-radius:4px;padding:14px 18px;margin:10px 0 16px 0;">
# MAGIC   <div style="font-weight:bold;font-size:16px;color:#0D47A1;margin-bottom:6px;">Going Deeper — System Tables and Event Logs</div>
# MAGIC   <div style="font-size:16px;color:#333;margin-bottom:10px;">The pipeline event log records one row per flow update, capturing both throughput metrics and per-constraint expectation results. You can query it directly to build custom monitoring dashboards or alert pipelines.</div>
# MAGIC
# MAGIC <div class="code-block" data-language="sql">
# MAGIC SELECT timestamp, table_name, output_rows,
# MAGIC        data_quality.expectations
# MAGIC FROM event_log("pipeline_id")
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND data_quality.expectations IS NOT NULL
# MAGIC ORDER BY timestamp DESC;
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var code = block.textContent.trim();
# MAGIC         var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC         block.innerHTML = 
# MAGIC             '<div style="position:relative;margin:16px 0;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:15px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:16px;"></code></pre>' +
# MAGIC             '</div>';
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Liquid Clustering in Spark Declarative Pipelines

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. What is Liquid Clustering?
# MAGIC
# MAGIC Liquid Clustering is a data layout optimization technique in Delta Lake that replaces **traditional Hive-style partitioning and Z-Ordering**. It organizes data files based on **clustering keys** to improve query performance through efficient data skipping.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### How It Evolved
# MAGIC
# MAGIC <div style="display:flex;align-items:stretch;gap:0;margin:20px 0;">
# MAGIC
# MAGIC   <div style="flex:1;background:#FFEBEE;border-top:4px solid #C62828;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;color:#C62828;font-size:16px;margin-bottom:12px;">Hive Partitioning</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:18px;">
# MAGIC       <li>Rigid directory structure</li>
# MAGIC       <li>Full rewrite to change keys</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display:flex;align-items:center;padding:0 12px;font-size:22px;color:#999;">→</div>
# MAGIC
# MAGIC   <div style="flex:1;background:#FFF3E0;border-top:4px solid #F57C00;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;color:#E65100;font-size:16px;margin-bottom:12px;">Z-Ordering</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:18px;">
# MAGIC       <li>Manual <code>OPTIMIZE</code> required</li>
# MAGIC       <li>Full rewrite on every run</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display:flex;align-items:center;padding:0 12px;font-size:22px;color:#999;">→</div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #388E3C;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:12px;">Liquid Clustering</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:18px;">
# MAGIC       <li>Incremental — only new data reorganized</li>
# MAGIC       <li>Keys changeable at any time</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Three Properties That Make Liquid Clustering Powerful
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="background:#E8F5E9;border-top:4px solid #388E3C;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:8px;">Incremental</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">Optimizes only new or unclustered data; avoids rewriting already clustered files. Efficient for streaming and write-heavy workloads.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#E3F2FD;border-top:4px solid #1976d2;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;color:#0D47A1;font-size:16px;margin-bottom:8px;">Flexible</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">Clustering keys can be updated anytime without full table rewrite. Adapts to evolving query patterns.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#FFF3E0;border-top:4px solid #F57C00;border-radius:8px;padding:18px 20px;">
# MAGIC     <div style="font-weight:bold;color:#E65100;font-size:16px;margin-bottom:8px;">Self-Tuning</div>
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.6;">With <code>CLUSTER BY AUTO</code>, Databricks automatically selects optimal keys based on observed query usage.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#F8F9FA;border-left:4px solid #607D8B;padding:14px 18px;margin:20px 0;border-radius:4px;">
# MAGIC   <div style="font-size:16px;color:#444;line-height:1.7;">
# MAGIC     Think of it as a <strong>library that continuously rearranges its shelves</strong> based on what readers actually look for — <strong>Hive partitioning</strong> locks books into fixed rooms by genre, <strong>Z-Ordering</strong> sorts within those rooms manually, but <strong>Liquid Clustering</strong> observes borrowing patterns and reorganizes automatically over time.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. How Liquid Clustering Works
# MAGIC Below is a series of snapshots illustrating how liquid clustering works. We have a sales table where clustering keys are defined as Date and Customer Id columns (replacing traditional partitions). Click each step to explore further; an explanation for each step is provided below its image.
# MAGIC
# MAGIC
# MAGIC <div style="width:100%; margin:auto; font-family:sans-serif;">
# MAGIC
# MAGIC   <!-- Tab buttons -->
# MAGIC   <div style="display:flex; border-bottom:2px solid #e0e0e0; margin-bottom:0;">
# MAGIC     <button class="dbtab" onclick="showTab(1)" style="padding:10px 18px; border:none; border-bottom:3px solid #1976d2; background:none; font-size:15px; font-weight:bold; color:#1976d2; cursor:pointer; margin-bottom:-2px;">Step 1 — Before Clustering</button>
# MAGIC     <button class="dbtab" onclick="showTab(2)" style="padding:10px 18px; border:none; border-bottom:3px solid transparent; background:none; font-size:15px; font-weight:bold; color:#888; cursor:pointer; margin-bottom:-2px;">Step 2 — Cluster Analysis</button>
# MAGIC     <button class="dbtab" onclick="showTab(3)" style="padding:10px 18px; border:none; border-bottom:3px solid transparent; background:none; font-size:15px; font-weight:bold; color:#888; cursor:pointer; margin-bottom:-2px;">Step 3 — After Clustering</button>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Tab panels -->
# MAGIC   <div class="dbpanel" style="display:block;">
# MAGIC     <img src="./Includes/images/multi_flows_lecture/lq_1.png" style="width:50%; display:block; margin-top:16px;">
# MAGIC     <div style="color:#444; font-size:16px; padding:12px 4px; line-height:1.8;">
# MAGIC       <strong>Before clustering:</strong> Data files are scattered randomly by Date and Customer Id. This means queries must scan all files, even for a single customer or date. Small files exist for low-volume customers (D, E), while high-volume Customer C has larger files. There is no grouping — so performance is poor.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="dbpanel" style="display:none;">
# MAGIC     <img src="./Includes/images/multi_flows_lecture/lq_2.png" style="width:50%; display:block; margin-top:16px;">
# MAGIC     <div style="color:#444; font-size:16px; padding:12px 4px; line-height:1.8;">
# MAGIC       <strong>Clustering analysis:</strong> Liquid Clustering examines the data and groups files by key ranges (shown as green dashed boxes). Low-volume customers (A, B, D, E, F) are grouped together for better co-location. High-volume C is left alone. Only the files that need improvement are reorganized, not the whole table.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="dbpanel" style="display:none;">
# MAGIC     <img src="./Includes/images/multi_flows_lecture/lq_3.png" style="width:50%; display:block; margin-top:16px;">
# MAGIC     <div style="color:#444; font-size:16px; padding:12px 4px; line-height:1.8;">
# MAGIC       <strong>After clustering:</strong> Files are now grouped by Date and Customer Id. Queries can skip files that don't match their filters, so performance improves. Liquid Clustering uses a Hilbert curve to make file boundaries tighter, so skipping is more effective. Only fragmented files are rewritten; future runs only cluster new data.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC   </div>
# MAGIC
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showTab(n) {
# MAGIC   var tabs   = document.getElementsByClassName("dbtab");
# MAGIC   var panels = document.getElementsByClassName("dbpanel");
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].style.color = "#888";
# MAGIC     tabs[i].style.borderBottom = "3px solid transparent";
# MAGIC     panels[i].style.display = "none";
# MAGIC   }
# MAGIC   tabs[n-1].style.color = "#1976d2";
# MAGIC   tabs[n-1].style.borderBottom = "3px solid #1976d2";
# MAGIC   panels[n-1].style.display = "block";
# MAGIC }
# MAGIC window.onload = function() { showTab(1); };
# MAGIC </script>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D3. Applying Liquid Clustering to Streaming Tables in SDP
# MAGIC
# MAGIC You are already familiar with Liquid Clustering on Delta tables. In Spark Declarative Pipelines, it is enabled directly on the streaming table definition using the `CLUSTER BY` clause — no separate `OPTIMIZE` command is needed.
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:16px 0;">
# MAGIC <div style="flex:1;background:#F3F4F6;border:1px solid #E0E0E0;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#333;font-size:16px;margin-bottom:8px;"><code>CLUSTER BY AUTO</code></div>
# MAGIC <div style="font-size:16px;color:#555;margin-bottom:8px;">Databricks <strong>automatically selects</strong> the best clustering keys based on query history. Best when you are unsure which columns will be most frequently filtered.</div>
# MAGIC <pre style="background:#fff;border:1px solid #ddd;border-radius:4px;padding:10px;font-size:14px;margin:0;">
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE my_table
# MAGIC CLUSTER BY AUTO
# MAGIC AS SELECT * FROM STREAM source_table;</pre>
# MAGIC </div>
# MAGIC <div style="flex:1;background:#F3F4F6;border:1px solid #E0E0E0;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#333;font-size:16px;margin-bottom:8px;"><code>CLUSTER BY (columns)</code></div>
# MAGIC <div style="font-size:16px;color:#555;margin-bottom:8px;">You <strong>explicitly specify</strong> the clustering keys. Best when you have strong domain knowledge of your most common filter patterns.</div>
# MAGIC <pre style="background:#fff;border:1px solid #ddd;border-radius:4px;padding:10px;font-size:14px;margin:0;">
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE my_table
# MAGIC CLUSTER BY (region, order_date)
# MAGIC AS SELECT * FROM STREAM source_table;</pre>
# MAGIC </div>
# MAGIC </div>
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var code = block.textContent.trim();
# MAGIC         var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC         block.innerHTML = 
# MAGIC             '<div style="position:relative;margin:16px 0;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:15px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:16px;"></code></pre>' +
# MAGIC             '</div>';
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>
# MAGIC </pre>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D4. Clustering Strategy Selection
# MAGIC
# MAGIC <div style="display:flex;gap:0;align-items:stretch;margin:32px 0;">
# MAGIC
# MAGIC   <div style="flex:1;display:flex;flex-direction:column;padding:32px 24px;background:#E8F5E9;border-radius:12px 0 0 12px;border:1px solid #C8E6C9;">
# MAGIC     <div style="font-size:18px;font-weight:bold;color:#1B5E20;margin-bottom:16px;">CLUSTER BY AUTO</div>
# MAGIC     <div style="font-size:16px;color:#555;margin-bottom:8px;">Let Databricks <strong>learn and decide</strong></div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Keys evolve with query patterns</li>
# MAGIC       <li>Zero manual tuning needed</li>
# MAGIC       <li>Best for new or evolving tables</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display:flex;align-items:center;justify-content:center;padding:0 20px;background:#F8F9FA;border-top:1px solid #E0E0E0;border-bottom:1px solid #E0E0E0;">
# MAGIC     <div style="font-size:28px;color:#999;font-weight:300;">vs</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;display:flex;flex-direction:column;padding:32px 24px;background:#E3F2FD;border-radius:0 12px 12px 0;border:1px solid #BBDEFB;">
# MAGIC     <div style="font-size:18px;font-weight:bold;color:#0D47A1;margin-bottom:16px;">CLUSTER BY (columns)</div>
# MAGIC     <div style="font-size:16px;color:#555;margin-bottom:8px;">You <strong>know your queries</strong></div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Consistent filter columns</li>
# MAGIC       <li>Full control, predictable layout</li>
# MAGIC       <li>Best for stable, well-known patterns</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:24px 0;">
# MAGIC
# MAGIC   <div style="flex:1;display:flex;align-items:center;gap:14px;background:#FFF3E0;border-left:4px solid #F57C00;border-radius:4px;padding:16px 20px;">
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.7;"><strong style="color:#E65100;">AUTO starts unset</strong> — Databricks needs to observe queries first before selecting keys. Check <code>clusterByAuto=true</code> to confirm it is active.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;display:flex;align-items:center;gap:14px;background:#F3E5F5;border-left:4px solid #7B1FA2;border-radius:4px;padding:16px 20px;">
# MAGIC     <div style="font-size:16px;color:#555;line-height:1.7;"><strong style="color:#6A1B9A;">Hybrid is possible</strong> — set explicit columns as a starting hint while still enabling <code>clusterByAuto=true</code> for long-term evolution.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ####CLICK FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC #### When to use CLUSTER BY AUTO
# MAGIC - Best for new tables or when you don't know your query patterns yet
# MAGIC - Lets Databricks automatically pick the best clustering keys based on actual queries
# MAGIC - Adapts as your table grows or query patterns change
# MAGIC - No manual tuning required — Databricks keeps optimizing in the background
# MAGIC
# MAGIC #### When to use CLUSTER BY (columns)
# MAGIC - Use when you know exactly which columns are most often filtered in your queries
# MAGIC - Gives you full control and predictable clustering layout 
# MAGIC - Ideal for stable workloads with consistent filter columns
# MAGIC - Recommended for regulatory or performance-critical tables
# MAGIC
# MAGIC ####  How AUTO Actually Works
# MAGIC Databricks does not blindly pick columns — it runs a continuous **cost-benefit analysis** behind the scenes. Clustering keys are only changed when the predicted savings from data skipping outweigh the cost of reorganizing data files.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>