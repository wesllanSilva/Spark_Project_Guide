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
# MAGIC #Lecture - Introduction to Multiplex Streaming, Delta Sinks, and Iceberg Reads
# MAGIC
# MAGIC This lecture introduces three advanced concepts used in Spark Declarative Pipelines: the **Multiplex pattern** for efficiently ingesting mixed event streams, **Delta Sinks** for writing streaming data to external tables, and **Iceberg reads via Delta UniForm** for enabling cross-platform access to Delta tables.
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - Explain the **Multiplex pattern** and how fan-out works for mixed event streams
# MAGIC - Describe what a **Delta Sink** is and when to use it over managed streaming tables
# MAGIC - Explain how **Delta UniForm** enables Iceberg reads without data duplication
# MAGIC - Understand how these three concepts **chain together** in a single pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The Multiplex Pattern
# MAGIC
# MAGIC The Multiplex pattern addresses a common production challenge: efficiently processing multiple event types that arrive through a single data transport mechanism.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. The Problem: One Stream, Many Schemas
# MAGIC
# MAGIC In production environments, multiple business systems often share a **single data transport** such as:
# MAGIC - One Kafka topic
# MAGIC - One cloud storage path  
# MAGIC - One message queue
# MAGIC
# MAGIC Each message carries a **type field** identifying which business domain it belongs to. Without the Multiplex pattern, this creates significant operational overhead:
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:16px 0;">
# MAGIC <div style="flex:1;background:#FFEBEE;border:1px solid #FFCDD2;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#C62828;font-size:13px;margin-bottom:8px;"> Without Multiplex</div>
# MAGIC <ul style="font-size:13px;color:#333;margin:0;padding-left:16px;">
# MAGIC <li>N event types → N separate pipelines</li>
# MAGIC <li>N separate checkpoints and source scans</li>
# MAGIC <li>Source change must be applied N times</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC <div style="flex:1;background:#E8F5E9;border:1px solid #C8E6C9;border-radius:6px;padding:14px 16px;">
# MAGIC <div style="font-weight:bold;color:#1B5E20;font-size:13px;margin-bottom:8px;"> With Multiplex</div>
# MAGIC <ul style="font-size:13px;color:#333;margin:0;padding-left:16px;">
# MAGIC <li>N event types → 1 ingestion pipeline</li>
# MAGIC <li>1 checkpoint, 1 source scan shared across all domains</li>
# MAGIC <li>Source change applied in one place</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC **Reference Documentation:**
# MAGIC - [Multiplexing Data Pipelines Blog](https://www.databricks.com/blog/2022/04/27/how-uplift-built-cdc-and-multiplexing-data-pipelines-with-databricks-delta-live-tables.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Ingest Once, Fan Out by Type
# MAGIC
# MAGIC The Multiplex pattern follows a simple but powerful approach:
# MAGIC
# MAGIC 1. **Single Ingestion**: Read all event types into one bronze table
# MAGIC 2. **Type-Based Filtering**: Use the event type field to separate domains
# MAGIC 3. **Fan-Out Processing**: Create domain-specific tables downstream
# MAGIC
# MAGIC **Architecture Flow:**
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     SRC["☁️ Mixed Event Stream"]
# MAGIC     BRZ["Bronze Table
# MAGIC All event types together
# MAGIC VARIANT PAYLOAD"]
# MAGIC     A["Domain A Table
# MAGIC WHERE type = A"]
# MAGIC     B["Domain B Table
# MAGIC WHERE type = B"]
# MAGIC     C["Domain C Table
# MAGIC WHERE type = C"]
# MAGIC     SRC -->|"Single ingest
# MAGIC one checkpoint"| BRZ
# MAGIC     BRZ --> A
# MAGIC     BRZ --> B
# MAGIC     BRZ --> C
# MAGIC     style SRC fill:#607D8B,color:#fff
# MAGIC     style BRZ fill:#FF5722,color:#fff
# MAGIC     style A fill:#EF6C00,color:#fff
# MAGIC     style B fill:#EF6C00,color:#fff
# MAGIC     style C fill:#EF6C00,color:#fff
# MAGIC </div>
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - Single checkpoint management
# MAGIC - Shared source scanning
# MAGIC - Centralized error handling
# MAGIC - Simplified monitoring

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Sinks
# MAGIC
# MAGIC Sinks provide a mechanism to write streaming data from a Spark Declarative Pipeline to external Delta tables that exist outside the pipeline's managed scope.
# MAGIC
# MAGIC <div style="display:flex;justify-content:left;margin:32px 0;">
# MAGIC <div class="mermaid" style="min-width:750px;">
# MAGIC flowchart LR
# MAGIC     F["Pipeline Flow\nappend_flow"]
# MAGIC     D["Default"]
# MAGIC     A["@dp.append_flow"]
# MAGIC     subgraph PIPE["   Inside Pipeline — Managed Scope   "]
# MAGIC         ST["Streaming Table\nMaterialized View"]
# MAGIC     end
# MAGIC     subgraph EXT["   Outside Pipeline — External Targets   "]
# MAGIC         SK["Sink\nDelta · Kafka · Custom"]
# MAGIC     end
# MAGIC     F --- D --> ST
# MAGIC     F --- A --> SK
# MAGIC     style F fill:#FF7043,color:#fff,stroke:#E64A19,stroke-width:2px
# MAGIC     style D fill:#F5F5F5,color:#333,stroke:#BDBDBD,stroke-width:1px
# MAGIC     style A fill:#F5F5F5,color:#333,stroke:#BDBDBD,stroke-width:1px
# MAGIC     style ST fill:#FFF3E0,stroke:#F57C00,stroke-width:2px,color:#000
# MAGIC     style SK fill:#1565C0,color:#fff,stroke:#0D47A1,stroke-width:2px
# MAGIC     style PIPE fill:#FFF8E1,stroke:#FF9800,stroke-width:2px,color:#E65100
# MAGIC     style EXT fill:#E3F2FD,stroke:#1976D2,stroke-width:2px,color:#0D47A1
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({
# MAGIC     startOnLoad: true,
# MAGIC     theme: "default",
# MAGIC     flowchart: { padding: 40, nodeSpacing: 60, rankSpacing: 80 }
# MAGIC });
# MAGIC </script>
# MAGIC
# MAGIC <div style="background:#F8F9FA;border-left:4px solid #607D8B;padding:14px 18px;margin:24px 0;border-radius:4px;">
# MAGIC   <div style="font-size:16px;color:#444;line-height:1.8;">
# MAGIC     <strong>Only the Python API is supported</strong> — SQL is not supported for sinks. Only <code>append_flow</code> can write to a sink.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC **Reference Documentation:**
# MAGIC - [Sinks in SDP](https://docs.databricks.com/delta-live-tables/dlt-sinks.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Supported Sink Types
# MAGIC
# MAGIC Databricks supports <strong>four types of sinks</strong> — each suited for a different destination and use case.
# MAGIC
# MAGIC
# MAGIC <div style="display:flex;gap:0;margin:24px 0;align-items:stretch;">
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #388E3C;border-radius:12px 0 0 12px;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:14px;">Delta Table Sink</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Unity Catalog managed tables</li>
# MAGIC       <li>External Delta tables</li>
# MAGIC       <li>Write by path or table name</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E3F2FD;border-top:4px solid #1976D2;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#0D47A1;font-size:16px;margin-bottom:14px;">Apache Kafka Sink</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Write back to Kafka topics</li>
# MAGIC       <li>Low-latency operational use cases</li>
# MAGIC       <li>Reverse ETL out of Databricks</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#FFF3E0;border-top:4px solid #F57C00;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#E65100;font-size:16px;margin-bottom:14px;">Azure Event Hubs Sink</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Uses Kafka interface format</li>
# MAGIC       <li>Real-time event streaming</li>
# MAGIC       <li>Fraud detection · recommendations</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#F3E5F5;border-top:4px solid #7B1FA2;border-radius:0 12px 12px 0;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#6A1B9A;font-size:16px;margin-bottom:14px;">Python Custom Sink</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Write to any data store</li>
# MAGIC       <li>Uses PySpark custom data sources</li>
# MAGIC       <li>Maximum flexibility</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#E8F5E9;border-left:4px solid #388E3C;padding:14px 18px;margin:20px 0;border-radius:4px;">
# MAGIC   <div style="font-size:16px;color:#1B5E20;line-height:1.8;">
# MAGIC     For code examples of each sink type, refer to the <a href="https://docs.databricks.com/aws/en/ldp/ldp-sinks?language=Delta%C2%A0sinks#create-a-sink" style="color:#1976D2;">Creating a Sink Documentation</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Managed Tables vs. Sinks
# MAGIC
# MAGIC Every standard dataset in a Spark Declarative Pipeline — streaming table or materialized view — is **owned and managed by the pipeline**. A **sink** breaks this intentionally: it lets the pipeline write streaming data to a **plain Delta table that exists outside the pipeline's managed scope**.
# MAGIC
# MAGIC <div style="display:flex;gap:0;margin:24px 0;align-items:stretch;">
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #388E3C;border-radius:12px 0 0 12px;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:14px;">Managed Table (Default)</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Data stays within Unity Catalog</li>
# MAGIC       <li>Full pipeline lineage tracking</li>
# MAGIC       <li>Supports expectations and CDC</li>
# MAGIC       <li>Streaming Tables and Materialized Views</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display:flex;align-items:center;justify-content:center;padding:0 20px;background:#F8F9FA;border-top:1px solid #E0E0E0;border-bottom:1px solid #E0E0E0;">
# MAGIC     <div style="font-size:24px;color:#999;font-weight:300;">vs</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E3F2FD;border-top:4px solid #1976D2;border-radius:0 12px 12px 0;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#0D47A1;font-size:16px;margin-bottom:14px;">Sink</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Write to external systems outside Databricks</li>
# MAGIC       <li>Enables reverse ETL and operational use cases</li>
# MAGIC       <li>Supports Kafka, Event Hubs, custom targets</li>
# MAGIC       <li>No expectations — append only</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Delta Sink In Action
# MAGIC
# MAGIC
# MAGIC A <strong>Delta sink</strong> writes pipeline output to a Delta table <em>outside</em> the pipeline's managed lifecycle — unlocking configurations not possible on pipeline-managed streaming tables, such as <strong>Iceberg compatibility</strong>.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:16px 0 28px 0;">
# MAGIC   <div style="flex:1;background:#FFF3E0;border-left:4px solid #F57C00;border-radius:4px;padding:16px 20px;">
# MAGIC     <div style="font-weight:bold;color:#E65100;font-size:16px;margin-bottom:8px;">Streaming Tables Cannot</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Enable Iceberg compatibility</li>
# MAGIC       <li>Share with non-Databricks platforms</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div style="flex:1;background:#E8F5E9;border-left:4px solid #388E3C;border-radius:4px;padding:16px 20px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:8px;">Delta Sink Can</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Have full Delta table property control</li>
# MAGIC       <li>Enables Iceberg UniForm for cross-platform reads</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Implementation
# MAGIC
# MAGIC <div style="display:flex;gap:0;margin:20px 0;align-items:stretch;">
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #388E3C;border-radius:12px 0 0 12px;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:16px;">Step 1 — Register the Sink</div>
# MAGIC     <pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:6px;padding:16px;font-size:14px;line-height:1.8;overflow-x:auto;">
# MAGIC <div class="code-block" data-language="python">
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC dp.create_sink(
# MAGIC     name    = "my_sink",
# MAGIC     format  = "delta",
# MAGIC     options = {
# MAGIC         "tableName": "catalog.schema.table"
# MAGIC     }
# MAGIC )</pre>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display:flex;align-items:center;justify-content:center;padding:0 20px;background:#F8F9FA;border-top:1px solid #E0E0E0;border-bottom:1px solid #E0E0E0;">
# MAGIC     <div style="font-size:24px;color:#999;font-weight:300;">→</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E3F2FD;border-top:4px solid #1976D2;border-radius:0 12px 12px 0;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#0D47A1;font-size:16px;margin-bottom:16px;">Step 2 — Write to the Sink</div>
# MAGIC     <pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:6px;padding:16px;font-size:14px;line-height:1.8;overflow-x:auto;">
# MAGIC
# MAGIC <div class="code-block" data-language="python">
# MAGIC @dp.append_flow(
# MAGIC name   = "my_sink_flow",
# MAGIC target = "my_sink"
# MAGIC )
# MAGIC def my_sink_flow():
# MAGIC     return spark.readStream.table(
# MAGIC         "schema.source_table"
# MAGIC     )</pre>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#F8F9FA;border-left:4px solid #607D8B;border-radius:4px;padding:16px 20px;margin-top:8px;">
# MAGIC   <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC     <li>Checkpointing is handled automatically by <code>append_flow</code></li>
# MAGIC     <li>Only new records written per run — no overwrites</li>
# MAGIC     <li>Python only — no SQL equivalent</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
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
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Iceberg Reads via Delta UniForm
# MAGIC
# MAGIC Delta UniForm enables cross-platform access to Delta tables by automatically generating Apache Iceberg metadata without duplicating the underlying data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. The Cross-Platform Challenge
# MAGIC
# MAGIC <div style="font-size:16px;color:#555;line-height:1.8;margin-bottom:24px;">
# MAGIC   Modern data architectures span multiple platforms. Traditionally, supporting each platform meant <strong>copying data into multiple formats</strong> — creating duplication, sync issues, and increased storage costs. <strong>Delta UniForm</strong> solves this with a single set of files and two metadata layers.
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;gap:24px;margin:24px 0;align-items:flex-start;">
# MAGIC
# MAGIC   <div style="flex:1;display:flex;flex-direction:column;align-items:center;">
# MAGIC     <div class="mermaid" style="width:100%;">
# MAGIC flowchart TB
# MAGIC     subgraph OLD["Traditional Approach"]
# MAGIC         D1["Delta Files"] --> C["Copy and Convert"]
# MAGIC         C --> IC["Iceberg Copy"]
# MAGIC         C --> PA["Parquet Copy"]
# MAGIC         C --> HV["Hive Copy"]
# MAGIC     end
# MAGIC     style D1 fill:#FFEBEE,stroke:#C62828,color:#000
# MAGIC     style C fill:#FFCDD2,stroke:#C62828,color:#000
# MAGIC     style IC fill:#FFEBEE,stroke:#C62828,color:#000
# MAGIC     style PA fill:#FFEBEE,stroke:#C62828,color:#000
# MAGIC     style HV fill:#FFEBEE,stroke:#C62828,color:#000
# MAGIC     style OLD fill:#FFF3F3,stroke:#C62828,stroke-width:2px,color:#C62828
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display:flex;align-items:center;justify-content:center;padding:0 8px;margin-top:80px;">
# MAGIC     <div style="font-size:28px;color:#999;font-weight:300;"></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;display:flex;flex-direction:column;align-items:center;">
# MAGIC     <div class="mermaid" style="width:100%;">
# MAGIC flowchart TB
# MAGIC     subgraph NEW["Delta UniForm"]
# MAGIC         PQ["One Set of Parquet Files"] --> DM["Delta Metadata"]
# MAGIC         PQ --> IM["Iceberg Metadata"]
# MAGIC     end
# MAGIC     style PQ fill:#E8F5E9,stroke:#388E3C,color:#000
# MAGIC     style DM fill:#E3F2FD,stroke:#1976D2,color:#000
# MAGIC     style IM fill:#E3F2FD,stroke:#1976D2,color:#000
# MAGIC     style NEW fill:#F1F8E9,stroke:#388E3C,stroke-width:2px,color:#388E3C
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:24px 0;">
# MAGIC   <div style="flex:1;background:#FFEBEE;border-left:4px solid #C62828;border-radius:4px;padding:16px 20px;">
# MAGIC     <div style="font-weight:bold;color:#C62828;font-size:16px;margin-bottom:8px;">Traditional Approach</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>Data copied into multiple formats</li>
# MAGIC       <li>Sync issues across copies</li>
# MAGIC       <li>Increased storage costs</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div style="flex:1;background:#E8F5E9;border-left:4px solid #388E3C;border-radius:4px;padding:16px 20px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:16px;margin-bottom:8px;">Delta UniForm</div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC       <li>One set of Parquet files</li>
# MAGIC       <li>Delta + Iceberg metadata layers</li>
# MAGIC       <li>No duplication — automatic sync</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#E3F2FD;border-left:4px solid #1976D2;padding:14px 18px;border-radius:4px;">
# MAGIC   <div style="font-size:16px;color:#0D47A1;line-height:1.8;">
# MAGIC     For full details refer to the <a href="https://docs.databricks.com/aws/en/delta/uniform" style="color:#1976D2;font-weight:bold;">Delta UniForm Documentation →</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. How Delta UniForm Works
# MAGIC
# MAGIC   Delta UniForm maintains <strong>one physical dataset</strong> with <strong>two logical views</strong> — no data duplication, no separate copies. Iceberg metadata is generated <strong>asynchronously</strong> after every Delta write, keeping both views in sync automatically.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC <div class="mermaid" style="min-width:700px;">
# MAGIC flowchart LR
# MAGIC     subgraph STORE["<b>One Delta Table — One Set of Parquet Files</b>"]
# MAGIC         PF("<div style='text-align:center;font-size:13px;'>
# MAGIC <img src='./Includes/images/icons/file_icon.png' width='10' style='display:inline-block;margin:0 3px;'/>
# MAGIC <img src='./Includes/images/icons/file_icon.png' width='10' style='display:inline-block;margin:0 3px;'/>
# MAGIC <br/><b>Parquet Data Files</b>
# MAGIC </div>")
# MAGIC         DL["Delta Transaction Log\n_delta_log/"]
# MAGIC         IM["Iceberg Metadata\nmetadata/*.metadata.json"]
# MAGIC         PF --- DL
# MAGIC         PF --- IM
# MAGIC     end
# MAGIC     DC["Databricks Clients\nRead via Delta protocol"]
# MAGIC     IC["External Tools\nSnowflake · Trino · Athena · Spark OSS\nRead via Iceberg REST Catalog"]
# MAGIC     DL --> DC
# MAGIC     IM --> IC
# MAGIC     style DL fill:#1565C0,color:#fff,stroke:#0D47A1,stroke-width:2px
# MAGIC     style IM fill:#2E7D32,color:#fff,stroke:#1B5E20,stroke-width:2px
# MAGIC     style DC fill:#FF3621,color:#fff,stroke:#C62828,stroke-width:2px
# MAGIC     style IC fill:#455A64,color:#fff,stroke:#263238,stroke-width:2px
# MAGIC     style STORE fill:#F8F9FA,stroke:#607D8B,stroke-width:2px,color:#333
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({
# MAGIC     startOnLoad: true,
# MAGIC     theme: "default",
# MAGIC     flowchart: { padding: 40, nodeSpacing: 60, rankSpacing: 80 }
# MAGIC });
# MAGIC </script>
# MAGIC ### Enabling Iceberg Reads 
# MAGIC
# MAGIC <div style="display:flex;flex-direction:column;gap:10px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;background:#F5F5F5;border-radius:6px;padding:12px 14px;">
# MAGIC     <div style="font-size:22px;min-width:32px;">1️⃣</div>
# MAGIC     <div>
# MAGIC       <div style="font-weight:bold;font-size:13px;color:#333;">Disable Deletion Vectors</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:2px;">
# MAGIC         <strong>Property:</strong> <code style="font-size:12px;">'delta.enableDeletionVectors' = 'false'</code><br>
# MAGIC         Iceberg v2 cannot represent Delta's soft-delete markers. Disabling ensures all deletes are hard deletes, making the table fully readable by Iceberg clients.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;background:#F5F5F5;border-radius:6px;padding:12px 14px;">
# MAGIC     <div style="font-size:22px;min-width:32px;">2️⃣</div>
# MAGIC     <div>
# MAGIC       <div style="font-weight:bold;font-size:13px;color:#333;">Column Mapping Mode</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:2px;">
# MAGIC         <strong>Property:</strong> <code style="font-size:12px;">'delta.columnMapping.mode' = 'name'</code><br>
# MAGIC         Ensures column identifiers are consistent between Delta and Iceberg schemas, preventing schema drift and enabling seamless cross-platform access.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;background:#F5F5F5;border-radius:6px;padding:12px 14px;">
# MAGIC     <div style="font-size:22px;min-width:32px;">3️⃣</div>
# MAGIC     <div>
# MAGIC       <div style="font-weight:bold;font-size:13px;color:#333;">Enable IcebergCompatV2</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:2px;">
# MAGIC         <strong>Property:</strong> <code style="font-size:12px;">'delta.enableIcebergCompatV2' = 'true'</code><br>
# MAGIC         Activates Delta's write protocol compatible with Iceberg v2, allowing Iceberg clients to read Delta tables without data conversion.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;background:#F5F5F5;border-radius:6px;padding:12px 14px;">
# MAGIC     <div style="font-size:22px;min-width:32px;">4️⃣</div>
# MAGIC     <div>
# MAGIC       <div style="font-weight:bold;font-size:13px;color:#333;">Enable Universal Format</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:2px;">
# MAGIC         <strong>Property:</strong> <code style="font-size:12px;">'delta.universalFormat.enabledFormats' = 'iceberg'</code><br>
# MAGIC         Triggers asynchronous Iceberg metadata generation after every Delta commit, ensuring up-to-date Iceberg views for external tools.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#FFEBEE;border-left:4px solid #F44336;border-radius:4px;padding:14px 18px;margin:16px 0;">
# MAGIC   <div style="font-weight:bold;font-size:14px;color:#B71C1C;margin-bottom:6px;">⚠️ Iceberg Reads Only Work on Plain Delta Tables</div>
# MAGIC   <div style="font-size:14px;color:#333;">
# MAGIC     <strong>Note:</strong> Pipeline-managed <b>streaming tables</b> and <b>materialized views</b> cannot have Iceberg reads enabled. Only a plain external Delta table—such as one created via a <b>Delta Sink</b>—supports UniForm. This bridge is required for cross-platform access.
# MAGIC     <br><br>
# MAGIC     <strong>Additional Details:</strong> Setting these properties is a one-time operation, but must be done before any data is written. Once enabled, Iceberg metadata is kept in sync automatically, allowing tools like Snowflake, Trino, and Athena to query the same Delta table.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background:#E3F2FD;border-left:4px solid #1976D2;padding:14px 18px;border-radius:4px;margin-top:8px;">
# MAGIC   <div style="font-size:16px;color:#0D47A1;line-height:1.8;">
# MAGIC     For more details refer to the <a href="https://docs.databricks.com/aws/en/delta/uniform" style="color:#1976D2;font-weight:bold;">Delta UniForm Documentation</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### EXPAND FOR ADDITIONAL DETAILS
# MAGIC <details>
# MAGIC
# MAGIC
# MAGIC <div style="font-size:16px;color:#555;line-height:2;margin-bottom:24px;">
# MAGIC
# MAGIC Delta UniForm (Universal Format) solves a long-standing problem in the lakehouse ecosystem: <strong>different tools speak different table formats</strong>. Snowflake, Trino, Athena, and open-source Spark all understand Apache Iceberg — but not Delta Lake natively. UniForm bridges this gap by making your Delta tables <em>simultaneously readable as Iceberg tables</em>, without any data duplication or separate pipelines.
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="font-size:16px;color:#555;line-height:2;margin-bottom:24px;">
# MAGIC
# MAGIC Both Delta Lake and Apache Iceberg are built on the same foundation: <strong>Parquet data files</strong> plus a <strong>metadata layer</strong>. UniForm exploits this by asynchronously generating Iceberg metadata after every Delta write — the same Parquet files now serve two formats at once. From a Delta client's perspective, nothing changes. From an Iceberg client's perspective, it looks like a native Iceberg table. You get <strong>one dataset, two logical views, zero duplication</strong>.
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Benefits
# MAGIC
# MAGIC <div style="font-size:16px;color:#555;line-height:2;">
# MAGIC
# MAGIC - <strong>No data movement:</strong> A single copy of Parquet files is shared across Delta and Iceberg clients — no ETL, no replication, no storage overhead.
# MAGIC - <strong>Cross-platform interoperability:</strong> Tools like Snowflake, Trino, Apache Athena, and open-source Spark can query your Delta tables directly via the Iceberg REST Catalog.
# MAGIC - <strong>Negligible write overhead:</strong> Iceberg metadata generation happens asynchronously after the Delta commit, so your write pipelines are not slowed down.
# MAGIC - <strong>Works with Unity Catalog:</strong> Unity Catalog acts as the Iceberg REST Catalog, meaning external clients get governed, catalogued access without any extra infrastructure.
# MAGIC - <strong>Performance parity:</strong> Benchmarks show comparable read performance between Delta UniForm and natively managed Iceberg tables on Snowflake.
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Limitations
# MAGIC
# MAGIC <div style="font-size:16px;color:#555;line-height:2;">
# MAGIC
# MAGIC - <strong>Read-only for Iceberg clients:</strong> External tools can only <em>read</em> via Iceberg. All writes must go through Delta — you cannot write to a UniForm table from Snowflake or Trino.
# MAGIC - <strong>Deletion Vectors must be disabled:</strong> Tables using deletion vectors (a Delta performance feature) cannot have UniForm enabled. You must set <code>'delta.enableDeletionVectors' = 'false'</code>.
# MAGIC - <strong>Streaming Tables and Materialized Views are excluded:</strong> Pipeline-managed tables in Spark Declarative Pipelines cannot have the required <code>delta.universalFormat.enabledFormats</code> property set directly. This is where <strong>Delta Sinks</strong> come in — they write to plain external Delta tables where UniForm can be freely enabled.
# MAGIC - <strong>Column mapping is permanent:</strong> Enabling UniForm also enables column mapping (<code>delta.columnMapping.mode = 'name'</code>), which cannot be dropped once set.
# MAGIC - <strong>Databricks Runtime 14.3 LTS or above required:</strong> Any Databricks client writing to a UniForm-enabled table must use DBR 14.3+.
# MAGIC - <strong>Table must be accessed by name:</strong> Iceberg metadata generation is only triggered when the table is accessed by name (not by path).
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background:#E8F5E9;border-left:4px solid #2E7D32;padding:14px 20px;border-radius:4px;margin-top:8px;font-size:15px;color:#1B5E20;line-height:1.9;">
# MAGIC   <strong>🔗 How this connects to Delta Sinks:</strong> Since Streaming Tables and Materialized Views can't use UniForm directly, the pattern in this lecture is to use a <strong>Delta Sink</strong> — write your streaming pipeline output to a plain external Delta table, enable UniForm on that table, and Iceberg clients can immediately read it. This is the recommended production pattern for cross-platform Iceberg access from a live streaming pipeline.
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Connecting the Concepts
# MAGIC
# MAGIC These three concepts work together to solve complex real-world data pipeline requirements.Each concept solves a distinct problem. Together they form a complete pattern for pipelines that need real-time processing and cross-platform access.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div class="mermaid" style="min-width:800px;">
# MAGIC flowchart LR
# MAGIC     C1[" <b>Multiplex</b>\nOne source, many event types\nIngest once — fan out by type"]
# MAGIC     C2[" <b>Delta Sink</b>\nExternal Delta table\ndp.create_sink + append_flow"]
# MAGIC     C3[" <b>Delta UniForm</b>\nExternal tools need access\nAuto-generate Iceberg metadata"]
# MAGIC     C1 -->|"Silver table\nfeeds the sink"| C2
# MAGIC     C2 -->|"Plain Delta table\nenables UniForm"| C3
# MAGIC     style C1 fill:#FF5722,color:#fff,stroke:#E64A19,stroke-width:2px
# MAGIC     style C2 fill:#1565C0,color:#fff,stroke:#0D47A1,stroke-width:2px
# MAGIC     style C3 fill:#2E7D32,color:#fff,stroke:#1B5E20,stroke-width:2px
# MAGIC     linkStyle 0 stroke:#FF7043,stroke-width:2px
# MAGIC     linkStyle 1 stroke:#1976D2,stroke-width:2px
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({
# MAGIC     startOnLoad: true,
# MAGIC     theme: "default",
# MAGIC     flowchart: { padding: 40, nodeSpacing: 80, rankSpacing: 120 }
# MAGIC });
# MAGIC </script>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### EXPAND FOR ADDITIONAL DETAILS
# MAGIC <details>
# MAGIC
# MAGIC ### Combined Advanced Pipeline Pattern
# MAGIC
# MAGIC #### 1. Multiplex Pattern
# MAGIC - **Problem**: Multiple event types in one source
# MAGIC - **Solution**: Single ingestion with type-based fan-out
# MAGIC - **Output**: Domain-specific silver tables
# MAGIC
# MAGIC #### 2. Delta Sink
# MAGIC - **Problem**: Need external table for cross-platform access
# MAGIC - **Solution**: `dp.create_sink()` + `@dp.append_flow`
# MAGIC - **Output**: Plain Delta table outside pipeline scope
# MAGIC
# MAGIC #### 3. Delta UniForm
# MAGIC - **Problem**: External tools need Iceberg format access  
# MAGIC - **Solution**: Auto-generate Iceberg metadata on plain Delta table
# MAGIC - **Output**: Dual-protocol table accessible by any platform
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>