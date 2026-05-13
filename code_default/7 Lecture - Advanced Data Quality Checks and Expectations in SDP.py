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
# MAGIC # Lecture — Advanced Data Quality Checks and Expectations in Spark Declarative Pipelines
# MAGIC
# MAGIC In production data pipelines, data quality is paramount. While Spark Declarative Pipelines provide built-in expectations with three violation modes — <strong>WARN</strong>, <strong>DROP ROW</strong>, and <strong>FAIL UPDATE</strong> — real-world scenarios demand more sophisticated approaches. Production pipelines must validate business rules, handle schema evolution gracefully, and preserve every record for audit and remediation purposes.
# MAGIC
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - Explain the limitations of basic violation modes for production-grade data quality
# MAGIC - Implement range-based and NULL-tolerant constraint expressions using advanced SQL patterns
# MAGIC - **Apply advanced patterns** including row-count validation, missing record detection, and primary key uniqueness
# MAGIC - Describe **schema evolution** and design bronze layers that handle it safely without pipeline failures
# MAGIC - Apply the **quarantine pattern** **using inverse logic** to achieve zero data loss with detailed failure tracking
# MAGIC - Choose appropriate strategies between DROP ROW and quarantine based on business requirements
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Why Advanced Quality Expectations?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. The Limits of Basic Expectations
# MAGIC
# MAGIC
# MAGIC A <code>NOT NULL</code> check confirms that a field is <em>present</em> — but presence alone does not mean a value is <em>correct</em>. The table below shows the range of quality problems that appear in real production data, and whether a basic <code>NOT NULL</code> check catches them.
# MAGIC
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;font-size:16px;margin-bottom:20px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#0b2026;color:#F9F7F4;">
# MAGIC       <th style="padding:12px 16px;text-align:left;">Problem Type</th>
# MAGIC       <th style="padding:12px 16px;text-align:left;">Example</th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">NOT NULL catches it?</th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">Advanced Expectation?</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Numeric anomaly</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Negative quantity in an order</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Temporal inconsistency</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Event date set to year 1970 (system default)</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Range violation</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Discount rate = 120%</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Optional field rule</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Field may be NULL, but when present must be &gt;= 0</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Schema evolution</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">New column added mid-stream breaks existing rules</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;"><strong>Data loss</strong></td>
# MAGIC       <td style="padding:11px 16px;">Invalid records permanently dropped — no audit trail</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="background:#FFF8E8;border-left:4px solid #FFAB00;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#FF5F46;"> Real-World Example:</strong> A <code>discount_rate</code> field containing <code>120</code> passes every <code>NOT NULL</code> check without complaint — but it represents an impossible discount that will silently corrupt downstream revenue calculations. Only a range constraint like <code>discount_rate BETWEEN 0 AND 100</code> catches this before it flows downstream.
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block:not([data-processed])').forEach(function(block) {
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:12px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:3px 12px;font-size:12px;background:#EEEDE9;color:#0b2026;border:1px solid #618794;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#F9F7F4;border-radius:8px;padding:16px;padding-top:38px;overflow-x:auto;margin:0;border:1px solid #EEEDE9;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:15px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var el = document.getElementById(id);
# MAGIC     el.textContent = code;
# MAGIC     if (typeof Prism !== 'undefined') Prism.highlightElement(el);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var btn = this, t = document.createElement('textarea');
# MAGIC       t.value = code; document.body.appendChild(t); t.select();
# MAGIC       document.execCommand('copy'); document.body.removeChild(t);
# MAGIC       btn.textContent = '✓ Copied!';
# MAGIC       setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Advanced Expectation Patterns
# MAGIC
# MAGIC Beyond row-level constraints, Databricks expectations support <strong>cross-table validation</strong>. These patterns let you verify row counts, detect missing records, and enforce primary key uniqueness across datasets — catching problems that single-row checks cannot see.
# MAGIC
# MAGIC <div style="margin: 20px 0; display: flex; flex-direction: column; gap: 20px;">
# MAGIC
# MAGIC   <!-- Card 1 -->
# MAGIC   <div style="background:#E3F2FD; border-top: 4px solid #4299E0; border-radius: 8px; padding: 20px;">
# MAGIC     <div style="font-weight: bold; color: #4299E0; font-size: 15px; margin-bottom: 8px;">Row Count Validation</div>
# MAGIC     <div style="font-size: 15px; color: #618794; margin-bottom: 14px; line-height: 1.7;">Validates that row counts match between two tables — useful after joins, aggregations, or pipeline fan-out to ensure no records were silently dropped.</div>
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH MATERIALIZED VIEW count_verification (
# MAGIC   CONSTRAINT no_rows_dropped EXPECT (a_count == b_count)
# MAGIC     ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC AS SELECT * FROM
# MAGIC   (SELECT COUNT(*) AS a_count FROM table_a),
# MAGIC   (SELECT COUNT(*) AS b_count FROM table_b)</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 2 -->
# MAGIC   <div style="background:#FFF3E0; border-top: 4px solid #FFAB00; border-radius: 8px; padding: 20px;">
# MAGIC     <div style="font-weight: bold; color: #FFAB00; font-size: 15px; margin-bottom: 8px;">Missing Record Detection</div>
# MAGIC     <div style="font-size: 15px; color: #618794; margin-bottom: 14px; line-height: 1.7;">Uses a LEFT OUTER JOIN to identify records present in a validation copy but absent in the report table — catching completeness failures that row-level checks miss entirely.</div>
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH MATERIALIZED VIEW report_compare_tests (
# MAGIC   CONSTRAINT no_missing_records EXPECT (r_key IS NOT NULL)
# MAGIC     ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC AS SELECT v.*, r.key AS r_key
# MAGIC FROM validation_copy v
# MAGIC LEFT OUTER JOIN report r ON v.key = r.key</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 3 -->
# MAGIC   <div style="background:#FFEBEE; border-top: 4px solid #FF5F46; border-radius: 8px; padding: 20px;">
# MAGIC     <div style="font-weight: bold; color: #FF5F46; font-size: 15px; margin-bottom: 8px;">Primary Key Uniqueness</div>
# MAGIC     <div style="font-size: 15px; color: #618794; margin-bottom: 14px; line-height: 1.7;">Groups by the primary key and checks that every group has exactly one entry. Catches duplicate key violations before they corrupt downstream joins or aggregations.</div>
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH MATERIALIZED VIEW report_pk_tests (
# MAGIC   CONSTRAINT unique_pk EXPECT (num_entries = 1)
# MAGIC     ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC AS SELECT pk, COUNT(*) AS num_entries
# MAGIC FROM report
# MAGIC GROUP BY pk</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#F9F7F4; border-left: 4px solid #618794; border-radius: 4px; padding: 14px 20px; font-size: 16px; color: #0B2026; line-height: 1.9;">
# MAGIC   <strong style="color:#1B5162;">For more details, refer to the documentation:</strong>
# MAGIC   <a href="https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=SQL" style="color:#4299E0; font-weight: bold;">Databricks Expectation Patterns documentation</a>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block:not([data-processed])').forEach(function(block) {
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:4px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:3px 12px;font-size:12px;background:#EEEDE9;color:#0b2026;border:1px solid #618794;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#F9F7F4;border-radius:8px;padding:16px;padding-top:38px;overflow-x:auto;margin:0;border:1px solid #EEEDE9;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var el = document.getElementById(id);
# MAGIC     el.textContent = code;
# MAGIC     if (typeof Prism !== 'undefined') Prism.highlightElement(el);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var btn = this, t = document.createElement('textarea');
# MAGIC       t.value = code; document.body.appendChild(t); t.select();
# MAGIC       document.execCommand('copy'); document.body.removeChild(t);
# MAGIC       btn.textContent = '✓ Copied!';
# MAGIC       setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Writing NULL-Tolerant Constraints
# MAGIC
# MAGIC Each <code>CONSTRAINT</code> block should validate a <strong>single logical rule</strong>. This gives the pipeline UI precise per-constraint metrics. But there is a critical trap: <strong>NULL evaluates to NOT TRUE</strong> in SQL, so a basic range check treats every NULL as a violation.
# MAGIC
# MAGIC <div style="display:flex;flex-direction:column;gap:20px;margin:20px 0;">
# MAGIC
# MAGIC   <!-- Card 1: Naive -->
# MAGIC   <div style="background:#FFEBEE;border-top:4px solid #98102A;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#98102A;font-size:15px;margin-bottom:10px;">Naive — NULLs treated as violations</div>
# MAGIC     <div class="code-block" data-language="sql">CONSTRAINT valid_discount
# MAGIC EXPECT (
# MAGIC   discount_rate >= 0
# MAGIC   AND discount_rate <= 100
# MAGIC )
# MAGIC -- Every NULL record is flagged as a violation</div>
# MAGIC     <div style="margin-top:12px;font-size:14px;color:#618794;line-height:1.7;">After schema evolution, all historic records that predate the <code>discount_rate</code> column will have <code>NULL</code> — and every one will fail this constraint, flooding the pipeline UI with false violations.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 2: NULL-Tolerant -->
# MAGIC   <div style="background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:10px;">NULL-Tolerant — validates only when present</div>
# MAGIC     <div class="code-block" data-language="sql">CONSTRAINT valid_discount
# MAGIC EXPECT (
# MAGIC   CASE
# MAGIC     WHEN discount_rate IS NOT NULL
# MAGIC     THEN discount_rate >= 0
# MAGIC          AND discount_rate <= 100
# MAGIC     ELSE TRUE   -- NULL is acceptable
# MAGIC   END
# MAGIC )</div>
# MAGIC     <div style="margin-top:12px;font-size:14px;color:#618794;line-height:1.7;">NULL records pass cleanly. Only records where <code>discount_rate</code> is present <em>and</em> out of range are flagged — giving you accurate, noise-free violation metrics.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block:not([data-processed])').forEach(function(block) {
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:12px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:3px 12px;font-size:12px;background:#EEEDE9;color:#0b2026;border:1px solid #618794;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#F9F7F4;border-radius:8px;padding:16px;padding-top:38px;overflow-x:auto;margin:0;border:1px solid #EEEDE9;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var el = document.getElementById(id);
# MAGIC     el.textContent = code;
# MAGIC     if (typeof Prism !== 'undefined') Prism.highlightElement(el);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var btn = this, t = document.createElement('textarea');
# MAGIC       t.value = code; document.body.appendChild(t); t.select();
# MAGIC       document.execCommand('copy'); document.body.removeChild(t);
# MAGIC       btn.textContent = '✓ Copied!';
# MAGIC       setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>

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
# MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.2em;">Rule of thumb</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <b></b></strong> Whenever a column may be absent — because it is optional or was added after the pipeline started — wrap its constraint in a <code>'CASE WHEN ... IS NOT NULL THEN ... ELSE TRUE END'</code> block.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Resilient Pipeline Design

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Ingest Everything as STRING
# MAGIC
# MAGIC The most resilient bronze layer design <strong>never rejects a record due to type mismatch</strong>. By storing all incoming fields as <code>STRING</code>, you accept whatever the source sends — integers, decimals, mixed types — and defer type enforcement to silver where <code>TRY_CAST</code> handles failures gracefully.
# MAGIC
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:8px;"> Bronze — Accept Everything</div>
# MAGIC     All fields inferred as STRING. Type mismatches never fail the pipeline — an integer in a string field is just a string.
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH STREAMING TABLE bronze_events
# MAGIC COMMENT "Bronze: all fields as STRING, schema rescue enabled"
# MAGIC AS SELECT *
# MAGIC FROM STREAM read_files(
# MAGIC   '/path/to/source',
# MAGIC   format => 'json',
# MAGIC   schemaEvolutionMode => 'rescue'
# MAGIC )</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#F9F7F4;border-top:4px solid #1B5162;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#1B5162;font-size:15px;margin-bottom:8px;">Silver — Enforce Types Safely</div>
# MAGIC     <div style="font-size:15px;color:#618794;margin-bottom:10px;line-height:1.7;"><code>TRY_CAST</code> returns NULL on cast failure instead of halting the pipeline. The NULL-tolerant constraint pattern then handles the rest.</div>
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH STREAMING TABLE silver_events (
# MAGIC   CONSTRAINT valid_amount EXPECT (
# MAGIC     CASE WHEN amount IS NOT NULL
# MAGIC     THEN amount >= 0 ELSE TRUE END
# MAGIC   ) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT *,
# MAGIC   TRY_CAST(amount_str AS DOUBLE) AS amount
# MAGIC FROM STREAM bronze_events</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block:not([data-processed])').forEach(function(block) {
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:12px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:3px 12px;font-size:12px;background:#EEEDE9;color:#0b2026;border:1px solid #618794;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#F9F7F4;border-radius:8px;padding:16px;padding-top:38px;overflow-x:auto;margin:0;border:1px solid #EEEDE9;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:15px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var el = document.getElementById(id);
# MAGIC     el.textContent = code;
# MAGIC     if (typeof Prism !== 'undefined') Prism.highlightElement(el);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var btn = this, t = document.createElement('textarea');
# MAGIC       t.value = code; document.body.appendChild(t); t.select();
# MAGIC       document.execCommand('copy'); document.body.removeChild(t);
# MAGIC       btn.textContent = '✓ Copied!';
# MAGIC       setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Schema Evolution Tools at the Bronze Layer
# MAGIC
# MAGIC <div style="font-size:16px;color:#1B5162;line-height:1.9;margin-bottom:20px;">
# MAGIC Two built-in mechanisms cover the full lifecycle of schema change — <code>schemaHints</code> for columns you know are coming, and <code>_rescued_data</code> as the last line of defence for anything unexpected.
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:8px;"> schemaHints — Declare Future Columns Today</div>
# MAGIC     <div style="font-size:15px;color:#618794;margin-bottom:10px;line-height:1.7;">Declare columns expected in <em>upcoming</em> files before they arrive. When the new column appears, it populates automatically. Records before the evolution carry <code>NULL</code> — backward and forward compatible simultaneously.</div>
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH STREAMING TABLE bronze_events
# MAGIC AS SELECT *
# MAGIC FROM STREAM read_files(
# MAGIC   '/path/to/source',
# MAGIC   format => 'json',
# MAGIC   schemaHints => 'loyalty_tier STRING, region_code STRING',
# MAGIC )
# MAGIC -- Old records: loyalty_tier = NULL (acceptable)
# MAGIC -- New records: loyalty_tier populated automatically</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#FFF3E0;border-top:4px solid #FFAB00;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#FFAB00;font-size:15px;margin-bottom:8px;"> _rescued_data — Last Line of Defence</div>
# MAGIC     <div style="font-size:15px;color:#618794;margin-bottom:10px;line-height:1.7;">Any field arriving outside the declared schema — unexpected columns, type mismatches — is captured as JSON in <code>_rescued_data</code>. Nothing is silently discarded. Query it at any time for investigation or recovery.</div>
# MAGIC     <div class="code-block" data-language="sql">-- Inspect rescued fields after the fact
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   _rescued_data:unexpected_field  AS unexpected_field,
# MAGIC   _rescued_data:new_column        AS new_column
# MAGIC FROM bronze_events
# MAGIC WHERE _rescued_data IS NOT NULL</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#FFF8E8;border-left:4px solid #FFAB00;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#FF5F46;">⚠️ Critical interaction:</strong> When a column is added via schema evolution, all records ingested <em>before</em> the evolution carry <code>NULL</code> for that column. Any constraint written for that column <strong>must use the NULL-tolerant <code>CASE WHEN</code> pattern</strong> — otherwise every historic record fails the constraint, causing widespread false violations in the pipeline UI.
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block:not([data-processed])').forEach(function(block) {
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:12px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:3px 12px;font-size:12px;background:#EEEDE9;color:#0b2026;border:1px solid #618794;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#F9F7F4;border-radius:8px;padding:16px;padding-top:38px;overflow-x:auto;margin:0;border:1px solid #EEEDE9;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:15px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var el = document.getElementById(id);
# MAGIC     el.textContent = code;
# MAGIC     if (typeof Prism !== 'undefined') Prism.highlightElement(el);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var btn = this, t = document.createElement('textarea');
# MAGIC       t.value = code; document.body.appendChild(t); t.select();
# MAGIC       document.execCommand('copy'); document.body.removeChild(t);
# MAGIC       btn.textContent = '✓ Copied!';
# MAGIC       setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. The Quarantine Pattern

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. How the Quarantine Pattern Works
# MAGIC
# MAGIC The quarantine pattern routes every incoming record through quality evaluation, then splits output into two paths based on results—clean path for analytics and quarantine path for remediation.
# MAGIC
# MAGIC **Key Guarantee**: No record is ever dropped
# MAGIC
# MAGIC **Mathematical Relationship**: `Total Records In = Clean Records + Quarantine Records`
# MAGIC
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     IN([" Incoming Record"])
# MAGIC     EVAL["Evaluate all expectations\nagainst the record"]
# MAGIC     ALL{"Do ALL\nexpectations pass?"}
# MAGIC     PASS["is_quarantined = FALSE\nquarantine_reason = empty"]
# MAGIC     FAIL["is_quarantined = TRUE\nquarantine_reason = list of\nfailed rules"]
# MAGIC     CLEAN[/" Clean Records\nReady for analytics"/]
# MAGIC     QRTN[/" Quarantine Records\nFor investigation\nand reprocessing"/]
# MAGIC     IN --> EVAL
# MAGIC     EVAL --> ALL
# MAGIC     ALL -->|"Yes"| PASS
# MAGIC     ALL -->|"No — any rule fails"| FAIL
# MAGIC     PASS --> CLEAN
# MAGIC     FAIL --> QRTN
# MAGIC     style IN   fill:#1565C0,color:#fff,stroke:none
# MAGIC     style EVAL fill:#37474F,color:#fff,stroke:none
# MAGIC     style ALL  fill:#F9A825,color:#111,stroke:none
# MAGIC     style PASS fill:#2E7D32,color:#fff,stroke:none
# MAGIC     style FAIL fill:#B71C1C,color:#fff,stroke:none
# MAGIC     style CLEAN fill:#1B5E20,color:#fff,stroke:none
# MAGIC     style QRTN fill:#7F0000,color:#fff,stroke:none
# MAGIC </div>
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>
# MAGIC
# MAGIC
# MAGIC <div style="background:#FFF3E0;border-left:4px solid #FF9800;border-radius:4px;padding:14px 18px;margin:16px 0 10px 0;">
# MAGIC <div style="font-weight:bold;font-size:14px;color:#E65100;margin-bottom:6px;">⚠️ The Quality Tracking Table Must Always Use WARN</div>
# MAGIC <div style="font-size:14px;color:#333;">If <code>DROP ROW</code> or <code>FAIL UPDATE</code> is applied to the quality tracking table, invalid records are removed <em>before</em> the quarantine flag can be calculated — silently breaking the zero-data-loss guarantee. The <code>WARN</code> action is used here for one reason only: to surface per-constraint violation metrics in the pipeline UI. All actual routing logic is handled separately by the inverse logic flow.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#E3F2FD;border-left:4px solid #1976D2;border-radius:4px;padding:14px 18px;margin:10px 0;">
# MAGIC <div style="font-weight:bold;font-size:14px;color:#0D47A1;margin-bottom:6px;">ℹ️ Why Partition the Quality Tracking Table by is_quarantined?</div>
# MAGIC <div style="font-size:14px;color:#333;">Partitioning the quality tracking table by the <code>is_quarantined</code> column physically separates clean and failed records on storage. The downstream queries that split the two paths — filtering on <code>is_quarantined = FALSE</code> and <code>is_quarantined = TRUE</code> — then benefit from partition pruning, reading only the relevant partition rather than scanning the entire table.</div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Zero Data Loss with Inverse Logic
# MAGIC
# MAGIC <code>DROP ROW</code> permanently deletes invalid records — there is no recovery path. The <strong>quarantine pattern</strong> eliminates data loss entirely by routing <em>every</em> record into the table, then using an <code>is_quarantined</code> flag and inverse logic to split clean and failed records into separate downstream views.
# MAGIC <div style="display:flex;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="flex:1;background:#E3F2FD;border-top:4px solid #4299E0;border-radius:8px;padding:18px;">
# MAGIC     <div style="font-weight:bold;color:#4299E0;font-size:15px;margin-bottom:8px;"> Step 1 — Quarantine Table with Inverse Logic</div>
# MAGIC     <div style="font-size:15px;color:#618794;line-height:1.8;margin-bottom:10px;">All records are written using <code>WARN</code> — no records are dropped. The <code>is_quarantined</code> flag is derived using <code>NOT(all rules)</code>: if any rule fails, the record is flagged. <code>WARN</code> is used solely to surface per-constraint metrics in the pipeline UI.</div>
# MAGIC     <div class="code-block" data-language="sql">CREATE OR REFRESH STREAMING TABLE trips_quarantine (
# MAGIC   CONSTRAINT valid_distance EXPECT (trip_distance > 0),
# MAGIC   CONSTRAINT valid_fare     EXPECT (fare_amount >= 0),
# MAGIC   CONSTRAINT valid_pax      EXPECT (passenger_count BETWEEN 1 AND 9)
# MAGIC   -- WARN: surfaces metrics in UI, no records dropped
# MAGIC )
# MAGIC PARTITIONED BY (is_quarantined)
# MAGIC AS SELECT *,
# MAGIC   NOT(
# MAGIC     trip_distance > 0
# MAGIC     AND fare_amount >= 0
# MAGIC     AND passenger_count BETWEEN 1 AND 9
# MAGIC   ) AS is_quarantined
# MAGIC FROM STREAM bronze_trips</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:18px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:8px;"> Step 2 — Split into Clean and Failed Views</div>
# MAGIC     <div style="font-size:15px;color:#618794;line-height:1.8;margin-bottom:10px;">Two Materialized Views filter on the <code>is_quarantined</code> partition. Because the quarantine table is <strong>partitioned by <code>is_quarantined</code></strong>, each view benefits from full partition pruning — only its partition is scanned, not the whole table.</div>
# MAGIC     <div class="code-block" data-language="sql">-- Clean records for downstream analytics
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW valid_trips_data
# MAGIC AS SELECT * FROM trips_quarantine
# MAGIC WHERE is_quarantined = FALSE;
# MAGIC
# MAGIC -- Failed records preserved for remediation and audit
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW invalid_trips_data
# MAGIC AS SELECT * FROM trips_quarantine
# MAGIC WHERE is_quarantined = TRUE;</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#F9F7F4;border-left:4px solid #FFAB00;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#FFAB00;">💡 Why WARN and not DROP ROW on the quarantine table?</strong> If <code>DROP ROW</code> or <code>FAIL UPDATE</code> were applied to the quarantine table, invalid records would be removed before the <code>is_quarantined</code> flag is evaluated — defeating the entire pattern. <code>WARN</code> ensures every record is written and routed correctly by the inverse logic column.
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default", flowchart: { rankSpacing: 60, nodeSpacing: 50 } });
# MAGIC </script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block:not([data-processed])').forEach(function(block) {
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:12px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:3px 12px;font-size:12px;background:#EEEDE9;color:#0b2026;border:1px solid #618794;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#F9F7F4;border-radius:8px;padding:16px;padding-top:38px;overflow-x:auto;margin:0;border:1px solid #EEEDE9;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:15px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var el = document.getElementById(id);
# MAGIC     el.textContent = code;
# MAGIC     if (typeof Prism !== 'undefined') Prism.highlightElement(el);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var btn = this, t = document.createElement('textarea');
# MAGIC       t.value = code; document.body.appendChild(t); t.select();
# MAGIC       document.execCommand('copy'); document.body.removeChild(t);
# MAGIC       btn.textContent = '✓ Copied!';
# MAGIC       setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Choosing Between DROP ROW and Quarantine
# MAGIC
# MAGIC
# MAGIC Both strategies enforce data quality, but they differ fundamentally in what happens to invalid records. The right choice depends on whether your business needs an audit trail, recovery capability, and whether false violations from schema evolution are a concern.
# MAGIC
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;font-size:16px;margin-bottom:20px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#0b2026;color:#F9F7F4;">
# MAGIC       <th style="padding:12px 16px;text-align:left;"></th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">DROP ROW</th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">Quarantine Pattern</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Invalid records</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">Permanently deleted</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">Preserved in quarantine table</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Audit trail</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;"> None</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Full — queryable</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Data recovery</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;"> Not possible</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Fix rule → re-route from quarantine</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">UI violation metrics</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Visible in pipeline UI</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Per-constraint metrics via WARN</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Read performance</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Full table scan on clean data</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Partition pruning on <code>is_quarantined</code></td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Pipeline complexity</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Low — single table</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Moderate — temp table + 2 views</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;font-weight:bold;color:#1B5162;">Best for</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;">Non-critical streams with well-established rules</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;">Production pipelines with compliance, audit, or remediation needs</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="background:#F9F7F4;border-left:4px solid #4299E0;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#4299E0;">Recommendation:</strong> For enterprise production pipelines, the quarantine pattern is the preferred approach. It gives you zero data loss, per-constraint violation metrics in the pipeline UI, partition-pruned reads on clean data, and a recoverable quarantine table for root cause analysis and reprocessing — capabilities that <code>DROP ROW</code> cannot provide.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>