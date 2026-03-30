# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineer Design Questions for Genie Code
# MAGIC
# MAGIC Use these prompts to demonstrate Genie Code's ability to give architecture and design guidance using Databricks best practices. Each is a real design challenge without an obvious answer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Design Question 1 — Time-Windowed Streaming with Blackout Hours**
# MAGIC
# MAGIC > **Scenario:**  
# MAGIC I'm running a Lakeflow Spark Declarative Pipeline in continuous mode to process real-time adverse event reports from hospital systems.  
# MAGIC Our **GxP validation requirements** say the pipeline must **NOT process or emit data outside validated operating hours (6am–10pm local time)**.  
# MAGIC I also need to handle catch-up when the pipeline was down during operating hours — but even during catch-up I must not process data outside the allowed window.  
# MAGIC The source is a Kafka topic with 7-day retention.
# MAGIC
# MAGIC **Questions:**
# MAGIC - How do I architect this — do I control it at the pipeline trigger level, the Lakeflow Jobs schedule, or inside the pipeline logic itself?
# MAGIC - What happens to Kafka offsets and checkpoints during the blackout window?
# MAGIC
# MAGIC **Why this is a great design prompt:**  
# MAGIC There's no single obviously correct answer. Genie Code should reason through:
# MAGIC - Triggered vs. continuous mode tradeoffs
# MAGIC - Whether to gate at the orchestration layer (Jobs schedule) vs. inside pipeline logic
# MAGIC - How Kafka consumer group offsets behave during pauses
# MAGIC - The risk of offset lag accumulation across a multi-hour blackout  
# MAGIC The **GxP constraint makes the answer non-negotiable**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Design Question 2 — Schema Evolution Across a Multi-Source Bronze Layer**
# MAGIC
# MAGIC > **Scenario:**  
# MAGIC We ingest lab instrument data from **12 different vendor systems** into a single Bronze Delta table using Auto Loader with schema inference.  
# MAGIC Each vendor occasionally adds new columns without warning (**schema drift**).  
# MAGIC We run this as a Lakeflow streaming pipeline.
# MAGIC
# MAGIC **Questions:**
# MAGIC - What's the right strategy — use `schemaEvolutionMode = addNewColumns`, rescue unknown columns into a `_rescued_data` column, or split into per-vendor Bronze tables?
# MAGIC - What are the downstream implications for our Silver `APPLY CHANGES INTO` pipeline and data quality expectations when the schema shifts mid-stream?
# MAGIC
# MAGIC **Why this is a great design prompt:**  
# MAGIC Requires Genie Code to compare three legitimate approaches, reason about the downstream impact on `APPLY CHANGES INTO`, and explain how DLT expectations interact with a shifting schema.  
# MAGIC The right answer depends on how strict downstream consumers are — there's genuine architectural judgment involved.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Design Question 3 — SCD Type 2 Placement in Medallion Architecture**
# MAGIC
# MAGIC > **Scenario:**  
# MAGIC We're designing a Unity Catalog medallion architecture for patient master data.  
# MAGIC Business needs **full history of every demographic change (SCD Type 2)** for regulatory audit trails, but also needs a **current-state view for real-time drug safety lookups**.
# MAGIC
# MAGIC **Questions:**
# MAGIC - Should we (A) put SCD Type 2 history in Silver and serve current-state via a view, or (B) keep Silver as SCD Type 1 and do SCD Type 2 only in Gold?
# MAGIC - Should we use Lakeflow's native `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` or build it with hand-rolled MERGE?
# MAGIC - What are the write amplification and query performance tradeoffs of each approach?
# MAGIC
# MAGIC **Why this is a great design prompt:**  
# MAGIC Requires Genie Code to take a position on a genuinely debated architecture question (Silver vs. Gold for SCD history), explain the native `APPLY CHANGES INTO SCD TYPE 2` capability vs. hand-rolled MERGE, and reason about write amplification from bi-temporal data patterns.  
# MAGIC Pharma regulatory audit context adds real-world pressure that changes the answer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Design Question 4 — Liquid Clustering vs. Partitioning for a High-Churn Gold Table**
# MAGIC
# MAGIC > **Scenario:**  
# MAGIC We have a Gold-layer Delta table with **5 years of clinical study observations** — roughly 8 TB, appended daily with ~50M new rows, and queried heavily by `study_id`, `site_id`, and `visit_date`.  
# MAGIC We Hive-partitioned by year/month but see **data skew** because some months have 10x more rows than others due to trial enrollment spikes.  
# MAGIC We also run frequent MERGE operations for late-arriving corrections, and OPTIMIZE after each MERGE is expensive.
# MAGIC
# MAGIC **Questions:**
# MAGIC - Should we migrate to **Liquid Clustering** (on `study_id`, `visit_date`), and if so: what is the migration path, what are the write amplification implications from our daily MERGE + OPTIMIZE cycle, and when would traditional partitioning still be the better choice?
# MAGIC
# MAGIC **Why this is a great design prompt:**  
# MAGIC Requires Databricks-specific knowledge that **Liquid Clustering and PARTITION BY are mutually exclusive**, when traditional partitioning is still the right call for very high cardinality keys, how incremental OPTIMIZE interacts with clustering, and a concrete migration path.  
# MAGIC The data skew from uneven trial enrollment is a realistic pharma scenario that changes the optimal answer.