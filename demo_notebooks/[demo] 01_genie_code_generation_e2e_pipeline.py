# Databricks notebook source
# DBTITLE 1,Step 1: Data Discovery
# MAGIC %md
# MAGIC # Genie Code Prompts : Data to E2E pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1: Data Discovery
# MAGIC
# MAGIC *"Let's say I'm a pharmacovigilance analyst who just joined the team. The first thing I need is to understand what data we have and how it fits together."*
# MAGIC
# MAGIC ### Prompt
# MAGIC > Search pharma_vigilance schema in pharma. Show me what tables and columns are available and summarize the data. Identify the key relationships between adverse\_events, prescriptions, patients, drugs, and conditions.
# MAGIC
# MAGIC ### What to highlight
# MAGIC * Genie Code uses Unity Catalog metadata to understand your data landscape instantly
# MAGIC * It discovers table schemas, column names, and infers relationships — no setup needed
# MAGIC * The `@table-name` syntax pulls schema context directly from Unity Catalog

# COMMAND ----------

# DBTITLE 1,Step 2: Exploratory Data Analysis
# MAGIC %md
# MAGIC ## Step 2: Exploratory Data Analysis
# MAGIC
# MAGIC *"Now let's explore the data for safety signals — the kind of analysis that would normally take a data team days to set up."*
# MAGIC
# MAGIC ### Prompt 2a
# MAGIC > Analyze the adverse\_events data in `pharma.pharma_vigilance`. What are the top insights? Show me adverse event rates by drug class, severity, and outcome. Highlight any concerning safety signals or unusual patterns.
# MAGIC
# MAGIC ### Prompt 2b
# MAGIC > Which drugs have the highest adverse event rates relative to their prescription volume? Cross-reference adverse\_events with prescriptions and drugs to find high-risk, high-volume drugs that need closer monitoring.
# MAGIC
# MAGIC ### Prompt 2c
# MAGIC > Analyze how patient demographics (age, weight, gender) correlate with adverse event severity. Are certain patient populations at higher risk for severe outcomes? Use the patients and conditions tables.
# MAGIC
# MAGIC ### What to highlight
# MAGIC * Genie Code creates a multi-step analysis plan and executes it autonomously
# MAGIC * It joins across multiple tables (adverse\_events, prescriptions, patients, drugs) automatically
# MAGIC * Generates visualizations and surfaces non-obvious patterns without manual coding

# COMMAND ----------

# DBTITLE 1,Step 3: ML Model — Predict Adverse Event Severity
# MAGIC %md
# MAGIC ## Step 3: ML Model — Predict Adverse Event Severity
# MAGIC
# MAGIC *"We've identified the patterns. Now let's build a predictive model so we can flag high-risk patients before adverse events happen — this is the kind of proactive safety intelligence that regulators and safety teams need."*
# MAGIC
# MAGIC ### Prompt 3a
# MAGIC > Build a machine learning model to predict whether an adverse event will be severe (SEVERE severity) vs mild/moderate. Use features from patients (age, weight, gender), drugs (drug\_class, route), prescriptions (dosage\_mg, frequency), and conditions (condition\_name). Train using scikit-learn, log the model with MLflow, and show feature importance and accuracy metrics.
# MAGIC
# MAGIC ### Prompt 3b
# MAGIC > Register the best model in Unity Catalog and evaluate it on a holdout test set. Show a confusion matrix and highlight which patient profiles are highest risk.
# MAGIC
# MAGIC ### What to highlight
# MAGIC * End-to-end ML pipeline across 4 joined tables — no boilerplate needed
# MAGIC * MLflow tracking and Unity Catalog model registration happen automatically
# MAGIC * Feature importance reveals the clinical drivers of severe outcomes

# COMMAND ----------

# DBTITLE 1,Step 4: Dashboard
# MAGIC %md
# MAGIC ## Step 4: Dashboard
# MAGIC
# MAGIC **Talk track:** *"Now let's make this accessible to safety officers and regulatory teams. I'll ask Genie Code to build a pharmacovigilance monitoring dashboard."*
# MAGIC
# MAGIC ### Prompt
# MAGIC > Create an AI/BI dashboard called "Pharmacovigilance Safety Monitor" with these charts:
# MAGIC > 1. Adverse event count trend over time by severity
# MAGIC > 2. Top 10 drugs by adverse event rate
# MAGIC > 3. Adverse event outcomes breakdown (recovered, recovering, fatal)
# MAGIC > 4. Patient risk distribution from our severity prediction model
# MAGIC > 5. Adverse events by reporter type (physician vs pharmacist vs patient)
# MAGIC
# MAGIC ### What to highlight
# MAGIC * Dashboard created from natural language — no drag-and-drop configuration needed
# MAGIC * References the ML model output from Step 3 automatically
# MAGIC * Can be shared with safety officers, medical affairs, and regulatory teams directly

# COMMAND ----------

# DBTITLE 1,Step 5: Lakeflow Pipeline
# MAGIC %md
# MAGIC ## Step 5: Lakeflow Pipeline
# MAGIC
# MAGIC **Talk track:** *"Finally, let's make this production-ready. Real pharmacovigilance systems need continuous data ingestion — I'll ask Genie Code to build the pipeline."*
# MAGIC
# MAGIC ### Prompt
# MAGIC > Create a Spark Declarative Pipeline that:
# MAGIC > 1. Ingests raw adverse event reports as a bronze table with Change Data Feed enabled
# MAGIC > 2. Cleans, deduplicates, and joins with patient and drug data as a silver table
# MAGIC > 3. Computes daily safety KPIs — adverse event rates per drug, severity distributions, and high-risk patient flags — as a gold table
# MAGIC >
# MAGIC > Use medallion architecture and store checkpoints in Volumes.
# MAGIC
# MAGIC ### What to highlight
# MAGIC * Full medallion architecture generated from one prompt, following Amgen's bronze/silver/gold conventions
# MAGIC * CDF and checkpoint paths go to Volumes (not DBFS) — aligned with Amgen standards
# MAGIC * Pipeline ready to schedule as a daily production job for continuous surveillance