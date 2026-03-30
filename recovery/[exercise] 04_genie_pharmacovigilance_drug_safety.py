# Databricks notebook source
# DBTITLE 1,Lab Overview
# MAGIC %md
# MAGIC # Lab: Build a Pharmacovigilance & Drug Safety Genie Space
# MAGIC
# MAGIC **Objective:** Create an AI/BI Genie space **from the UI** that enables natural-language querying of pharmacovigilance data — adverse events, drug safety profiles, patient demographics, and prescriptions.
# MAGIC
# MAGIC By the end of this lab you will have a fully configured Genie space with:
# MAGIC - **6 data tables** from `pharma_prod.pharma_vigilance`
# MAGIC - **Sample questions** for users to get started
# MAGIC - **Text instructions** with business rules and key definitions
# MAGIC - **SQL expressions** — reusable measures, filters, and dimensions
# MAGIC - **Join relationships** so Genie knows how tables relate
# MAGIC - **Example SQL queries** (static & parameterized) teaching Genie complex patterns
# MAGIC - **Benchmarks** to measure and track answer accuracy
# MAGIC
# MAGIC > **Naming convention:** Name your space **"Pharmacovigilance and Drug Safety – \<YOUR INITIALS\>"** (e.g., *Pharmacovigilance and Drug Safety – JD*).
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. Access to a **Pro or Serverless SQL warehouse**
# MAGIC 2. Read access to the `pharma_prod.pharma_vigilance` schema
# MAGIC 3. Permissions to create Genie spaces in your workspace
# MAGIC
# MAGIC Run the cell below to verify your data access.

# COMMAND ----------

# DBTITLE 1,Verify Data Access
# Verify access to the pharma_vigilance schema
for table in ["adverse_events_cleaned", "drug_safety_summary", "drugs", "patients", "prescriptions", "conditions"]:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM pharma_prod.pharma_vigilance.{table}").collect()[0]["cnt"]
        print(f"\u2705 {table}: {cnt:,} rows")
    except Exception as e:
        print(f"\u274c {table}: {e}")

# COMMAND ----------

# DBTITLE 1,Step 1 - Create the Space
# MAGIC %md
# MAGIC ## Step 1: Create the Genie Space
# MAGIC
# MAGIC 1. In the left sidebar, click **New** → **Genie space**
# MAGIC 2. Fill in:
# MAGIC    - **Name:** `Pharmacovigilance and Drug Safety – <YOUR INITIALS>`
# MAGIC    - **Description:** Copy the text below
# MAGIC    - **Warehouse:** Select a Pro or Serverless SQL warehouse
# MAGIC 3. Click **Create**
# MAGIC
# MAGIC ### Description (copy this):
# MAGIC
# MAGIC ```
# MAGIC This space supports comprehensive analysis of pharmacovigilance and drug safety, enabling monitoring of adverse events, patient outcomes, drug utilization, and related risk factors.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 2 - Add Tables
# MAGIC %md
# MAGIC ## Step 2: Add Data Tables
# MAGIC
# MAGIC In your Genie space, go to **Configure** (gear icon) → **Data** → **Add tables**.
# MAGIC
# MAGIC Add the following 6 tables from **`pharma_prod.pharma_vigilance`**:
# MAGIC
# MAGIC | # | Table | Purpose | Rows |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `adverse_events_cleaned` | Core adverse drug reaction reports (primary fact table) | \~23,698 |
# MAGIC | 2 | `drug_safety_summary` | Pre-aggregated safety stats by drug/event/severity | \~4,582 |
# MAGIC | 3 | `drugs` | Drug catalog — name, class, manufacturer, route | 50 |
# MAGIC | 4 | `patients` | Patient demographics — age, gender, weight, country | 5,000 |
# MAGIC | 5 | `prescriptions` | Prescription records — dosage, frequency, dates | 15,000 |
# MAGIC | 6 | `conditions` | Pre-existing medical conditions per patient | 8,000 |
# MAGIC
# MAGIC > **Tip:** After adding tables, verify that **prompt matching** (format assistance & entity matching) is enabled for key filter columns. Click on a column → **Advanced settings** to check. This is auto-enabled when adding tables via the UI.

# COMMAND ----------

# DBTITLE 1,Step 3 - Add Sample Questions
# MAGIC %md
# MAGIC ## Step 3: Add Sample Questions
# MAGIC
# MAGIC Go to **Configure** → **About** → **Common questions** and add these 5 starter questions:
# MAGIC
# MAGIC 1. `Which drugs have the highest rate of severe adverse events?`
# MAGIC 2. `Show me the trend of adverse events for Cardivex over the last 6 months`
# MAGIC 3. `What are the most common adverse events for SSRI drugs?`
# MAGIC 4. `How many fatal adverse events have been reported for each drug class?`
# MAGIC 5. `What is the average days to onset for hepatotoxicity across all drugs?`

# COMMAND ----------

# DBTITLE 1,Step 4 - Add General Instructions
# MAGIC %md
# MAGIC ## Step 4: Add Text Instructions
# MAGIC
# MAGIC Go to **Configure** → **Instructions**
# MAGIC
# MAGIC Copy and paste the full block below:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ```
# MAGIC ## General Instructions
# MAGIC
# MAGIC This space provides pharmacovigilance and drug safety analytics across 6 tables in pharma_prod.pharma_vigilance. The data spans from March 2024 to March 2026.
# MAGIC
# MAGIC ### Data Overview
# MAGIC
# MAGIC - adverse_events_cleaned (~23,698 rows): Core adverse drug reaction reports. Each row is one reported event with severity, outcome, event type, reporter type, and days to onset. This is the primary fact table.
# MAGIC - drug_safety_summary (~4,582 rows): Pre-aggregated safety statistics by drug, event type, and severity. Contains event counts, fatal counts, and average days to onset. Use for quick aggregate lookups.
# MAGIC - drugs (50 rows): Drug reference table. Contains drug name, generic name, drug class, manufacturer, route, and approval date.
# MAGIC - patients (5,000 rows): Patient demographics including age, gender, weight (kg), country, and enrollment date.
# MAGIC - prescriptions (15,000 rows): Prescription records linking patients to drugs with dosage (mg), frequency, start/end dates.
# MAGIC - conditions (8,000 rows): Pre-existing medical conditions for patients, with diagnosis dates.
# MAGIC
# MAGIC ### Table Relationships
# MAGIC
# MAGIC - patient_id links patients, adverse_events_cleaned, prescriptions, and conditions
# MAGIC - drug_id links drugs, adverse_events_cleaned, and prescriptions
# MAGIC - drug_name links drug_safety_summary to drugs and adverse_events_cleaned
# MAGIC
# MAGIC ### Key Definitions and Business Rules
# MAGIC
# MAGIC - Severity levels (ordered): mild, moderate, severe, life-threatening
# MAGIC - Outcomes: recovered, recovering, not_recovered, unknown, fatal
# MAGIC - Reporter types: physician, nurse, pharmacist, other_health_professional, patient
# MAGIC - Drug classes (10): Statin, SSRI, NSAID, ACE Inhibitor, Anticoagulant, Beta-Blocker, PPI, Antidiabetic, Antibiotic, Antihistamine
# MAGIC - Routes: oral, subcutaneous, intravenous
# MAGIC - Frequencies: once_daily, twice_daily, three_times_daily, as_needed
# MAGIC - Countries (10): US, UK, Germany, Japan, India, France, Canada, Brazil, South_Korea, Australia
# MAGIC - days_to_onset: Days between starting the drug and experiencing the adverse event. Lower = faster onset.
# MAGIC - Fatal rate: Calculate as fatal_count / event_count from drug_safety_summary, or count outcomes = 'fatal' from adverse_events_cleaned.
# MAGIC - When asked about "serious" events, include both severity = 'severe' and severity = 'life-threatening'.
# MAGIC - When asked about "common" adverse events, rank by event count descending.
# MAGIC - Use drug_safety_summary for aggregate drug-level questions (faster). Use adverse_events_cleaned joined with other tables for patient-level or time-series analysis.
# MAGIC - Condition names use underscores (e.g., type_2_diabetes, chronic_kidney_disease).
# MAGIC - Event types also use underscores (e.g., C_diff_infection, QT_prolongation, GI_bleeding).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 5 - Add SQL Expressions
# MAGIC %md
# MAGIC ## Step 5: Add SQL Expressions
# MAGIC
# MAGIC Go to **Configure** → **Instructions** → **SQL expressions** → **Add**.
# MAGIC
# MAGIC For each expression, select the **type** of expression, enter the **name**, paste the **SQL code**, and optionally add synonyms.
# MAGIC
# MAGIC > **Important:** All column references must be **table-qualified** (e.g., `adverse_events_cleaned.severity`). Filters must **not** include the `WHERE` keyword.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Measures
# MAGIC
# MAGIC **1. fatal_rate**
# MAGIC - Synonyms: `fatality rate`, `death rate`, `mortality rate`
# MAGIC ```
# MAGIC ROUND(SUM(drug_safety_summary.fatal_count) * 100.0 / SUM(drug_safety_summary.event_count), 2)
# MAGIC ```
# MAGIC
# MAGIC **2. total_events**
# MAGIC - Synonyms: `event count`, `number of events`, `total adverse events`
# MAGIC ```
# MAGIC COUNT(*)
# MAGIC ```
# MAGIC
# MAGIC **3. avg_days_to_onset**
# MAGIC - Synonyms: `mean onset time`, `average onset days`, `time to onset`
# MAGIC ```
# MAGIC ROUND(AVG(adverse_events_cleaned.days_to_onset), 1)
# MAGIC ```
# MAGIC
# MAGIC **4. serious_event_count**
# MAGIC - Synonyms: `serious events`, `severe event count`
# MAGIC ```
# MAGIC COUNT(CASE WHEN adverse_events_cleaned.severity IN ('severe', 'life-threatening') THEN 1 END)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Filters
# MAGIC
# MAGIC **1. serious events**
# MAGIC - Synonyms: `serious`, `severe events`, `critical events`
# MAGIC ```
# MAGIC adverse_events_cleaned.severity IN ('severe', 'life-threatening')
# MAGIC ```
# MAGIC
# MAGIC **2. fatal outcome**
# MAGIC - Synonyms: `deaths`, `fatalities`, `deadly`, `lethal`
# MAGIC ```
# MAGIC adverse_events_cleaned.outcome = 'fatal'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dimensions
# MAGIC
# MAGIC **1. event_year**
# MAGIC - Synonyms: `year`, `reporting year`
# MAGIC ```
# MAGIC YEAR(adverse_events_cleaned.event_date)
# MAGIC ```
# MAGIC
# MAGIC **2. event_month**
# MAGIC - Synonyms: `month`, `reporting month`
# MAGIC ```
# MAGIC DATE_TRUNC('month', adverse_events_cleaned.event_date)
# MAGIC ```
# MAGIC
# MAGIC **3. age_group**
# MAGIC - Synonyms: `age bracket`, `age range`, `age category`
# MAGIC ```
# MAGIC CASE WHEN patients.age < 30 THEN 'Under 30' WHEN patients.age < 50 THEN '30-49' WHEN patients.age < 65 THEN '50-64' ELSE '65+' END
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 6 - Add Join Relationships
# MAGIC %md
# MAGIC ## Step 6: Define Join Relationships
# MAGIC
# MAGIC Go to **Configure** → **Instructions** → **Join ** → **Add**.
# MAGIC
# MAGIC Define these 5 joins:
# MAGIC
# MAGIC | # | Left Table | Right Table | Join Condition | Relationship Type |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `adverse_events_cleaned` | `drugs` | `adverse_events_cleaned.drug_id = drugs.drug_id` | Many to One |
# MAGIC | 2 | `adverse_events_cleaned` | `patients` | `adverse_events_cleaned.patient_id = patients.patient_id` | Many to One |
# MAGIC | 3 | `prescriptions` | `drugs` | `prescriptions.drug_id = drugs.drug_id` | Many to One |
# MAGIC | 4 | `prescriptions` | `patients` | `prescriptions.patient_id = patients.patient_id` | Many to One |
# MAGIC | 5 | `conditions` | `patients` | `conditions.patient_id = patients.patient_id` | Many to One |
# MAGIC
# MAGIC > **Why these matter:** Without explicit joins, Genie may not know how to combine tables correctly. For example, answering *"fatal events by drug class"* requires joining `adverse_events_cleaned` to `drugs`.

# COMMAND ----------

# DBTITLE 1,Step 7 - Add Example SQL Queries
# MAGIC %md
# MAGIC ## Step 7: Add Example SQL Queries
# MAGIC
# MAGIC Go to **Configure** → **Instructions** → **SQL queries** → **Add**.
# MAGIC
# MAGIC For each query, enter the **question** and paste the **SQL**. Queries with `:parameter_name` are **parameterized** — Genie will prompt users for the value.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 1 (Static) — Drugs with highest serious AE rate
# MAGIC
# MAGIC **Question:** `Which drugs have the highest rate of severe adverse events?`
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.drug_name,
# MAGIC   d.drug_class,
# MAGIC   COUNT(CASE WHEN ae.severity IN ('severe', 'life-threatening') THEN 1 END) AS serious_event_count,
# MAGIC   COUNT(*) AS total_event_count,
# MAGIC   ROUND(COUNT(CASE WHEN ae.severity IN ('severe', 'life-threatening') THEN 1 END) * 100.0 / COUNT(*), 2) AS serious_event_rate_pct
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC JOIN pharma_prod.pharma_vigilance.drugs d ON ae.drug_id = d.drug_id
# MAGIC GROUP BY d.drug_name, d.drug_class
# MAGIC ORDER BY serious_event_rate_pct DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 2 (Parameterized) — Monthly trend for a drug
# MAGIC
# MAGIC **Question:** `Show me the monthly trend of adverse events for a specific drug`
# MAGIC
# MAGIC **Parameter:** `drug_name` (String, default: `Cardivex`)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', ae.event_date) AS event_month,
# MAGIC   COUNT(*) AS event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC WHERE ae.drug_name = :drug_name
# MAGIC GROUP BY DATE_TRUNC('month', ae.event_date)
# MAGIC ORDER BY event_month
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 3 (Parameterized) — Most common AEs for a drug class
# MAGIC
# MAGIC **Question:** `What are the most common adverse events for a specific drug class?`
# MAGIC
# MAGIC **Parameter:** `drug_class` (String, default: `SSRI`)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ae.event_type,
# MAGIC   COUNT(*) AS event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC JOIN pharma_prod.pharma_vigilance.drugs d ON ae.drug_id = d.drug_id
# MAGIC WHERE d.drug_class = :drug_class
# MAGIC GROUP BY ae.event_type
# MAGIC ORDER BY event_count DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 4 (Static) — Fatal events by drug class
# MAGIC
# MAGIC **Question:** `How many fatal adverse events have been reported for each drug class?`
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.drug_class,
# MAGIC   COUNT(*) AS fatal_event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC JOIN pharma_prod.pharma_vigilance.drugs d ON ae.drug_id = d.drug_id
# MAGIC WHERE ae.outcome = 'fatal'
# MAGIC GROUP BY d.drug_class
# MAGIC ORDER BY fatal_event_count DESC
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 5 (Parameterized) — Avg days to onset by event type
# MAGIC
# MAGIC **Question:** `What is the average days to onset for a specific event type across all drugs?`
# MAGIC
# MAGIC **Parameter:** `event_type` (String, default: `hepatotoxicity`)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ae.drug_name,
# MAGIC   ROUND(AVG(ae.days_to_onset), 1) AS avg_days_to_onset,
# MAGIC   COUNT(*) AS event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC WHERE ae.event_type = :event_type
# MAGIC GROUP BY ae.drug_name
# MAGIC ORDER BY avg_days_to_onset
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 6 (Parameterized) — Drug safety profile
# MAGIC
# MAGIC **Question:** `What is the safety profile of a specific drug broken down by severity and outcome?`
# MAGIC
# MAGIC **Parameter:** `drug_name` (String, default: `Cardivex`)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ae.severity,
# MAGIC   ae.outcome,
# MAGIC   COUNT(*) AS event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC WHERE ae.drug_name = :drug_name
# MAGIC GROUP BY ae.severity, ae.outcome
# MAGIC ORDER BY ae.severity, ae.outcome
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Query 7 (Static) — Patient demographics & adverse events
# MAGIC
# MAGIC **Question:** `Which patient demographics are associated with the most adverse events by country and gender?`
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   p.country,
# MAGIC   p.gender,
# MAGIC   COUNT(*) AS event_count,
# MAGIC   ROUND(AVG(p.age), 1) AS avg_age
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC JOIN pharma_prod.pharma_vigilance.patients p ON ae.patient_id = p.patient_id
# MAGIC WHERE p.gender IS NOT NULL
# MAGIC GROUP BY p.country, p.gender
# MAGIC ORDER BY event_count DESC
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 8 - Add Benchmarks
# MAGIC %md
# MAGIC ## Step 8: Add Benchmarks
# MAGIC
# MAGIC Benchmarks let you measure how accurately Genie answers questions. Go to **Configure** → **Benchmarks** → **Add question**.
# MAGIC
# MAGIC For each benchmark, enter the **question** and the **expected SQL** (ground truth). After adding all benchmarks, click **Run all** to evaluate accuracy.
# MAGIC
# MAGIC ### Core Benchmarks (rephrased versions of example SQL queries)
# MAGIC
# MAGIC These test whether Genie handles alternate phrasings of questions it has example SQL for:
# MAGIC
# MAGIC | # | Benchmark Question | Ground Truth = Same SQL As |
# MAGIC |---|---|---|
# MAGIC | 1 | `Which drugs have the highest rate of severe adverse events?` | Query 1 |
# MAGIC | 2 | `Rank drugs by their serious adverse event rate` | Query 1 |
# MAGIC | 3 | `What medications have the most severe side effect rates?` | Query 1 |
# MAGIC | 4 | `How many fatal adverse events have been reported for each drug class?` | Query 4 |
# MAGIC | 5 | `Show fatality counts by therapeutic drug class` | Query 4 |
# MAGIC | 6 | `Which drug classes have the most deaths from adverse events?` | Query 4 |
# MAGIC | 7 | `Break down adverse event counts by country and gender with average age` | Query 7 |
# MAGIC
# MAGIC ### Stretch Benchmarks (new questions without example SQL)
# MAGIC
# MAGIC These test Genie's ability to generalize:
# MAGIC
# MAGIC **8.** `What are the top 5 drugs by total number of adverse events?`
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ae.drug_name,
# MAGIC   COUNT(*) AS total_events
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC GROUP BY ae.drug_name
# MAGIC ORDER BY total_events DESC
# MAGIC LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC **9.** `How many adverse events were reported in each country?`
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   p.country,
# MAGIC   COUNT(*) AS event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC JOIN pharma_prod.pharma_vigilance.patients p ON ae.patient_id = p.patient_id
# MAGIC GROUP BY p.country
# MAGIC ORDER BY event_count DESC
# MAGIC ```
# MAGIC
# MAGIC **10.** `What is the distribution of adverse event severity levels?`
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ae.severity,
# MAGIC   COUNT(*) AS event_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC GROUP BY ae.severity
# MAGIC ORDER BY event_count DESC
# MAGIC ```
# MAGIC
# MAGIC **11.** `Which event types have the highest fatality count across all drugs?`
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ae.event_type,
# MAGIC   COUNT(*) AS fatal_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC WHERE ae.outcome = 'fatal'
# MAGIC GROUP BY ae.event_type
# MAGIC ORDER BY fatal_count DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC **12.** `What are the top 5 pre-existing conditions among patients with fatal adverse events?`
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   c.condition_name,
# MAGIC   COUNT(*) AS patient_count
# MAGIC FROM pharma_prod.pharma_vigilance.adverse_events_cleaned ae
# MAGIC JOIN pharma_prod.pharma_vigilance.conditions c ON ae.patient_id = c.patient_id
# MAGIC WHERE ae.outcome = 'fatal'
# MAGIC GROUP BY c.condition_name
# MAGIC ORDER BY patient_count DESC
# MAGIC LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC **13.** `What is the average dosage in mg for each drug class?`
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.drug_class,
# MAGIC   ROUND(AVG(rx.dosage_mg), 1) AS avg_dosage_mg
# MAGIC FROM pharma_prod.pharma_vigilance.prescriptions rx
# MAGIC JOIN pharma_prod.pharma_vigilance.drugs d ON rx.drug_id = d.drug_id
# MAGIC GROUP BY d.drug_class
# MAGIC ORDER BY avg_dosage_mg DESC
# MAGIC ```
# MAGIC
# MAGIC > 🎯 **Target:** Aim for 80%+ accuracy on core benchmarks and 60%+ on stretch benchmarks. Iterate by adding more example SQL queries for questions Genie gets wrong.

# COMMAND ----------

# DBTITLE 1,Step 8 - Test Your Space
# MAGIC %md
# MAGIC ## Step 9: Test Your Genie Space ✅
# MAGIC
# MAGIC Open your Genie space and try these test questions:
# MAGIC
# MAGIC 1. Click a **sample question** — verify it returns results
# MAGIC 2. `What are the top 5 drugs by total adverse events?`
# MAGIC 3. `Show me fatal events by country`
# MAGIC 4. `What is the fatality rate for each NSAID drug?`
# MAGIC 5. `Show me the trend of adverse events for Neurazol` (tests parameterized query)
# MAGIC
# MAGIC ### Checklist
# MAGIC
# MAGIC - [ ] Correct tables are being joined
# MAGIC - [ ] Filters work (“serious events” triggers severity IN ('severe','life-threatening'))
# MAGIC - [ ] Parameterized queries prompt for input values
# MAGIC - [ ] SQL expressions are applied (e.g., “fatal rate” uses the defined measure)
# MAGIC - [ ] Demographics queries join patients correctly
# MAGIC - [ ] Benchmarks run and show accuracy scores
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC | Problem | Solution |
# MAGIC |---|---|
# MAGIC | Wrong table or column used | Check column descriptions and join relationships |
# MAGIC | Filter values don’t match (e.g., “California” vs “CA”) | Verify entity matching is enabled in column Advanced settings |
# MAGIC | Joins fail or produce wrong results | Double-check join relationships in Knowledge store |
# MAGIC | Instructions seem ignored | Start a **new chat** to test (prior context influences responses) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏁 Congratulations!
# MAGIC You’ve built a production-ready Genie space with text instructions, SQL expressions, join relationships, parameterized example queries, and benchmarks. Next steps to explore on your own:
# MAGIC
# MAGIC - Add more **example SQL queries** for questions Genie gets wrong
# MAGIC - Create **SQL functions** (UDFs) for complex multi-step logic
# MAGIC - Add **clarification question instructions** for ambiguous prompts
# MAGIC - Iterate based on **benchmark results** — aim for 90%+ accuracy

# COMMAND ----------

# DBTITLE 1,Appendix - Explore the Data
# MAGIC %md
# MAGIC ---
# MAGIC ## Appendix: Explore the Data
# MAGIC
# MAGIC Run the cells below to familiarize yourself with the tables and key column values before building the space.

# COMMAND ----------

# DBTITLE 1,Preview adverse events
# MAGIC %sql
# MAGIC SELECT * FROM pharma_prod.pharma_vigilance.adverse_events_cleaned LIMIT 5

# COMMAND ----------

# DBTITLE 1,Preview drugs catalog
# MAGIC %sql
# MAGIC SELECT * FROM pharma_prod.pharma_vigilance.drugs LIMIT 10

# COMMAND ----------

# DBTITLE 1,Key categorical values
# MAGIC %sql
# MAGIC SELECT 'severity' AS column_name, severity AS value FROM pharma_prod.pharma_vigilance.adverse_events_cleaned GROUP BY severity
# MAGIC UNION ALL
# MAGIC SELECT 'outcome', outcome FROM pharma_prod.pharma_vigilance.adverse_events_cleaned GROUP BY outcome
# MAGIC UNION ALL
# MAGIC SELECT 'drug_class', drug_class FROM pharma_prod.pharma_vigilance.drugs GROUP BY drug_class
# MAGIC UNION ALL
# MAGIC SELECT 'country', country FROM pharma_prod.pharma_vigilance.patients GROUP BY country
# MAGIC ORDER BY column_name, value