# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Prompt 1 — Ask Genie Code to explain the code:
# MAGIC Explain what this code is doing, step by step. What is the business purpose, what data does it join, and what does the final output represent?
# MAGIC What to observe: Genie Code reads the full cell, identifies the business logic (adverse event risk scoring, patient segmentation by age group), the join strategy, and the final output — all without you explaining the dataset first.
# MAGIC ———
# MAGIC Prompt 2 — Ask Genie Code to identify optimization opportunities:
# MAGIC This code works but may have performance issues at scale. What are all the optimization opportunities available? Explain each issue and show me the corrected version.
# MAGIC What to observe: Genie Code should identify several anti-patterns:
# MAGIC .collect() called on 25,000-row table (brings all data to the driver)
# MAGIC Python UDFs bypassing Spark's Catalyst optimizer — should use built-in when/otherwise
# MAGIC Data brought to Python dict for aggregation instead of using DataFrame groupBy
# MAGIC Full table overwrite on every run instead of MERGE for incremental updates
# MAGIC Missing column pruning (select only needed columns before joins)
# MAGIC No caching of joined_df despite multiple downstream operations
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, DoubleType
import datetime

spark = SparkSession.builder.getOrCreate()

# Load all tables
ae_df = spark.table("lakehouse_catalog.pharma_vigilance.adverse_events")
patients_df = spark.table("lakehouse_catalog.pharma_vigilance.patients")
drugs_df = spark.table("lakehouse_catalog.pharma_vigilance.drugs")
rx_df = spark.table("lakehouse_catalog.pharma_vigilance.prescriptions")

# UDF to classify severity into a numeric risk score
def severity_to_score(severity):
    if severity == "MILD":
        return 1.0
    elif severity == "MODERATE":
        return 2.0
    elif severity == "SEVERE":
        return 3.0
    else:
        return 0.0

severity_udf = udf(severity_to_score, DoubleType())

# UDF to compute patient age group
def age_group(age):
    if age is None:
        return "UNKNOWN"
    elif age < 18:
        return "PEDIATRIC"
    elif age < 65:
        return "ADULT"
    else:
        return "ELDERLY"

age_group_udf = udf(age_group, StringType())

# Collect all adverse events to driver and loop through
all_events = ae_df.collect()

high_risk_events = []
for row in all_events:
    score = severity_to_score(row["severity"].upper() if row["severity"] else "")
    if score >= 2.0:
        high_risk_events.append(row)

print("High risk event count: " + str(len(high_risk_events)))

# Recreate as DataFrame from collected rows
from pyspark.sql import Row
high_risk_df = spark.createDataFrame(high_risk_events)

# Join with patients and drugs (full join on each)
joined_df = high_risk_df \
    .join(patients_df, "patient_id") \
    .join(drugs_df, "drug_id") \
    .join(rx_df, ["patient_id", "drug_id"])

# Apply UDFs
result_df = joined_df \
    .withColumn("risk_score", severity_udf(col("severity"))) \
    .withColumn("age_group", age_group_udf(col("age")))

# Compute summary: collect again to Python for aggregation
summary_rows = result_df.select(
    "drug_name", "drug_class", "risk_score", "age_group", "outcome"
).collect()

summary = {}
for row in summary_rows:
    key = row["drug_class"] + "_" + row["age_group"]
    if key not in summary:
        summary[key] = {"total": 0, "severe_count": 0, "avg_risk": 0.0}
    summary[key]["total"] += 1
    if row["risk_score"] == 3.0:
        summary[key]["severe_count"] += 1
    summary[key]["avg_risk"] += row["risk_score"]

for key in summary:
    summary[key]["avg_risk"] = summary[key]["avg_risk"] / summary[key]["total"]

print(summary)

# Write result back — overwrite the entire table each time
result_df.write.format("delta").mode("overwrite") \
    .saveAsTable("lakehouse_catalog.pharma_vigilance.high_risk_analysis")
