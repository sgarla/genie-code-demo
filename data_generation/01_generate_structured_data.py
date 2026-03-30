# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Structured Data — Pharmacovigilance Hackathon
# MAGIC
# MAGIC Generates 5 tables as Parquet → Unity Catalog Volume, then registers them as Delta tables.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `patients` | ~5,000 | Patient demographics |
# MAGIC | `drugs` | ~50 | Drug catalog |
# MAGIC | `adverse_events` | ~25,000 | Reported adverse events |
# MAGIC | `conditions` | ~8,000 | Pre-existing patient conditions |
# MAGIC | `prescriptions` | ~15,000 | Prescription records |
# MAGIC
# MAGIC ~5% intentional dirty data for pipeline cleaning challenges.

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import random
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_q4xxoi_usama_catalog", "Catalog")
dbutils.widgets.text("schema", "pharma_vigilance", "Schema")
dbutils.widgets.text("volume_structured", "structured_data", "Structured Data Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume_structured")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"Volume:  {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema and Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print("Schema and volume ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Drug Catalog
# MAGIC
# MAGIC 50 synthetic but realistic drug names across common therapeutic classes.

# COMMAND ----------

DRUG_CATALOG = [
    # Statins
    {"drug_name": "Cardivex", "generic_name": "atorvastatin-X", "manufacturer": "NovaPharm", "drug_class": "Statin", "route": "oral", "approved_date": "2018-03-15"},
    {"drug_name": "Lipovant", "generic_name": "rosuvastatin-V", "manufacturer": "GenMed Labs", "drug_class": "Statin", "route": "oral", "approved_date": "2019-07-22"},
    {"drug_name": "Cholestix", "generic_name": "simvastatin-C", "manufacturer": "PharmaCure", "drug_class": "Statin", "route": "oral", "approved_date": "2017-01-10"},
    {"drug_name": "Statirol", "generic_name": "pravastatin-S", "manufacturer": "BioVida", "drug_class": "Statin", "route": "oral", "approved_date": "2020-05-08"},
    {"drug_name": "Lipidex", "generic_name": "fluvastatin-L", "manufacturer": "MediCore", "drug_class": "Statin", "route": "oral", "approved_date": "2016-11-30"},
    # SSRIs
    {"drug_name": "Neurazol", "generic_name": "sertraline-N", "manufacturer": "MindWell", "drug_class": "SSRI", "route": "oral", "approved_date": "2017-06-14"},
    {"drug_name": "Serenix", "generic_name": "fluoxetine-S", "manufacturer": "NovaPharm", "drug_class": "SSRI", "route": "oral", "approved_date": "2018-09-03"},
    {"drug_name": "Calmitex", "generic_name": "citalopram-C", "manufacturer": "NeuroGen", "drug_class": "SSRI", "route": "oral", "approved_date": "2019-02-18"},
    {"drug_name": "Moodafix", "generic_name": "escitalopram-M", "manufacturer": "GenMed Labs", "drug_class": "SSRI", "route": "oral", "approved_date": "2020-11-25"},
    {"drug_name": "Anxiorel", "generic_name": "paroxetine-A", "manufacturer": "PharmaCure", "drug_class": "SSRI", "route": "oral", "approved_date": "2016-08-07"},
    # NSAIDs
    {"drug_name": "Inflamax", "generic_name": "ibuprofen-I", "manufacturer": "PainFree Inc", "drug_class": "NSAID", "route": "oral", "approved_date": "2015-04-20"},
    {"drug_name": "Dolorix", "generic_name": "naproxen-D", "manufacturer": "MediCore", "drug_class": "NSAID", "route": "oral", "approved_date": "2016-12-01"},
    {"drug_name": "Arthroven", "generic_name": "diclofenac-A", "manufacturer": "BioVida", "drug_class": "NSAID", "route": "oral", "approved_date": "2017-03-28"},
    {"drug_name": "Painzero", "generic_name": "celecoxib-P", "manufacturer": "NovaPharm", "drug_class": "NSAID", "route": "oral", "approved_date": "2018-07-15"},
    {"drug_name": "Flexidol", "generic_name": "meloxicam-F", "manufacturer": "GenMed Labs", "drug_class": "NSAID", "route": "oral", "approved_date": "2019-10-09"},
    # ACE Inhibitors
    {"drug_name": "PressDown", "generic_name": "lisinopril-P", "manufacturer": "CardioMed", "drug_class": "ACE Inhibitor", "route": "oral", "approved_date": "2016-05-17"},
    {"drug_name": "Vasotrim", "generic_name": "enalapril-V", "manufacturer": "NovaPharm", "drug_class": "ACE Inhibitor", "route": "oral", "approved_date": "2017-08-22"},
    {"drug_name": "Renapril", "generic_name": "ramipril-R", "manufacturer": "PharmaCure", "drug_class": "ACE Inhibitor", "route": "oral", "approved_date": "2018-01-30"},
    {"drug_name": "Angioblock", "generic_name": "captopril-A", "manufacturer": "BioVida", "drug_class": "ACE Inhibitor", "route": "oral", "approved_date": "2015-09-12"},
    {"drug_name": "Hypertrol", "generic_name": "benazepril-H", "manufacturer": "MediCore", "drug_class": "ACE Inhibitor", "route": "oral", "approved_date": "2019-06-05"},
    # Anticoagulants
    {"drug_name": "Clotrix", "generic_name": "rivaroxaban-C", "manufacturer": "HemaGen", "drug_class": "Anticoagulant", "route": "oral", "approved_date": "2018-04-11"},
    {"drug_name": "Thrombonil", "generic_name": "apixaban-T", "manufacturer": "NovaPharm", "drug_class": "Anticoagulant", "route": "oral", "approved_date": "2017-10-19"},
    {"drug_name": "Coagustop", "generic_name": "dabigatran-C", "manufacturer": "CardioMed", "drug_class": "Anticoagulant", "route": "oral", "approved_date": "2019-01-25"},
    {"drug_name": "Venoshield", "generic_name": "edoxaban-V", "manufacturer": "GenMed Labs", "drug_class": "Anticoagulant", "route": "oral", "approved_date": "2020-03-14"},
    {"drug_name": "Heparex", "generic_name": "enoxaparin-H", "manufacturer": "BioVida", "drug_class": "Anticoagulant", "route": "subcutaneous", "approved_date": "2016-07-08"},
    # Beta-Blockers
    {"drug_name": "Rhythmox", "generic_name": "metoprolol-R", "manufacturer": "CardioMed", "drug_class": "Beta-Blocker", "route": "oral", "approved_date": "2017-02-14"},
    {"drug_name": "Betacalm", "generic_name": "atenolol-B", "manufacturer": "NovaPharm", "drug_class": "Beta-Blocker", "route": "oral", "approved_date": "2016-06-30"},
    {"drug_name": "Heartease", "generic_name": "propranolol-H", "manufacturer": "PharmaCure", "drug_class": "Beta-Blocker", "route": "oral", "approved_date": "2018-11-20"},
    {"drug_name": "Pulsefix", "generic_name": "bisoprolol-P", "manufacturer": "MediCore", "drug_class": "Beta-Blocker", "route": "oral", "approved_date": "2019-04-17"},
    {"drug_name": "Cardiozen", "generic_name": "carvedilol-C", "manufacturer": "BioVida", "drug_class": "Beta-Blocker", "route": "oral", "approved_date": "2020-08-03"},
    # Proton Pump Inhibitors
    {"drug_name": "Gastronil", "generic_name": "omeprazole-G", "manufacturer": "DigestiCare", "drug_class": "PPI", "route": "oral", "approved_date": "2016-03-25"},
    {"drug_name": "Acidblock", "generic_name": "lansoprazole-A", "manufacturer": "NovaPharm", "drug_class": "PPI", "route": "oral", "approved_date": "2017-07-12"},
    {"drug_name": "Refluxend", "generic_name": "pantoprazole-R", "manufacturer": "GenMed Labs", "drug_class": "PPI", "route": "oral", "approved_date": "2018-10-05"},
    {"drug_name": "Stomacure", "generic_name": "esomeprazole-S", "manufacturer": "PharmaCure", "drug_class": "PPI", "route": "oral", "approved_date": "2019-12-18"},
    {"drug_name": "Ulceraid", "generic_name": "rabeprazole-U", "manufacturer": "DigestiCare", "drug_class": "PPI", "route": "oral", "approved_date": "2020-02-28"},
    # Antidiabetics
    {"drug_name": "Glucomin", "generic_name": "metformin-G", "manufacturer": "EndoMed", "drug_class": "Antidiabetic", "route": "oral", "approved_date": "2015-08-14"},
    {"drug_name": "Insulex", "generic_name": "glipizide-I", "manufacturer": "NovaPharm", "drug_class": "Antidiabetic", "route": "oral", "approved_date": "2016-11-22"},
    {"drug_name": "Diabetrol", "generic_name": "sitagliptin-D", "manufacturer": "GenMed Labs", "drug_class": "Antidiabetic", "route": "oral", "approved_date": "2017-05-09"},
    {"drug_name": "Sugardown", "generic_name": "empagliflozin-S", "manufacturer": "EndoMed", "drug_class": "Antidiabetic", "route": "oral", "approved_date": "2018-09-30"},
    {"drug_name": "Pancrefix", "generic_name": "liraglutide-P", "manufacturer": "BioVida", "drug_class": "Antidiabetic", "route": "subcutaneous", "approved_date": "2019-03-17"},
    # Antibiotics
    {"drug_name": "Bactercide", "generic_name": "amoxicillin-B", "manufacturer": "InfectiGuard", "drug_class": "Antibiotic", "route": "oral", "approved_date": "2016-01-05"},
    {"drug_name": "Septikill", "generic_name": "azithromycin-S", "manufacturer": "NovaPharm", "drug_class": "Antibiotic", "route": "oral", "approved_date": "2017-04-18"},
    {"drug_name": "Germaway", "generic_name": "ciprofloxacin-G", "manufacturer": "PharmaCure", "drug_class": "Antibiotic", "route": "oral", "approved_date": "2018-08-11"},
    {"drug_name": "Microbex", "generic_name": "doxycycline-M", "manufacturer": "InfectiGuard", "drug_class": "Antibiotic", "route": "oral", "approved_date": "2019-06-24"},
    {"drug_name": "Infectrol", "generic_name": "levofloxacin-I", "manufacturer": "GenMed Labs", "drug_class": "Antibiotic", "route": "intravenous", "approved_date": "2020-01-15"},
    # Antihistamines
    {"drug_name": "Allergex", "generic_name": "cetirizine-A", "manufacturer": "AllerFree", "drug_class": "Antihistamine", "route": "oral", "approved_date": "2016-04-10"},
    {"drug_name": "Histabloc", "generic_name": "loratadine-H", "manufacturer": "NovaPharm", "drug_class": "Antihistamine", "route": "oral", "approved_date": "2017-09-28"},
    {"drug_name": "Sneezaway", "generic_name": "fexofenadine-S", "manufacturer": "AllerFree", "drug_class": "Antihistamine", "route": "oral", "approved_date": "2018-12-03"},
    {"drug_name": "Rhinoclear", "generic_name": "desloratadine-R", "manufacturer": "PharmaCure", "drug_class": "Antihistamine", "route": "oral", "approved_date": "2019-07-16"},
    {"drug_name": "Itchnil", "generic_name": "diphenhydramine-I", "manufacturer": "MediCore", "drug_class": "Antihistamine", "route": "oral", "approved_date": "2020-10-01"},
]

DRUG_NAMES = [d["drug_name"] for d in DRUG_CATALOG]
DRUG_IDS = {d["drug_name"]: f"DRG-{str(i+1).zfill(3)}" for i, d in enumerate(DRUG_CATALOG)}

print(f"Drug catalog: {len(DRUG_CATALOG)} drugs across {len(set(d['drug_class'] for d in DRUG_CATALOG))} classes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Adverse Event Types and Conditions

# COMMAND ----------

# Adverse event types mapped to drug classes (realistic associations)
AE_TYPES_BY_CLASS = {
    "Statin": ["myalgia", "rhabdomyolysis", "hepatotoxicity", "elevated_LFTs", "fatigue", "headache", "nausea", "diarrhea"],
    "SSRI": ["nausea", "insomnia", "sexual_dysfunction", "weight_gain", "serotonin_syndrome", "dizziness", "dry_mouth", "tremor", "QT_prolongation"],
    "NSAID": ["GI_bleeding", "peptic_ulcer", "renal_impairment", "hypertension", "edema", "headache", "nausea", "rash"],
    "ACE Inhibitor": ["dry_cough", "hyperkalemia", "angioedema", "hypotension", "dizziness", "renal_impairment", "fatigue", "rash"],
    "Anticoagulant": ["bleeding", "hemorrhage", "bruising", "anemia", "hematuria", "epistaxis", "GI_bleeding", "intracranial_hemorrhage"],
    "Beta-Blocker": ["bradycardia", "hypotension", "fatigue", "dizziness", "bronchospasm", "cold_extremities", "depression", "QT_prolongation"],
    "PPI": ["headache", "nausea", "diarrhea", "abdominal_pain", "C_diff_infection", "bone_fracture", "vitamin_B12_deficiency", "hypomagnesemia"],
    "Antidiabetic": ["hypoglycemia", "nausea", "diarrhea", "lactic_acidosis", "weight_gain", "pancreatitis", "UTI", "ketoacidosis"],
    "Antibiotic": ["diarrhea", "nausea", "allergic_reaction", "C_diff_infection", "rash", "photosensitivity", "tendon_rupture", "hepatotoxicity"],
    "Antihistamine": ["drowsiness", "dry_mouth", "headache", "fatigue", "dizziness", "nausea", "blurred_vision", "urinary_retention"],
}

SEVERITY_LEVELS = ["mild", "moderate", "severe", "life-threatening"]
SEVERITY_WEIGHTS = [0.40, 0.35, 0.20, 0.05]

OUTCOMES = ["recovered", "recovering", "not_recovered", "fatal", "unknown"]
OUTCOME_WEIGHTS = [0.45, 0.25, 0.15, 0.05, 0.10]

REPORTER_TYPES = ["physician", "pharmacist", "nurse", "patient", "other_health_professional"]

CONDITION_NAMES = [
    "hypertension", "type_2_diabetes", "hyperlipidemia", "coronary_artery_disease",
    "atrial_fibrillation", "heart_failure", "chronic_kidney_disease", "COPD",
    "asthma", "depression", "anxiety_disorder", "osteoarthritis",
    "rheumatoid_arthritis", "GERD", "obesity", "hepatic_impairment",
    "renal_impairment", "hypothyroidism", "migraine", "DVT",
    "pulmonary_embolism", "stroke_history", "peripheral_neuropathy", "anemia",
    "osteoporosis", "sleep_apnea", "gout", "psoriasis",
    "epilepsy", "Parkinsons_disease",
]

COUNTRIES = ["US", "UK", "Germany", "France", "Japan", "India", "Brazil", "Canada", "Australia", "South_Korea"]
COUNTRY_WEIGHTS = [0.30, 0.12, 0.10, 0.08, 0.10, 0.08, 0.06, 0.06, 0.05, 0.05]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Patients (~5,000)

# COMMAND ----------

NUM_PATIENTS = 5000
DIRTY_RATE = 0.05  # 5% dirty data

patients = []
for i in range(NUM_PATIENTS):
    pid = f"PAT-{str(i+1).zfill(5)}"
    age = int(np.random.normal(55, 18))
    age = max(18, min(95, age))  # clamp to realistic range
    gender = random.choice(["M", "F"])
    weight = round(np.random.normal(75 if gender == "M" else 65, 15), 1)
    weight = max(40.0, min(150.0, weight))
    country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
    enrollment_date = fake.date_between(start_date="-3y", end_date="today")

    record = {
        "patient_id": pid,
        "age": age,
        "gender": gender,
        "weight_kg": weight,
        "country": country,
        "enrollment_date": str(enrollment_date),
    }

    # Inject dirty data (~5%)
    if random.random() < DIRTY_RATE:
        dirty_type = random.choice(["null_age", "null_gender", "invalid_weight", "future_date"])
        if dirty_type == "null_age":
            record["age"] = None
        elif dirty_type == "null_gender":
            record["gender"] = None
        elif dirty_type == "invalid_weight":
            record["weight_kg"] = -1.0
        elif dirty_type == "future_date":
            record["enrollment_date"] = str(fake.date_between(start_date="+1y", end_date="+3y"))

    patients.append(record)

patients_schema = T.StructType([
    T.StructField("patient_id", T.StringType(), False),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("weight_kg", T.DoubleType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("enrollment_date", T.StringType(), True),
])

df_patients = spark.createDataFrame(patients, schema=patients_schema)
print(f"Patients generated: {df_patients.count()}")
df_patients.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Drugs (~50)

# COMMAND ----------

drugs = []
for i, d in enumerate(DRUG_CATALOG):
    drugs.append({
        "drug_id": DRUG_IDS[d["drug_name"]],
        "drug_name": d["drug_name"],
        "generic_name": d["generic_name"],
        "manufacturer": d["manufacturer"],
        "drug_class": d["drug_class"],
        "route": d["route"],
        "approved_date": d["approved_date"],
    })

df_drugs = spark.createDataFrame(drugs)
print(f"Drugs generated: {df_drugs.count()}")
df_drugs.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Adverse Events (~25,000)

# COMMAND ----------

NUM_ADVERSE_EVENTS = 25000
drug_class_lookup = {d["drug_name"]: d["drug_class"] for d in DRUG_CATALOG}

adverse_events = []
used_event_ids = set()

for i in range(NUM_ADVERSE_EVENTS):
    eid = f"AE-{str(i+1).zfill(6)}"
    patient_id = f"PAT-{str(random.randint(1, NUM_PATIENTS)).zfill(5)}"
    drug_name = random.choice(DRUG_NAMES)
    drug_id = DRUG_IDS[drug_name]
    drug_class = drug_class_lookup[drug_name]

    # Pick AE type weighted toward class-specific events (80%) vs random (20%)
    if random.random() < 0.80:
        event_type = random.choice(AE_TYPES_BY_CLASS[drug_class])
    else:
        all_ae_types = [ae for aes in AE_TYPES_BY_CLASS.values() for ae in aes]
        event_type = random.choice(list(set(all_ae_types)))

    severity = random.choices(SEVERITY_LEVELS, weights=SEVERITY_WEIGHTS, k=1)[0]
    outcome = random.choices(OUTCOMES, weights=OUTCOME_WEIGHTS, k=1)[0]
    reporter = random.choice(REPORTER_TYPES)
    event_date = fake.date_between(start_date="-2y", end_date="today")
    days_to_onset = max(1, int(np.random.exponential(14)))  # avg 14 days

    record = {
        "event_id": eid,
        "patient_id": patient_id,
        "drug_id": drug_id,
        "drug_name": drug_name,
        "event_date": str(event_date),
        "event_type": event_type,
        "severity": severity,
        "outcome": outcome,
        "reporter_type": reporter,
        "days_to_onset": days_to_onset,
    }

    # Inject dirty data (~5%)
    if random.random() < DIRTY_RATE:
        dirty_type = random.choice([
            "null_severity", "null_event_type", "future_date",
            "duplicate_id", "invalid_days",
        ])
        if dirty_type == "null_severity":
            record["severity"] = None
        elif dirty_type == "null_event_type":
            record["event_type"] = None
        elif dirty_type == "future_date":
            record["event_date"] = str(fake.date_between(start_date="+1y", end_date="+3y"))
        elif dirty_type == "duplicate_id" and len(adverse_events) > 10:
            record["event_id"] = adverse_events[random.randint(0, len(adverse_events) - 1)]["event_id"]
        elif dirty_type == "invalid_days":
            record["days_to_onset"] = -999

    adverse_events.append(record)

ae_schema = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("patient_id", T.StringType(), True),
    T.StructField("drug_id", T.StringType(), True),
    T.StructField("drug_name", T.StringType(), True),
    T.StructField("event_date", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("severity", T.StringType(), True),
    T.StructField("outcome", T.StringType(), True),
    T.StructField("reporter_type", T.StringType(), True),
    T.StructField("days_to_onset", T.IntegerType(), True),
])

df_adverse_events = spark.createDataFrame(adverse_events, schema=ae_schema)
print(f"Adverse events generated: {df_adverse_events.count()}")
df_adverse_events.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Conditions (~8,000)

# COMMAND ----------

NUM_CONDITIONS = 8000

conditions = []
for i in range(NUM_CONDITIONS):
    cid = f"CND-{str(i+1).zfill(5)}"
    patient_id = f"PAT-{str(random.randint(1, NUM_PATIENTS)).zfill(5)}"
    condition_name = random.choice(CONDITION_NAMES)
    diagnosis_date = fake.date_between(start_date="-10y", end_date="today")

    record = {
        "condition_id": cid,
        "patient_id": patient_id,
        "condition_name": condition_name,
        "diagnosis_date": str(diagnosis_date),
    }

    # Inject dirty data (~5%)
    if random.random() < DIRTY_RATE:
        dirty_type = random.choice(["null_condition", "future_diagnosis", "duplicate_id"])
        if dirty_type == "null_condition":
            record["condition_name"] = None
        elif dirty_type == "future_diagnosis":
            record["diagnosis_date"] = str(fake.date_between(start_date="+1y", end_date="+5y"))
        elif dirty_type == "duplicate_id" and len(conditions) > 10:
            record["condition_id"] = conditions[random.randint(0, len(conditions) - 1)]["condition_id"]

    conditions.append(record)

df_conditions = spark.createDataFrame(conditions)
print(f"Conditions generated: {df_conditions.count()}")
df_conditions.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Prescriptions (~15,000)

# COMMAND ----------

NUM_PRESCRIPTIONS = 15000

# Typical dosage ranges per drug class (in mg)
DOSAGE_RANGES = {
    "Statin": (10, 80),
    "SSRI": (25, 200),
    "NSAID": (200, 800),
    "ACE Inhibitor": (5, 40),
    "Anticoagulant": (2.5, 20),
    "Beta-Blocker": (25, 200),
    "PPI": (20, 40),
    "Antidiabetic": (500, 2000),
    "Antibiotic": (250, 1000),
    "Antihistamine": (5, 20),
}

FREQUENCIES = ["once_daily", "twice_daily", "three_times_daily", "as_needed"]
FREQ_WEIGHTS = [0.45, 0.30, 0.15, 0.10]

prescriptions = []
for i in range(NUM_PRESCRIPTIONS):
    rx_id = f"RX-{str(i+1).zfill(6)}"
    patient_id = f"PAT-{str(random.randint(1, NUM_PATIENTS)).zfill(5)}"
    drug_name = random.choice(DRUG_NAMES)
    drug_id = DRUG_IDS[drug_name]
    drug_class = drug_class_lookup[drug_name]

    dose_range = DOSAGE_RANGES[drug_class]
    dosage_mg = round(random.uniform(dose_range[0], dose_range[1]), 0)
    frequency = random.choices(FREQUENCIES, weights=FREQ_WEIGHTS, k=1)[0]

    start_date = fake.date_between(start_date="-2y", end_date="-30d")
    duration_days = int(np.random.exponential(180))  # avg 6 months
    duration_days = max(7, min(730, duration_days))
    end_date = start_date + timedelta(days=duration_days)

    record = {
        "rx_id": rx_id,
        "patient_id": patient_id,
        "drug_id": drug_id,
        "drug_name": drug_name,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "dosage_mg": dosage_mg,
        "frequency": frequency,
    }

    # Inject dirty data (~5%)
    if random.random() < DIRTY_RATE:
        dirty_type = random.choice(["null_dosage", "negative_dosage", "end_before_start", "duplicate_id"])
        if dirty_type == "null_dosage":
            record["dosage_mg"] = None
        elif dirty_type == "negative_dosage":
            record["dosage_mg"] = -100.0
        elif dirty_type == "end_before_start":
            record["end_date"] = str(start_date - timedelta(days=random.randint(1, 30)))
        elif dirty_type == "duplicate_id" and len(prescriptions) > 10:
            record["rx_id"] = prescriptions[random.randint(0, len(prescriptions) - 1)]["rx_id"]

    prescriptions.append(record)

rx_schema = T.StructType([
    T.StructField("rx_id", T.StringType(), False),
    T.StructField("patient_id", T.StringType(), True),
    T.StructField("drug_id", T.StringType(), True),
    T.StructField("drug_name", T.StringType(), True),
    T.StructField("start_date", T.StringType(), True),
    T.StructField("end_date", T.StringType(), True),
    T.StructField("dosage_mg", T.DoubleType(), True),
    T.StructField("frequency", T.StringType(), True),
])

df_prescriptions = spark.createDataFrame(prescriptions, schema=rx_schema)
print(f"Prescriptions generated: {df_prescriptions.count()}")
df_prescriptions.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Parquet to Volume & Register as Delta Tables

# COMMAND ----------

tables = {
    "patients": df_patients,
    "drugs": df_drugs,
    "adverse_events": df_adverse_events,
    "conditions": df_conditions,
    "prescriptions": df_prescriptions,
}

for table_name, df in tables.items():
    # Write Parquet to Volume (raw data for hackathon exploration)
    parquet_path = f"{VOLUME_PATH}/{table_name}"
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"  Parquet written: {parquet_path}")

    # Register as Delta table in catalog (bronze layer)
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.mode("overwrite").saveAsTable(full_table_name)
    print(f"  Delta table registered: {full_table_name}")

print("\nAll tables written and registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

print("=== Data Verification ===\n")
for table_name in tables.keys():
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    count = spark.table(full_name).count()
    print(f"{full_name}: {count:,} rows")

# Quick dirty data check on adverse_events
ae_df = spark.table(f"{CATALOG}.{SCHEMA}.adverse_events")
null_severity = ae_df.filter(F.col("severity").isNull()).count()
future_dates = ae_df.filter(F.col("event_date") > F.current_date()).count()
invalid_days = ae_df.filter(F.col("days_to_onset") < 0).count()
print(f"\nDirty data in adverse_events:")
print(f"  Null severity: {null_severity}")
print(f"  Future dates: {future_dates}")
print(f"  Invalid days_to_onset: {invalid_days}")
print(f"  Total dirty ~{null_severity + future_dates + invalid_days} ({(null_severity + future_dates + invalid_days) / 25000 * 100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Structured data generation complete. Tables are available as:
# MAGIC - **Parquet** in Volume: `/Volumes/{catalog}/{schema}/structured_data/{table_name}/`
# MAGIC - **Delta** in catalog: `{catalog}.{schema}.{table_name}`
# MAGIC
# MAGIC Dirty data (~5%) has been injected for pipeline cleaning challenges:
# MAGIC - Null values in critical fields
# MAGIC - Future dates
# MAGIC - Duplicate IDs
# MAGIC - Invalid numeric values