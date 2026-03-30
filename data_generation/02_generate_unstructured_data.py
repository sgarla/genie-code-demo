# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Unstructured Data — Pharmacovigilance Hackathon
# MAGIC
# MAGIC Generates ~170 PDFs → Unity Catalog Volume:
# MAGIC
# MAGIC | Document Type | Count | Content |
# MAGIC |---------------|-------|---------|
# MAGIC | Drug Safety Labels | ~50 (1 per drug) | Indications, contraindications, warnings, adverse reactions, drug interactions, special populations |
# MAGIC | Clinical Case Reports | ~100 | Patient narrative, drug regimen, AE description, causality assessment, outcome |
# MAGIC | Medical Literature | ~20 | Synthetic abstracts: background, methods, results, conclusions |
# MAGIC
# MAGIC Uses the same drug names, conditions, and AE types as structured data for cross-referencing.

# COMMAND ----------

# MAGIC %pip install fpdf2 faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import random
import os
from datetime import datetime, timedelta
from faker import Faker
from fpdf import FPDF

fake = Faker()
Faker.seed(42)
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_q4xxoi_usama_catalog", "Catalog")
dbutils.widgets.text("schema", "pharma_vigilance", "Schema")
dbutils.widgets.text("volume_unstructured", "unstructured_data", "Unstructured Data Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume_unstructured")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"Volume:  {VOLUME_PATH}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

for subdir in ["drug_labels", "case_reports", "literature"]:
    os.makedirs(f"{VOLUME_PATH}/{subdir}", exist_ok=True)
    print(f"Created: {VOLUME_PATH}/{subdir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drug & Medical Reference Data
# MAGIC
# MAGIC Must match the structured data notebook exactly.

# COMMAND ----------

DRUG_CATALOG = [
    {"drug_name": "Cardivex", "generic_name": "atorvastatin-X", "manufacturer": "NovaPharm", "drug_class": "Statin", "route": "oral"},
    {"drug_name": "Lipovant", "generic_name": "rosuvastatin-V", "manufacturer": "GenMed Labs", "drug_class": "Statin", "route": "oral"},
    {"drug_name": "Cholestix", "generic_name": "simvastatin-C", "manufacturer": "PharmaCure", "drug_class": "Statin", "route": "oral"},
    {"drug_name": "Statirol", "generic_name": "pravastatin-S", "manufacturer": "BioVida", "drug_class": "Statin", "route": "oral"},
    {"drug_name": "Lipidex", "generic_name": "fluvastatin-L", "manufacturer": "MediCore", "drug_class": "Statin", "route": "oral"},
    {"drug_name": "Neurazol", "generic_name": "sertraline-N", "manufacturer": "MindWell", "drug_class": "SSRI", "route": "oral"},
    {"drug_name": "Serenix", "generic_name": "fluoxetine-S", "manufacturer": "NovaPharm", "drug_class": "SSRI", "route": "oral"},
    {"drug_name": "Calmitex", "generic_name": "citalopram-C", "manufacturer": "NeuroGen", "drug_class": "SSRI", "route": "oral"},
    {"drug_name": "Moodafix", "generic_name": "escitalopram-M", "manufacturer": "GenMed Labs", "drug_class": "SSRI", "route": "oral"},
    {"drug_name": "Anxiorel", "generic_name": "paroxetine-A", "manufacturer": "PharmaCure", "drug_class": "SSRI", "route": "oral"},
    {"drug_name": "Inflamax", "generic_name": "ibuprofen-I", "manufacturer": "PainFree Inc", "drug_class": "NSAID", "route": "oral"},
    {"drug_name": "Dolorix", "generic_name": "naproxen-D", "manufacturer": "MediCore", "drug_class": "NSAID", "route": "oral"},
    {"drug_name": "Arthroven", "generic_name": "diclofenac-A", "manufacturer": "BioVida", "drug_class": "NSAID", "route": "oral"},
    {"drug_name": "Painzero", "generic_name": "celecoxib-P", "manufacturer": "NovaPharm", "drug_class": "NSAID", "route": "oral"},
    {"drug_name": "Flexidol", "generic_name": "meloxicam-F", "manufacturer": "GenMed Labs", "drug_class": "NSAID", "route": "oral"},
    {"drug_name": "PressDown", "generic_name": "lisinopril-P", "manufacturer": "CardioMed", "drug_class": "ACE Inhibitor", "route": "oral"},
    {"drug_name": "Vasotrim", "generic_name": "enalapril-V", "manufacturer": "NovaPharm", "drug_class": "ACE Inhibitor", "route": "oral"},
    {"drug_name": "Renapril", "generic_name": "ramipril-R", "manufacturer": "PharmaCure", "drug_class": "ACE Inhibitor", "route": "oral"},
    {"drug_name": "Angioblock", "generic_name": "captopril-A", "manufacturer": "BioVida", "drug_class": "ACE Inhibitor", "route": "oral"},
    {"drug_name": "Hypertrol", "generic_name": "benazepril-H", "manufacturer": "MediCore", "drug_class": "ACE Inhibitor", "route": "oral"},
    {"drug_name": "Clotrix", "generic_name": "rivaroxaban-C", "manufacturer": "HemaGen", "drug_class": "Anticoagulant", "route": "oral"},
    {"drug_name": "Thrombonil", "generic_name": "apixaban-T", "manufacturer": "NovaPharm", "drug_class": "Anticoagulant", "route": "oral"},
    {"drug_name": "Coagustop", "generic_name": "dabigatran-C", "manufacturer": "CardioMed", "drug_class": "Anticoagulant", "route": "oral"},
    {"drug_name": "Venoshield", "generic_name": "edoxaban-V", "manufacturer": "GenMed Labs", "drug_class": "Anticoagulant", "route": "oral"},
    {"drug_name": "Heparex", "generic_name": "enoxaparin-H", "manufacturer": "BioVida", "drug_class": "Anticoagulant", "route": "subcutaneous"},
    {"drug_name": "Rhythmox", "generic_name": "metoprolol-R", "manufacturer": "CardioMed", "drug_class": "Beta-Blocker", "route": "oral"},
    {"drug_name": "Betacalm", "generic_name": "atenolol-B", "manufacturer": "NovaPharm", "drug_class": "Beta-Blocker", "route": "oral"},
    {"drug_name": "Heartease", "generic_name": "propranolol-H", "manufacturer": "PharmaCure", "drug_class": "Beta-Blocker", "route": "oral"},
    {"drug_name": "Pulsefix", "generic_name": "bisoprolol-P", "manufacturer": "MediCore", "drug_class": "Beta-Blocker", "route": "oral"},
    {"drug_name": "Cardiozen", "generic_name": "carvedilol-C", "manufacturer": "BioVida", "drug_class": "Beta-Blocker", "route": "oral"},
    {"drug_name": "Gastronil", "generic_name": "omeprazole-G", "manufacturer": "DigestiCare", "drug_class": "PPI", "route": "oral"},
    {"drug_name": "Acidblock", "generic_name": "lansoprazole-A", "manufacturer": "NovaPharm", "drug_class": "PPI", "route": "oral"},
    {"drug_name": "Refluxend", "generic_name": "pantoprazole-R", "manufacturer": "GenMed Labs", "drug_class": "PPI", "route": "oral"},
    {"drug_name": "Stomacure", "generic_name": "esomeprazole-S", "manufacturer": "PharmaCure", "drug_class": "PPI", "route": "oral"},
    {"drug_name": "Ulceraid", "generic_name": "rabeprazole-U", "manufacturer": "DigestiCare", "drug_class": "PPI", "route": "oral"},
    {"drug_name": "Glucomin", "generic_name": "metformin-G", "manufacturer": "EndoMed", "drug_class": "Antidiabetic", "route": "oral"},
    {"drug_name": "Insulex", "generic_name": "glipizide-I", "manufacturer": "NovaPharm", "drug_class": "Antidiabetic", "route": "oral"},
    {"drug_name": "Diabetrol", "generic_name": "sitagliptin-D", "manufacturer": "GenMed Labs", "drug_class": "Antidiabetic", "route": "oral"},
    {"drug_name": "Sugardown", "generic_name": "empagliflozin-S", "manufacturer": "EndoMed", "drug_class": "Antidiabetic", "route": "oral"},
    {"drug_name": "Pancrefix", "generic_name": "liraglutide-P", "manufacturer": "BioVida", "drug_class": "Antidiabetic", "route": "subcutaneous"},
    {"drug_name": "Bactercide", "generic_name": "amoxicillin-B", "manufacturer": "InfectiGuard", "drug_class": "Antibiotic", "route": "oral"},
    {"drug_name": "Septikill", "generic_name": "azithromycin-S", "manufacturer": "NovaPharm", "drug_class": "Antibiotic", "route": "oral"},
    {"drug_name": "Germaway", "generic_name": "ciprofloxacin-G", "manufacturer": "PharmaCure", "drug_class": "Antibiotic", "route": "oral"},
    {"drug_name": "Microbex", "generic_name": "doxycycline-M", "manufacturer": "InfectiGuard", "drug_class": "Antibiotic", "route": "oral"},
    {"drug_name": "Infectrol", "generic_name": "levofloxacin-I", "manufacturer": "GenMed Labs", "drug_class": "Antibiotic", "route": "intravenous"},
    {"drug_name": "Allergex", "generic_name": "cetirizine-A", "manufacturer": "AllerFree", "drug_class": "Antihistamine", "route": "oral"},
    {"drug_name": "Histabloc", "generic_name": "loratadine-H", "manufacturer": "NovaPharm", "drug_class": "Antihistamine", "route": "oral"},
    {"drug_name": "Sneezaway", "generic_name": "fexofenadine-S", "manufacturer": "AllerFree", "drug_class": "Antihistamine", "route": "oral"},
    {"drug_name": "Rhinoclear", "generic_name": "desloratadine-R", "manufacturer": "PharmaCure", "drug_class": "Antihistamine", "route": "oral"},
    {"drug_name": "Itchnil", "generic_name": "diphenhydramine-I", "manufacturer": "MediCore", "drug_class": "Antihistamine", "route": "oral"},
]

DRUG_NAMES = [d["drug_name"] for d in DRUG_CATALOG]
DRUG_LOOKUP = {d["drug_name"]: d for d in DRUG_CATALOG}

# Adverse event types by drug class (same as structured data notebook)
AE_TYPES_BY_CLASS = {
    "Statin": ["myalgia", "rhabdomyolysis", "hepatotoxicity", "elevated LFTs", "fatigue", "headache", "nausea", "diarrhea"],
    "SSRI": ["nausea", "insomnia", "sexual dysfunction", "weight gain", "serotonin syndrome", "dizziness", "dry mouth", "tremor", "QT prolongation"],
    "NSAID": ["GI bleeding", "peptic ulcer", "renal impairment", "hypertension", "edema", "headache", "nausea", "rash"],
    "ACE Inhibitor": ["dry cough", "hyperkalemia", "angioedema", "hypotension", "dizziness", "renal impairment", "fatigue", "rash"],
    "Anticoagulant": ["bleeding", "hemorrhage", "bruising", "anemia", "hematuria", "epistaxis", "GI bleeding", "intracranial hemorrhage"],
    "Beta-Blocker": ["bradycardia", "hypotension", "fatigue", "dizziness", "bronchospasm", "cold extremities", "depression", "QT prolongation"],
    "PPI": ["headache", "nausea", "diarrhea", "abdominal pain", "C. diff infection", "bone fracture", "vitamin B12 deficiency", "hypomagnesemia"],
    "Antidiabetic": ["hypoglycemia", "nausea", "diarrhea", "lactic acidosis", "weight gain", "pancreatitis", "UTI", "ketoacidosis"],
    "Antibiotic": ["diarrhea", "nausea", "allergic reaction", "C. diff infection", "rash", "photosensitivity", "tendon rupture", "hepatotoxicity"],
    "Antihistamine": ["drowsiness", "dry mouth", "headache", "fatigue", "dizziness", "nausea", "blurred vision", "urinary retention"],
}

CONDITION_NAMES = [
    "hypertension", "type 2 diabetes", "hyperlipidemia", "coronary artery disease",
    "atrial fibrillation", "heart failure", "chronic kidney disease", "COPD",
    "asthma", "depression", "anxiety disorder", "osteoarthritis",
    "rheumatoid arthritis", "GERD", "obesity", "hepatic impairment",
    "renal impairment", "hypothyroidism", "migraine", "DVT",
]

# Indications by drug class
INDICATIONS_BY_CLASS = {
    "Statin": "Treatment of hyperlipidemia and reduction of cardiovascular risk in patients with elevated LDL cholesterol. Indicated as adjunctive therapy to diet for reduction of elevated total cholesterol, LDL-C, apolipoprotein B, and triglycerides.",
    "SSRI": "Treatment of major depressive disorder (MDD), generalized anxiety disorder (GAD), panic disorder, social anxiety disorder, and obsessive-compulsive disorder (OCD) in adults.",
    "NSAID": "Relief of signs and symptoms of osteoarthritis, rheumatoid arthritis, and acute pain. Management of mild to moderate pain and inflammation in adults.",
    "ACE Inhibitor": "Treatment of hypertension, heart failure, and diabetic nephropathy. Reduction of risk of myocardial infarction, stroke, and death from cardiovascular causes in high-risk patients.",
    "Anticoagulant": "Prevention and treatment of deep vein thrombosis (DVT) and pulmonary embolism (PE). Reduction of risk of stroke and systemic embolism in patients with nonvalvular atrial fibrillation.",
    "Beta-Blocker": "Management of hypertension, angina pectoris, heart failure, and cardiac arrhythmias. Prevention of cardiovascular mortality and reinfarction after myocardial infarction.",
    "PPI": "Treatment of gastroesophageal reflux disease (GERD), erosive esophagitis, and Zollinger-Ellison syndrome. Eradication of H. pylori in combination with antibiotics.",
    "Antidiabetic": "Treatment of type 2 diabetes mellitus as an adjunct to diet and exercise to improve glycemic control. May be used as monotherapy or in combination with other antidiabetic agents.",
    "Antibiotic": "Treatment of susceptible bacterial infections including respiratory tract infections, urinary tract infections, skin and soft tissue infections, and intra-abdominal infections.",
    "Antihistamine": "Relief of symptoms associated with allergic rhinitis (seasonal and perennial), chronic idiopathic urticaria, and other allergic conditions in adults and children.",
}

# Contraindications by drug class
CONTRAINDICATIONS_BY_CLASS = {
    "Statin": [
        "Active liver disease or unexplained persistent elevations of serum transaminases",
        "Known hypersensitivity to any component of this medication",
        "Pregnancy and lactation (Category X)",
        "Concomitant use with strong CYP3A4 inhibitors (e.g., itraconazole, ketoconazole, erythromycin)",
    ],
    "SSRI": [
        "Concomitant use of monoamine oxidase inhibitors (MAOIs) or within 14 days of discontinuing an MAOI",
        "Concomitant use of pimozide or thioridazine",
        "Known hypersensitivity to the active substance",
        "Severe hepatic impairment (Child-Pugh Class C)",
    ],
    "NSAID": [
        "Known hypersensitivity to aspirin or other NSAIDs (including asthma, urticaria, or allergic-type reactions)",
        "Active GI bleeding or history of peptic ulcer disease",
        "Severe renal impairment (CrCl < 30 mL/min)",
        "Third trimester of pregnancy",
        "Coronary artery bypass graft (CABG) surgery setting",
    ],
    "ACE Inhibitor": [
        "History of angioedema related to previous ACE inhibitor therapy",
        "Hereditary or idiopathic angioedema",
        "Bilateral renal artery stenosis",
        "Pregnancy (Category D in second and third trimesters)",
        "Concomitant use with aliskiren in patients with diabetes",
    ],
    "Anticoagulant": [
        "Active pathological bleeding",
        "Severe hepatic impairment associated with coagulopathy",
        "Known hypersensitivity to the active substance",
        "Prosthetic heart valve requiring anticoagulation",
        "Concomitant treatment with other anticoagulants except under specific switching conditions",
    ],
    "Beta-Blocker": [
        "Sinus bradycardia (heart rate < 50 bpm)",
        "Second or third degree heart block without a pacemaker",
        "Cardiogenic shock or decompensated heart failure",
        "Severe bronchospastic disease (asthma, severe COPD)",
        "Pheochromocytoma without prior alpha-blockade",
    ],
    "PPI": [
        "Known hypersensitivity to substituted benzimidazoles",
        "Concomitant use with rilpivirine-containing products",
        "Patients with known hypersensitivity to any component of the formulation",
    ],
    "Antidiabetic": [
        "Severe renal impairment (eGFR < 30 mL/min/1.73 m2)",
        "Metabolic acidosis including diabetic ketoacidosis",
        "Known hypersensitivity to the active substance",
        "Conditions associated with hypoxia (e.g., acute heart failure, respiratory failure)",
    ],
    "Antibiotic": [
        "Known hypersensitivity to the drug class or any component",
        "History of tendon disorders related to fluoroquinolone administration (fluoroquinolones only)",
        "Myasthenia gravis (fluoroquinolones only)",
        "Severe hepatic impairment with jaundice",
    ],
    "Antihistamine": [
        "Known hypersensitivity to the active substance or excipients",
        "Severe renal impairment requiring dose adjustment",
        "Patients currently receiving MAOIs (first-generation antihistamines)",
        "Neonates and premature infants",
    ],
}

# Drug interactions by drug class
INTERACTIONS_BY_CLASS = {
    "Statin": [
        ("CYP3A4 inhibitors (itraconazole, ketoconazole, erythromycin, clarithromycin)", "Increased statin exposure leading to elevated risk of myopathy and rhabdomyolysis. Avoid concomitant use or limit statin dose."),
        ("Gemfibrozil and other fibrates", "Increased risk of myopathy. If combination is necessary, use lowest effective statin dose and monitor CK levels."),
        ("Warfarin", "Statins may potentiate anticoagulant effect. Monitor INR closely upon initiation or dose adjustment."),
        ("Cyclosporine", "Markedly increased statin levels. Contraindicated or limit to lowest dose depending on specific statin."),
        ("Grapefruit juice (large quantities)", "Increased bioavailability of certain statins via CYP3A4 inhibition."),
    ],
    "SSRI": [
        ("MAOIs", "Risk of serotonin syndrome, a potentially life-threatening condition. Contraindicated within 14 days of MAOI use."),
        ("Triptans (sumatriptan, rizatriptan)", "Increased risk of serotonin syndrome. Use with caution and monitor for symptoms."),
        ("NSAIDs and anticoagulants", "Increased risk of bleeding, particularly GI bleeding. Monitor for signs of hemorrhage."),
        ("CYP2D6 substrates (tamoxifen, codeine)", "SSRIs may inhibit CYP2D6, reducing efficacy of prodrugs requiring CYP2D6 activation."),
        ("Lithium", "Enhanced serotonergic effects and potential lithium toxicity. Monitor lithium levels and clinical status."),
    ],
    "NSAID": [
        ("ACE inhibitors and ARBs", "NSAIDs may reduce antihypertensive effect and increase risk of renal impairment and hyperkalemia."),
        ("Anticoagulants (warfarin, DOACs)", "Increased risk of serious bleeding. Avoid combination when possible."),
        ("Lithium", "NSAIDs may elevate lithium plasma levels. Monitor lithium levels closely."),
        ("Methotrexate", "NSAIDs may increase methotrexate levels, enhancing toxicity. Monitor methotrexate levels."),
        ("Diuretics", "Reduced diuretic and antihypertensive effect. Increased risk of acute renal failure."),
    ],
    "ACE Inhibitor": [
        ("Potassium-sparing diuretics and potassium supplements", "Risk of hyperkalemia. Monitor serum potassium regularly."),
        ("NSAIDs", "May reduce antihypertensive effect and worsen renal function. Monitor blood pressure and renal function."),
        ("Lithium", "ACE inhibitors increase lithium levels. Frequent monitoring of serum lithium is recommended."),
        ("Aliskiren", "Dual RAAS blockade increases risk of hypotension, hyperkalemia, and renal impairment. Contraindicated in diabetes."),
        ("Gold (sodium aurothiomalate)", "Nitritoid reactions (flushing, nausea, hypotension) reported with concomitant use."),
    ],
    "Anticoagulant": [
        ("Antiplatelet agents (aspirin, clopidogrel)", "Increased risk of bleeding. Use lowest effective aspirin dose if combination is necessary."),
        ("NSAIDs", "Significantly increased risk of major bleeding, particularly GI hemorrhage. Avoid combination."),
        ("Strong CYP3A4 and P-gp inhibitors (ketoconazole, ritonavir)", "Increased anticoagulant exposure. Dose reduction or avoidance may be required."),
        ("CYP3A4 and P-gp inducers (rifampin, carbamazepine, phenytoin)", "Decreased anticoagulant exposure leading to reduced efficacy. Avoid concomitant use."),
        ("SSRIs and SNRIs", "Increased bleeding risk due to serotonin-mediated effects on platelet aggregation."),
    ],
    "Beta-Blocker": [
        ("Calcium channel blockers (verapamil, diltiazem)", "Risk of severe bradycardia, heart block, and heart failure. Avoid IV combination."),
        ("Clonidine", "Rebound hypertension risk if clonidine is withdrawn while on beta-blocker. Discontinue beta-blocker first."),
        ("Digitalis glycosides", "Additive effects on AV conduction, potentially causing severe bradycardia or heart block."),
        ("Catecholamine-depleting drugs (reserpine)", "Additive hypotension and bradycardia. Monitor heart rate and blood pressure."),
        ("CYP2D6 inhibitors (fluoxetine, paroxetine, quinidine)", "Increased beta-blocker levels, particularly metoprolol. Monitor for excessive beta-blockade."),
    ],
    "PPI": [
        ("Clopidogrel", "PPIs (especially omeprazole) may reduce antiplatelet effect by inhibiting CYP2C19. Consider pantoprazole as alternative."),
        ("Methotrexate", "PPIs may increase methotrexate levels, especially at high doses. Monitor for toxicity."),
        ("Tacrolimus", "PPIs may increase tacrolimus levels, particularly in CYP2C19 poor metabolizers. Monitor drug levels."),
        ("HIV protease inhibitors (atazanavir, nelfinavir)", "Reduced absorption of protease inhibitors due to increased gastric pH. Avoid combination."),
        ("Warfarin", "Potential increased anticoagulant effect. Monitor INR upon PPI initiation or discontinuation."),
    ],
    "Antidiabetic": [
        ("Insulin and insulin secretagogues", "Increased risk of hypoglycemia. Consider dose reduction of insulin or secretagogue."),
        ("Alcohol", "Excessive alcohol intake increases risk of lactic acidosis (metformin) and hypoglycemia."),
        ("Iodinated contrast media", "Risk of acute renal failure and lactic acidosis with metformin. Withhold metformin before and 48h after procedure."),
        ("Carbonic anhydrase inhibitors (topiramate, acetazolamide)", "Increased risk of lactic acidosis. Monitor closely."),
        ("Corticosteroids", "May worsen glycemic control. More frequent glucose monitoring and dose adjustment may be needed."),
    ],
    "Antibiotic": [
        ("Oral anticoagulants (warfarin)", "Antibiotics may alter gut flora and vitamin K metabolism, potentiating anticoagulant effect. Monitor INR."),
        ("Antacids and metal cations (aluminum, magnesium, iron, zinc)", "Reduced antibiotic absorption for fluoroquinolones and tetracyclines. Separate dosing by 2-4 hours."),
        ("Theophylline", "Certain antibiotics (macrolides, fluoroquinolones) inhibit theophylline metabolism. Monitor levels."),
        ("Oral contraceptives", "Efficacy of oral contraceptives may be reduced. Use additional contraceptive methods during treatment."),
        ("Cyclosporine", "Macrolide antibiotics may increase cyclosporine levels. Monitor drug levels and renal function."),
    ],
    "Antihistamine": [
        ("CNS depressants (alcohol, benzodiazepines, opioids)", "Enhanced sedation with first-generation antihistamines. Caution patients about impaired alertness."),
        ("MAOIs", "Intensified anticholinergic effects. Contraindicated with first-generation antihistamines."),
        ("CYP3A4 inhibitors (ketoconazole, erythromycin)", "Increased antihistamine levels. Monitor for adverse effects."),
        ("Anticholinergic drugs", "Additive anticholinergic effects (dry mouth, urinary retention, constipation). Use with caution."),
        ("Grapefruit juice", "May alter absorption of certain antihistamines. Avoid large quantities."),
    ],
}

# Warnings for special populations
SPECIAL_POPULATIONS = {
    "Statin": {
        "Geriatric": "No dose adjustment required for elderly patients. However, advanced age (>= 65 years) is a predisposing factor for myopathy. Use the lowest effective dose and monitor for muscle symptoms.",
        "Hepatic Impairment": "Contraindicated in active liver disease. Use with caution in patients with history of liver disease or heavy alcohol consumption. Monitor LFTs before and during therapy.",
        "Renal Impairment": "No dose adjustment for mild to moderate renal impairment. For severe renal impairment (CrCl < 30 mL/min), initiate at the lowest dose with careful monitoring.",
        "Pediatric": "Safety and efficacy established in patients aged 10-17 years for heterozygous familial hypercholesterolemia. Not studied in pre-pubertal patients.",
    },
    "SSRI": {
        "Geriatric": "Start at the lowest dose and titrate slowly. Elderly patients are at increased risk of hyponatremia (SIADH) and falls. Monitor sodium levels.",
        "Hepatic Impairment": "Reduce dose by 50% in mild to moderate hepatic impairment. Contraindicated in severe hepatic impairment. Prolonged half-life expected.",
        "Renal Impairment": "No dose adjustment for mild to moderate renal impairment. Use with caution in severe renal impairment (CrCl < 20 mL/min).",
        "Pediatric": "Black box warning: Increased risk of suicidal thinking and behavior in children and adolescents. Close monitoring required during initial treatment.",
    },
    "NSAID": {
        "Geriatric": "Increased risk of serious GI events, renal toxicity, and cardiovascular events. Use the lowest effective dose for the shortest duration. Consider gastroprotection with PPI.",
        "Hepatic Impairment": "Use with caution. NSAIDs may cause elevations of liver enzymes. Discontinue if signs of liver disease develop.",
        "Renal Impairment": "Avoid in severe renal impairment. NSAIDs may cause fluid retention, edema, and deterioration of renal function. Monitor serum creatinine.",
        "Pediatric": "Not recommended in children under 12 years except as specifically indicated. Weight-based dosing required.",
    },
    "ACE Inhibitor": {
        "Geriatric": "Start at the lowest dose due to higher risk of hypotension and renal impairment. Monitor blood pressure, renal function, and potassium closely.",
        "Hepatic Impairment": "Use with caution. Some ACE inhibitors are hepatically metabolized. Monitor liver function in patients with pre-existing hepatic disease.",
        "Renal Impairment": "Reduce dose in renal impairment. Increased risk of hyperkalemia and deterioration of renal function. Monitor serum creatinine and potassium at least every 2 weeks initially.",
        "Pediatric": "Some ACE inhibitors are approved for hypertension in children >= 6 years. Dose based on body weight. Not recommended in neonates due to risk of renal failure.",
    },
    "Anticoagulant": {
        "Geriatric": "No routine dose adjustment, but elderly patients have increased bleeding risk. Assess renal function regularly as it declines with age. Consider reduced dose if low body weight.",
        "Hepatic Impairment": "Contraindicated in severe hepatic impairment with coagulopathy. Use with caution in moderate impairment. No dose adjustment in mild impairment.",
        "Renal Impairment": "Dose adjustment required based on CrCl. Contraindicated in severe renal impairment for some agents. Monitor renal function periodically.",
        "Pediatric": "Limited data in pediatric populations. Use only when benefit outweighs risk. Weight-based dosing under specialist supervision.",
    },
    "Beta-Blocker": {
        "Geriatric": "Increased sensitivity to bradycardia and hypotension. Start at the lowest dose and titrate cautiously. Monitor heart rate and blood pressure.",
        "Hepatic Impairment": "Some beta-blockers (propranolol, metoprolol) undergo extensive hepatic metabolism. Reduce dose in hepatic impairment and monitor closely.",
        "Renal Impairment": "Atenolol and other renally cleared beta-blockers require dose adjustment. Monitor for accumulation in severe renal impairment.",
        "Pediatric": "Some beta-blockers approved for pediatric hypertension. Dose based on body weight. Monitor growth and development.",
    },
    "PPI": {
        "Geriatric": "No dose adjustment required. However, long-term use in elderly is associated with increased risk of bone fractures, C. diff infection, and hypomagnesemia.",
        "Hepatic Impairment": "Dose reduction recommended in severe hepatic impairment. PPIs are hepatically metabolized. Maximum dose should not exceed 20 mg/day in severe impairment.",
        "Renal Impairment": "No dose adjustment required. PPIs are not significantly renally eliminated.",
        "Pediatric": "Approved for GERD in children >= 1 year (varies by agent). Weight-based dosing required.",
    },
    "Antidiabetic": {
        "Geriatric": "Increased risk of hypoglycemia and lactic acidosis. Start at low dose and titrate based on glycemic response. Regular monitoring of renal function essential.",
        "Hepatic Impairment": "Avoid metformin in patients with clinical or laboratory evidence of hepatic disease. Risk of impaired lactate clearance.",
        "Renal Impairment": "Metformin: Contraindicated if eGFR < 30. Dose reduction at eGFR 30-45. Assess renal function before initiation and at least annually.",
        "Pediatric": "Metformin approved for type 2 diabetes in children >= 10 years. Insulin remains first-line for type 1 diabetes.",
    },
    "Antibiotic": {
        "Geriatric": "Increased risk of C. diff infection, tendon disorders (fluoroquinolones), and renal impairment. Adjust dose based on renal function.",
        "Hepatic Impairment": "Some antibiotics require dose adjustment or are contraindicated in hepatic impairment. Monitor LFTs during prolonged therapy.",
        "Renal Impairment": "Most antibiotics require dose adjustment in renal impairment. Use CrCl-based dosing tables. Monitor drug levels where applicable (e.g., vancomycin, aminoglycosides).",
        "Pediatric": "Dose based on weight and age. Fluoroquinolones generally avoided in children due to cartilage toxicity risk. Tetracyclines avoided in children < 8 years.",
    },
    "Antihistamine": {
        "Geriatric": "First-generation antihistamines should be avoided in elderly (Beers criteria) due to increased risk of confusion, sedation, falls, and anticholinergic effects. Prefer second-generation agents.",
        "Hepatic Impairment": "Dose reduction may be required for hepatically metabolized antihistamines. Monitor for increased sedation and anticholinergic effects.",
        "Renal Impairment": "Dose reduction recommended for cetirizine and other renally cleared agents in moderate to severe impairment.",
        "Pediatric": "Second-generation antihistamines preferred in children. Dosing based on age and weight. Avoid first-generation agents in infants.",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDF Helper Class

# COMMAND ----------

class SafetyPDF(FPDF):
    """Custom PDF class for pharmacovigilance documents."""

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(100, 100, 100)
        if hasattr(self, "doc_header_text"):
            self.cell(0, 8, self.doc_header_text, new_x="LMARGIN", new_y="NEXT", align="R")
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(0, 51, 102)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 51, 102)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(51, 51, 51)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        # Encode to latin-1 safe text (fpdf2 with default fonts)
        safe_text = text.encode("latin-1", errors="replace").decode("latin-1")
        self.multi_cell(0, 5, safe_text)
        self.ln(2)

    def bullet_list(self, items):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        for item in items:
            safe_item = item.encode("latin-1", errors="replace").decode("latin-1")
            self.cell(5)
            self.cell(5, 5, "-")
            self.multi_cell(0, 5, f" {safe_item}")
            self.ln(1)
        self.ln(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Drug Safety Labels (50 PDFs)

# COMMAND ----------

def generate_drug_label(drug, output_dir):
    """Generate a comprehensive drug safety label PDF for a single drug."""
    d_class = drug["drug_class"]

    pdf = SafetyPDF()
    pdf.doc_header_text = f"{drug['drug_name']} ({drug['generic_name']}) - Prescribing Information"
    pdf.alias_nb_pages()
    pdf.add_page()

    # Title block
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 12, drug["drug_name"], new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "I", 12)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 8, f"({drug['generic_name']})", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Manufactured by {drug['manufacturer']}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 6, f"Drug Class: {d_class} | Route: {drug['route']}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(8)

    # Indications
    pdf.section_title("1. INDICATIONS AND USAGE")
    pdf.body_text(f"{drug['drug_name']} ({drug['generic_name']}) is indicated for the following:")
    pdf.body_text(INDICATIONS_BY_CLASS[d_class])

    # Contraindications
    pdf.section_title("2. CONTRAINDICATIONS")
    pdf.body_text(f"{drug['drug_name']} is contraindicated in the following conditions:")
    pdf.bullet_list(CONTRAINDICATIONS_BY_CLASS[d_class])

    # Warnings and Precautions
    pdf.section_title("3. WARNINGS AND PRECAUTIONS")
    ae_types = AE_TYPES_BY_CLASS[d_class]
    serious_aes = ae_types[:3]  # first 3 are typically the most serious
    pdf.body_text(
        f"Serious adverse reactions associated with {drug['drug_name']} include "
        f"{', '.join(serious_aes[:-1])}, and {serious_aes[-1]}. "
        f"Patients should be monitored for signs and symptoms of these conditions. "
        f"If any serious adverse reaction occurs, discontinue {drug['drug_name']} and initiate appropriate medical management."
    )
    pdf.body_text(
        f"Post-marketing surveillance has identified rare cases of {serious_aes[0]} in patients "
        f"taking {drug['drug_name']}, particularly in elderly patients (>= 65 years) and those "
        f"with pre-existing {random.choice(['hepatic impairment', 'renal impairment', 'cardiovascular disease'])}. "
        f"Healthcare providers should weigh the benefits against the risks before prescribing."
    )

    # Adverse Reactions
    pdf.section_title("4. ADVERSE REACTIONS")
    pdf.subsection_title("4.1 Common Adverse Reactions (>= 1%)")
    common_aes = ae_types[:5]
    rates = sorted([random.uniform(1.0, 15.0) for _ in common_aes], reverse=True)
    ae_with_rates = [f"{ae} ({rate:.1f}%)" for ae, rate in zip(common_aes, rates)]
    pdf.body_text(
        f"In clinical trials, the most common adverse reactions reported with {drug['drug_name']} were: "
        f"{', '.join(ae_with_rates)}."
    )
    pdf.subsection_title("4.2 Serious Adverse Reactions")
    rare_aes = ae_types[5:]
    pdf.body_text(
        f"Less common but clinically significant adverse reactions include: "
        f"{', '.join(rare_aes)}. These reactions were reported in < 1% of patients in clinical trials "
        f"but have been observed in post-marketing experience."
    )

    # Drug Interactions
    pdf.section_title("5. DRUG INTERACTIONS")
    interactions = INTERACTIONS_BY_CLASS[d_class]
    for interacting_drug, description in interactions:
        pdf.subsection_title(f"5.{interactions.index((interacting_drug, description)) + 1} {interacting_drug}")
        pdf.body_text(description)

    # Special Populations
    pdf.section_title("6. USE IN SPECIFIC POPULATIONS")
    populations = SPECIAL_POPULATIONS[d_class]
    for pop_name, guidance in populations.items():
        pdf.subsection_title(f"6.{list(populations.keys()).index(pop_name) + 1} {pop_name}")
        pdf.body_text(guidance)

    # Dosage and Administration
    pdf.section_title("7. DOSAGE AND ADMINISTRATION")
    dose_text = {
        "Statin": f"Recommended starting dose: 10-20 mg {drug['route']}ly once daily. May be increased to a maximum of 80 mg/day based on therapeutic response and tolerability. Take with or without food. Evening administration preferred for short-acting statins.",
        "SSRI": f"Initial dose: 25-50 mg {drug['route']}ly once daily. May be increased in increments of 25-50 mg at intervals of at least one week. Maximum dose: 200 mg/day. Taper gradually when discontinuing to minimize withdrawal symptoms.",
        "NSAID": f"Recommended dose: 200-400 mg {drug['route']}ly twice daily. Use the lowest effective dose for the shortest duration consistent with treatment goals. Maximum daily dose: 800 mg. Take with food to reduce GI irritation.",
        "ACE Inhibitor": f"Initial dose: 5-10 mg {drug['route']}ly once daily. Titrate based on blood pressure response. Usual maintenance dose: 20-40 mg/day. Maximum: 40 mg/day. Monitor blood pressure and renal function during titration.",
        "Anticoagulant": f"Standard dose: 5-20 mg {drug['route']}ly once or twice daily depending on indication. Dose adjustment required for renal impairment and concomitant medications. Take with food to improve absorption.",
        "Beta-Blocker": f"Initial dose: 25-50 mg {drug['route']}ly once or twice daily. Titrate gradually at 1-2 week intervals. Target dose for heart failure: 200 mg/day. Do not abruptly discontinue; taper over 1-2 weeks.",
        "PPI": f"Standard dose: 20-40 mg {drug['route']}ly once daily before breakfast. For erosive esophagitis: 40 mg daily for 4-8 weeks. Long-term maintenance: use lowest effective dose. Swallow capsules whole.",
        "Antidiabetic": f"Starting dose: 500 mg {drug['route']}ly twice daily with meals. May increase by 500 mg weekly. Maximum dose: 2000 mg/day. Extended-release formulation: take once daily with evening meal.",
        "Antibiotic": f"Dose varies by infection type and severity. Typical dose: 250-500 mg {drug['route']}ly every 8-12 hours. Duration: 5-14 days depending on infection. Complete full course even if symptoms improve.",
        "Antihistamine": f"Adults: 5-10 mg {drug['route']}ly once daily. Second-generation agents preferred for daytime use. First-generation agents may be given at bedtime if sedation is acceptable. Reduce dose in renal impairment.",
    }
    pdf.body_text(dose_text.get(d_class, f"See package insert for detailed dosing information for {drug['drug_name']}."))

    # Save
    filename = f"{drug['drug_name'].lower()}_safety_label.pdf"
    filepath = os.path.join(output_dir, filename)
    pdf.output(filepath)
    return filepath

# Generate all 50 drug labels directly to Volume
labels_dir = f"{VOLUME_PATH}/drug_labels"
os.makedirs(labels_dir, exist_ok=True)
label_count = 0

for drug in DRUG_CATALOG:
    filepath = generate_drug_label(drug, labels_dir)
    label_count += 1

print(f"Generated {label_count} drug safety labels in {labels_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Clinical Case Reports (100 PDFs)

# COMMAND ----------

CAUSALITY_ASSESSMENTS = ["Certain", "Probable/Likely", "Possible", "Unlikely", "Conditional/Unclassified", "Unassessable"]
CAUSALITY_WEIGHTS = [0.05, 0.30, 0.40, 0.15, 0.05, 0.05]

CASE_OUTCOMES = [
    "Patient recovered fully after drug discontinuation.",
    "Patient recovered with residual symptoms requiring ongoing monitoring.",
    "Patient condition improved but did not fully resolve during the reporting period.",
    "Patient condition unchanged. Drug was continued with dose adjustment.",
    "Patient required hospitalization for management of the adverse event.",
    "Patient did not recover. Permanent sequelae reported.",
    "Fatal outcome. Causality assessment pending further investigation.",
]

def generate_case_report(case_id, output_dir):
    """Generate a clinical case report PDF."""
    drug = random.choice(DRUG_CATALOG)
    d_class = drug["drug_class"]
    ae_types = AE_TYPES_BY_CLASS[d_class]
    primary_ae = random.choice(ae_types[:4])  # pick from more common AEs
    secondary_ae = random.choice(ae_types[4:]) if len(ae_types) > 4 else None

    # Patient profile
    age = random.randint(25, 88)
    gender = random.choice(["male", "female"])
    weight = round(random.gauss(75 if gender == "male" else 65, 12), 1)
    num_conditions = random.randint(1, 4)
    patient_conditions = random.sample(CONDITION_NAMES, min(num_conditions, len(CONDITION_NAMES)))
    country = random.choice(["United States", "United Kingdom", "Germany", "Japan", "India", "France", "Canada", "Brazil"])

    # Event details
    event_date = fake.date_between(start_date="-2y", end_date="-30d")
    onset_days = random.randint(1, 90)
    severity = random.choices(["mild", "moderate", "severe", "life-threatening"], weights=[0.2, 0.35, 0.35, 0.1], k=1)[0]
    causality = random.choices(CAUSALITY_ASSESSMENTS, weights=CAUSALITY_WEIGHTS, k=1)[0]
    outcome_text = random.choice(CASE_OUTCOMES)
    reporter = random.choice(["Dr. " + fake.last_name(), "Dr. " + fake.last_name() + ", PharmD", "Dr. " + fake.last_name() + ", MD"])

    pdf = SafetyPDF()
    pdf.doc_header_text = f"Clinical Case Report - {case_id}"
    pdf.alias_nb_pages()
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 12, f"Clinical Case Report", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, f"Case ID: {case_id}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 7, f"Report Date: {event_date.strftime('%B %d, %Y')}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 7, f"Reporter: {reporter}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(8)

    # Patient Information
    pdf.section_title("Patient Information")
    pdf.body_text(
        f"The patient is a {age}-year-old {gender} ({weight} kg) from {country} "
        f"with a medical history significant for {', '.join(patient_conditions[:-1])}"
        f"{' and ' + patient_conditions[-1] if len(patient_conditions) > 1 else patient_conditions[0]}."
    )

    # Drug Regimen
    pdf.section_title("Drug Regimen")
    dose_ranges = {"Statin": (10, 80), "SSRI": (25, 200), "NSAID": (200, 800), "ACE Inhibitor": (5, 40), "Anticoagulant": (2.5, 20), "Beta-Blocker": (25, 200), "PPI": (20, 40), "Antidiabetic": (500, 2000), "Antibiotic": (250, 1000), "Antihistamine": (5, 20)}
    dose_range = dose_ranges.get(d_class, (10, 100))
    dosage = round(random.uniform(dose_range[0], dose_range[1]))
    freq = random.choice(["once daily", "twice daily", "three times daily"])

    start_date = event_date - timedelta(days=onset_days + random.randint(10, 90))
    pdf.body_text(
        f"The patient was started on {drug['drug_name']} ({drug['generic_name']}) "
        f"{dosage} mg {drug['route']} {freq} on {start_date.strftime('%B %d, %Y')} "
        f"for the treatment of {random.choice(patient_conditions)}."
    )

    # Concomitant medications
    concomitant_drugs = random.sample([d["drug_name"] for d in DRUG_CATALOG if d["drug_name"] != drug["drug_name"]], random.randint(1, 3))
    pdf.body_text(
        f"Concomitant medications at the time of the event included: {', '.join(concomitant_drugs)}."
    )

    # Adverse Event Description
    pdf.section_title("Adverse Event Description")
    pdf.body_text(
        f"Approximately {onset_days} days after initiating {drug['drug_name']}, the patient "
        f"developed {severity} {primary_ae}. "
        f"{'The patient also experienced ' + secondary_ae + ' concurrently. ' if secondary_ae else ''}"
        f"The onset was {'gradual' if onset_days > 14 else 'acute'}, with symptoms including "
        f"{_generate_symptom_narrative(primary_ae, d_class)}."
    )

    # Clinical Assessment
    pdf.section_title("Clinical Assessment")
    pdf.body_text(
        f"Physical examination revealed findings consistent with {primary_ae}. "
        f"Laboratory investigations were {'significant for abnormal values related to ' + primary_ae if severity in ['severe', 'life-threatening'] else 'within normal limits except for findings related to ' + primary_ae}. "
        f"The Naranjo adverse drug reaction probability scale score was "
        f"{random.randint(3, 9)}, indicating a {causality.lower()} causal relationship."
    )

    # Causality Assessment
    pdf.section_title("Causality Assessment")
    pdf.body_text(
        f"Using the WHO-UMC causality assessment system, the relationship between "
        f"{drug['drug_name']} and {primary_ae} was assessed as: {causality}."
    )
    pdf.body_text(
        f"Factors considered: temporal relationship ({onset_days} days from drug initiation), "
        f"biological plausibility (known {d_class} class effect), "
        f"{'positive dechallenge (symptoms improved after discontinuation)' if 'recover' in outcome_text.lower() else 'dechallenge not performed'}, "
        f"and absence of alternative explanations."
    )

    # Management and Outcome
    pdf.section_title("Management and Outcome")
    action = random.choice([
        f"{drug['drug_name']} was immediately discontinued.",
        f"The dose of {drug['drug_name']} was reduced.",
        f"{drug['drug_name']} was temporarily held and later restarted at a lower dose.",
        f"No change to {drug['drug_name']} regimen; supportive care provided.",
    ])
    pdf.body_text(f"{action} {outcome_text}")

    # Reporter Comments
    pdf.section_title("Reporter Comments")
    pdf.body_text(
        f"This case highlights the importance of monitoring for {primary_ae} in patients "
        f"receiving {d_class} therapy, particularly in {'elderly patients' if age >= 65 else 'patients'} "
        f"with {patient_conditions[0]}. "
        f"{'Given the severity of this reaction, alternative therapy should be considered.' if severity in ['severe', 'life-threatening'] else 'Continued vigilance is recommended.'}"
    )

    filename = f"case_report_{case_id}.pdf"
    filepath = os.path.join(output_dir, filename)
    pdf.output(filepath)
    return filepath


def _generate_symptom_narrative(ae_type, drug_class):
    """Generate realistic symptom descriptions based on AE type."""
    symptom_map = {
        "myalgia": "diffuse muscle pain and tenderness, primarily affecting the proximal limbs, with elevated creatine kinase (CK) levels",
        "rhabdomyolysis": "severe muscle pain, dark-colored urine (tea-colored), markedly elevated CK (>10x ULN), and acute renal injury",
        "hepatotoxicity": "jaundice, fatigue, right upper quadrant pain, elevated transaminases (ALT/AST >3x ULN), and elevated bilirubin",
        "elevated LFTs": "asymptomatic elevation of liver transaminases detected on routine laboratory monitoring",
        "nausea": "persistent nausea with occasional vomiting, reduced appetite, and mild dehydration",
        "insomnia": "difficulty initiating and maintaining sleep, daytime fatigue, and irritability",
        "sexual dysfunction": "decreased libido, delayed orgasm, and erectile dysfunction reported by the patient",
        "weight gain": "unintentional weight gain of approximately 4-6 kg over 3 months without dietary changes",
        "serotonin syndrome": "agitation, hyperthermia, diaphoresis, tremor, hyperreflexia, and clonus",
        "GI bleeding": "melena, hematemesis, and a drop in hemoglobin requiring blood transfusion",
        "peptic ulcer": "epigastric pain, hematemesis, and endoscopic confirmation of gastric ulceration",
        "renal impairment": "elevated serum creatinine, reduced urine output, and peripheral edema",
        "dry cough": "persistent dry, non-productive cough that worsened at night and disrupted sleep",
        "hyperkalemia": "serum potassium >5.5 mEq/L with ECG changes including peaked T waves",
        "angioedema": "rapid-onset swelling of the face, lips, tongue, and throat requiring emergency airway management",
        "bleeding": "spontaneous bruising, prolonged bleeding from minor cuts, and petechiae",
        "hemorrhage": "significant hemorrhage requiring blood transfusion and surgical intervention",
        "bradycardia": "heart rate below 50 bpm with associated dizziness, fatigue, and pre-syncope",
        "hypotension": "systolic blood pressure below 90 mmHg with orthostatic symptoms and lightheadedness",
        "bronchospasm": "acute onset wheezing, dyspnea, and chest tightness requiring bronchodilator therapy",
        "QT prolongation": "ECG showing corrected QT interval >500 ms with episodes of palpitations and near-syncope",
        "headache": "moderate to severe headache, bilateral in distribution, not relieved by standard analgesics",
        "diarrhea": "watery diarrhea 4-6 times daily with associated abdominal cramping and dehydration",
        "abdominal pain": "diffuse abdominal pain with bloating and altered bowel habits",
        "C. diff infection": "profuse watery diarrhea, abdominal pain, fever, and positive C. difficile toxin assay",
        "hypoglycemia": "blood glucose <70 mg/dL with diaphoresis, tremor, confusion, and palpitations",
        "lactic acidosis": "elevated lactate >5 mmol/L with metabolic acidosis, tachypnea, and altered mental status",
        "allergic reaction": "urticaria, pruritus, facial flushing, and mild angioedema within 30 minutes of administration",
        "rash": "maculopapular rash distributed over the trunk and extremities appearing 5-7 days after drug initiation",
        "drowsiness": "excessive daytime somnolence affecting daily activities and cognitive performance",
        "dry mouth": "persistent xerostomia with difficulty swallowing and increased dental caries risk",
        "fatigue": "persistent fatigue and lethargy not explained by other medical conditions",
        "dizziness": "episodic dizziness and lightheadedness, particularly upon standing",
        "blurred vision": "intermittent blurred vision and difficulty focusing, particularly at near distances",
        "urinary retention": "difficulty initiating urination, weak stream, and sensation of incomplete bladder emptying",
        "depression": "new-onset depressive symptoms including low mood, anhedonia, and sleep disturbance",
        "cold extremities": "cold hands and feet with associated numbness and Raynaud-like symptoms",
        "tremor": "fine postural tremor of the hands affecting handwriting and fine motor tasks",
        "edema": "bilateral lower extremity edema with 2+ pitting, weight gain of 3 kg over 2 weeks",
        "photosensitivity": "exaggerated sunburn reaction with erythema and vesicle formation after minimal sun exposure",
        "tendon rupture": "acute onset severe Achilles tendon pain during physical activity, confirmed rupture on MRI",
        "bone fracture": "atraumatic hip fracture in the setting of long-term therapy, DEXA scan showing osteoporosis",
        "vitamin B12 deficiency": "macrocytic anemia, peripheral neuropathy, and serum B12 level <200 pg/mL",
        "hypomagnesemia": "serum magnesium <1.5 mg/dL with muscle cramps, weakness, and cardiac arrhythmias",
        "pancreatitis": "severe epigastric pain radiating to the back, elevated lipase >3x ULN, and CT findings consistent with acute pancreatitis",
        "ketoacidosis": "hyperglycemia >250 mg/dL, metabolic acidosis, ketonemia, and altered consciousness",
        "UTI": "dysuria, urinary frequency, and suprapubic tenderness with positive urine culture",
        "epistaxis": "recurrent epistaxis requiring nasal packing and ENT consultation",
        "hematuria": "gross hematuria with cystoscopy showing no structural abnormality, attributed to anticoagulation",
        "intracranial hemorrhage": "sudden onset severe headache, neurological deficits, and CT confirming intracranial bleeding",
        "anemia": "progressive anemia with hemoglobin declining from 12.5 to 8.2 g/dL over 6 weeks",
        "bruising": "easy bruising with minimal trauma, particularly on the extremities",
    }
    return symptom_map.get(ae_type, f"symptoms consistent with {ae_type} requiring clinical evaluation and management")


# Generate 100 case reports directly to Volume
cases_dir = f"{VOLUME_PATH}/case_reports"
os.makedirs(cases_dir, exist_ok=True)
case_count = 0

for i in range(1, 101):
    case_id = f"CR-{str(i).zfill(4)}"
    filepath = generate_case_report(case_id, cases_dir)
    case_count += 1

print(f"Generated {case_count} clinical case reports in {cases_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Medical Literature (20 PDFs)

# COMMAND ----------

STUDY_DESIGNS = [
    "retrospective cohort study",
    "prospective observational study",
    "systematic review and meta-analysis",
    "nested case-control study",
    "randomized controlled trial",
    "pharmacoepidemiological study",
    "real-world evidence analysis",
    "post-marketing surveillance study",
]

JOURNALS = [
    "Journal of Pharmacovigilance Research",
    "International Drug Safety Review",
    "Clinical Pharmacology & Therapeutics Quarterly",
    "Adverse Drug Reaction Bulletin",
    "Pharmacoepidemiology and Drug Safety Journal",
    "Drug Safety Insights",
    "Journal of Clinical Risk Management",
    "Global Pharmacovigilance Reports",
]

# Each literature paper focuses on a specific drug-class + AE combination
LITERATURE_TOPICS = [
    ("Statin", "rhabdomyolysis", "elderly patients"),
    ("Statin", "hepatotoxicity", "patients with pre-existing liver disease"),
    ("SSRI", "QT prolongation", "elderly patients"),
    ("SSRI", "serotonin syndrome", "patients on combination therapy"),
    ("NSAID", "GI bleeding", "patients over 65 years"),
    ("NSAID", "renal impairment", "patients with chronic kidney disease"),
    ("ACE Inhibitor", "angioedema", "patients of African descent"),
    ("ACE Inhibitor", "hyperkalemia", "patients with diabetes"),
    ("Anticoagulant", "intracranial hemorrhage", "elderly patients on polypharmacy"),
    ("Anticoagulant", "GI bleeding", "patients co-prescribed NSAIDs"),
    ("Beta-Blocker", "QT prolongation", "patients with heart failure"),
    ("Beta-Blocker", "bradycardia", "elderly patients with renal impairment"),
    ("PPI", "C. diff infection", "hospitalized elderly patients"),
    ("PPI", "bone fracture", "postmenopausal women on long-term therapy"),
    ("Antidiabetic", "lactic acidosis", "patients with renal impairment"),
    ("Antidiabetic", "pancreatitis", "patients on incretin-based therapies"),
    ("Antibiotic", "tendon rupture", "patients over 60 on fluoroquinolones"),
    ("Antibiotic", "C. diff infection", "patients on broad-spectrum antibiotics"),
    ("Antihistamine", "drowsiness", "elderly patients on first-generation agents"),
    ("Statin", "myalgia", "patients on high-dose statin therapy"),
]


def generate_literature(topic_idx, drug_class, ae_type, population, output_dir):
    """Generate a medical literature abstract PDF."""
    # Pick 2-3 drugs from the drug class for the study
    class_drugs = [d for d in DRUG_CATALOG if d["drug_class"] == drug_class]
    study_drugs = random.sample(class_drugs, min(random.randint(2, 3), len(class_drugs)))
    drug_names_str = ", ".join([d["drug_name"] for d in study_drugs])

    study_design = random.choice(STUDY_DESIGNS)
    journal = random.choice(JOURNALS)
    pub_date = fake.date_between(start_date="-3y", end_date="-3m")
    sample_size = random.choice([500, 1000, 2500, 5000, 10000, 25000, 50000])
    duration_months = random.choice([6, 12, 18, 24, 36, 48])

    # Generate realistic statistics
    incidence_rate = round(random.uniform(0.1, 8.0), 2)
    odds_ratio = round(random.uniform(1.1, 4.5), 2)
    ci_lower = round(odds_ratio * random.uniform(0.7, 0.9), 2)
    ci_upper = round(odds_ratio * random.uniform(1.1, 1.5), 2)
    p_value = round(random.uniform(0.001, 0.04), 4) if random.random() > 0.2 else round(random.uniform(0.05, 0.15), 3)
    nnt_harm = random.randint(50, 500)

    authors = [f"{fake.last_name()} {fake.first_name()[0]}" for _ in range(random.randint(3, 6))]
    author_str = ", ".join(authors)

    pdf = SafetyPDF()
    pdf.doc_header_text = f"{journal} - {pub_date.strftime('%B %Y')}"
    pdf.alias_nb_pages()
    pdf.add_page()

    # Title
    title = _generate_paper_title(drug_class, ae_type, population, study_design, study_drugs)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(0, 51, 102)
    safe_title = title.encode("latin-1", errors="replace").decode("latin-1")
    pdf.multi_cell(0, 8, safe_title, align="C")
    pdf.ln(4)

    # Authors and Journal
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(80, 80, 80)
    safe_authors = author_str.encode("latin-1", errors="replace").decode("latin-1")
    pdf.multi_cell(0, 6, safe_authors, align="C")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, f"{journal}, {pub_date.strftime('%B %Y')}, Vol. {random.randint(10,50)}, Issue {random.randint(1,12)}, pp. {random.randint(100,900)}-{random.randint(901,999)}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(8)

    # Abstract
    pdf.section_title("Abstract")

    pdf.subsection_title("Background")
    pdf.body_text(
        f"{ae_type.capitalize()} is a recognized adverse effect of {drug_class.lower()} therapy, "
        f"but its incidence and risk factors in {population} remain poorly characterized. "
        f"Previous studies have reported conflicting results, with incidence rates ranging from "
        f"{round(incidence_rate * 0.3, 2)}% to {round(incidence_rate * 2.5, 2)}%. "
        f"This study aimed to evaluate the risk of {ae_type} associated with {drug_class.lower()} "
        f"use in {population} using real-world data."
    )

    pdf.subsection_title("Methods")
    pdf.body_text(
        f"We conducted a {study_design} using data from a multi-center pharmacovigilance database "
        f"spanning {duration_months} months ({pub_date.year - duration_months // 12}-{pub_date.year}). "
        f"The study included {sample_size:,} {population} who were prescribed {drug_names_str}. "
        f"The primary outcome was the incidence of {ae_type}. Secondary outcomes included time to onset, "
        f"severity distribution, and identification of independent risk factors. "
        f"Multivariable logistic regression was used to calculate adjusted odds ratios (aOR) "
        f"with 95% confidence intervals."
    )

    pdf.subsection_title("Results")
    significant = p_value < 0.05
    pdf.body_text(
        f"Among {sample_size:,} patients, {int(sample_size * incidence_rate / 100)} ({incidence_rate}%) "
        f"developed {ae_type} during the study period. The median time to onset was "
        f"{random.randint(7, 90)} days (IQR: {random.randint(3, 20)}-{random.randint(60, 180)} days). "
        f"The adjusted odds ratio for {ae_type} was {odds_ratio} (95% CI: {ci_lower}-{ci_upper}; "
        f"p{'<0.001' if p_value < 0.001 else '=' + str(p_value)}), "
        f"{'indicating a statistically significant association' if significant else 'which did not reach statistical significance'}. "
        f"The number needed to harm (NNH) was estimated at {nnt_harm}."
    )
    pdf.body_text(
        f"Independent risk factors for {ae_type} included age >= 65 years (aOR {round(random.uniform(1.5, 3.0), 2)}), "
        f"{'hepatic impairment' if random.random() > 0.5 else 'renal impairment'} "
        f"(aOR {round(random.uniform(1.8, 4.0), 2)}), "
        f"and concomitant use of {random.choice(['NSAIDs', 'anticoagulants', 'CYP3A4 inhibitors', 'diuretics'])} "
        f"(aOR {round(random.uniform(1.3, 2.5), 2)}). "
        f"Severity distribution: mild {random.randint(30, 50)}%, moderate {random.randint(25, 40)}%, "
        f"severe {random.randint(10, 25)}%, life-threatening {random.randint(1, 5)}%."
    )

    pdf.subsection_title("Conclusions")
    pdf.body_text(
        f"{'Our findings confirm that ' + drug_class.lower() + ' therapy is associated with an increased risk of ' + ae_type + ' in ' + population + '.' if significant else 'While a trend toward increased risk was observed, the association between ' + drug_class.lower() + ' use and ' + ae_type + ' in ' + population + ' did not reach statistical significance.'} "
        f"{'Clinicians should exercise heightened vigilance when prescribing ' + drug_class.lower() + 's to ' + population + ', particularly those with additional risk factors. ' if significant else ''}"
        f"Regular monitoring and early detection strategies are recommended. "
        f"Further prospective studies with larger sample sizes are warranted to validate these findings."
    )

    # Keywords
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Keywords: ", new_x="END")
    pdf.set_font("Helvetica", "I", 10)
    keywords = f"{ae_type}, {drug_class.lower()}, pharmacovigilance, {population}, drug safety, adverse drug reaction"
    safe_kw = keywords.encode("latin-1", errors="replace").decode("latin-1")
    pdf.cell(0, 6, safe_kw, new_x="LMARGIN", new_y="NEXT")

    filename = f"literature_{str(topic_idx + 1).zfill(3)}_{drug_class.lower().replace(' ', '_')}_{ae_type.replace(' ', '_')}.pdf"
    filepath = os.path.join(output_dir, filename)
    pdf.output(filepath)
    return filepath


def _generate_paper_title(drug_class, ae_type, population, study_design, study_drugs):
    """Generate a realistic academic paper title."""
    templates = [
        f"Risk of {ae_type.capitalize()} Associated with {drug_class} Therapy in {population.capitalize()}: A {study_design.title()}",
        f"Incidence and Risk Factors for {ae_type.capitalize()} Among {population.capitalize()} Treated with {drug_class}s: A {study_design.title()}",
        f"{ae_type.capitalize()} in {population.capitalize()} Receiving {study_drugs[0]['drug_name']} and Other {drug_class}s: Results from a {study_design.title()}",
        f"Comparative Safety Analysis of {drug_class}s Regarding {ae_type.capitalize()} Risk in {population.capitalize()}",
    ]
    return random.choice(templates)


# Generate 20 literature papers directly to Volume
lit_dir = f"{VOLUME_PATH}/literature"
os.makedirs(lit_dir, exist_ok=True)
lit_count = 0

for idx, (drug_class, ae_type, population) in enumerate(LITERATURE_TOPICS):
    filepath = generate_literature(idx, drug_class, ae_type, population, lit_dir)
    lit_count += 1

print(f"Generated {lit_count} medical literature PDFs in {lit_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Documents

# COMMAND ----------

print("=== Unstructured Data Verification ===\n")

for subdir in ["drug_labels", "case_reports", "literature"]:
    files = dbutils.fs.ls(f"{VOLUME_PATH}/{subdir}/")
    pdf_files = [f for f in files if f.name.endswith(".pdf")]
    total_size_mb = sum(f.size for f in pdf_files) / (1024 * 1024)
    print(f"{subdir}: {len(pdf_files)} PDFs ({total_size_mb:.1f} MB)")

print(f"\nTotal documents: {label_count + case_count + lit_count}")
print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Unstructured data generation complete. PDFs are available in:
# MAGIC - **Drug Labels**: `/Volumes/{catalog}/{schema}/unstructured_data/drug_labels/` (50 PDFs)
# MAGIC - **Case Reports**: `/Volumes/{catalog}/{schema}/unstructured_data/case_reports/` (100 PDFs)
# MAGIC - **Literature**: `/Volumes/{catalog}/{schema}/unstructured_data/literature/` (20 PDFs)
# MAGIC
# MAGIC All documents use the same drug names, conditions, and AE types as the structured data for cross-referencing.
