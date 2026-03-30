# Genie Code Demo

Databricks **Genie Code** walkthrough for a pharmacovigilance scenario: synthetic data generation, end-to-end analytics and ML prompts, and guided exercises. Notebooks are exported as Python (`.py`) with Databricks notebook headers (`# Databricks notebook source`, `# MAGIC %md`, etc.)—import them into a Databricks workspace as notebooks or open them in Repos.

## Repository layout

| Path | Purpose |
|------|--------|
| `data_generation/` | Scripts to generate structured and unstructured demo data (Parquet/Delta, Unity Catalog volumes). Run these first to populate catalogs used by the demos. |
| `demo_notebooks/` | **Demos** and **exercises**: Genie Code prompts for discovery, EDA, ML, dashboards, design assistance, and drug-safety analysis. |

### Data generation

| Notebook | Description |
|----------|-------------|
| `data_generation/01_generate_structured_data.py` | Builds five pharmacovigilance tables (`patients`, `drugs`, `adverse_events`, `conditions`, `prescriptions`) as Parquet on a UC volume and registers Delta tables. Includes configurable widgets for catalog, schema, and volume. |
| `data_generation/02_generate_unstructured_data.py` | Generates complementary unstructured demo content for the same domain. |

### Demos and exercises

| File | Type | Description |
|------|------|-------------|
| `demo_notebooks/[demo] 01_genie_code_generation_e2e_pipeline.py` | Demo | Data discovery → EDA → severity ML model → AI/BI dashboard using natural-language Genie Code prompts against your pharmacovigilance schema. |
| `demo_notebooks/[exercise] 02_genie_code_explanation_and_improvements.py` | Exercise | Explanation and code-improvement prompts. |
| `demo_notebooks/[exercise] 03_code_genie_design_assistance.py` | Exercise | Design-assistance workflows with Genie Code. |
| `demo_notebooks/[exercise] 04_genie_pharmacovigilance_drug_safety.py` | Exercise | Pharmacovigilance / drug-safety focused prompts. |

## Prerequisites

- A Databricks workspace with **Unity Catalog** and permissions to create catalogs/schemas/volumes (or use existing ones aligned with the notebook widgets).
- **Genie Code** (or the product features referenced in the prompts) enabled where you run the notebooks.
- For data generation: cluster or serverless SQL/Spark with `faker` (see `%pip` cells in the generators).

## Suggested order

1. Run `data_generation/01_generate_structured_data.py` (and `02_generate_unstructured_data.py` if you need that slice of the demo).
2. Open `demo_notebooks/[demo] 01_genie_code_generation_e2e_pipeline.py` and follow the steps (discovery → EDA → ML → dashboard).
3. Work through the `[exercise]` notebooks as needed.

## Configuration

Structured data generation uses widgets such as **catalog**, **schema**, and **volume** names. Point them at your UC location before running cells that write data.

## License

Content is provided as demo material for learning and presentations; adapt prompts and data paths to your environment and governance rules.
