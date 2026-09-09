# Databricks notebook source
# MAGIC %md
# MAGIC # St. Luke's AI Workshop - Synthetic Data Loader (DATA ONLY)
# MAGIC
# MAGIC Loads the synthetic data + files for ALL four use cases (CKD, Medication Diversion, HTM, AI Huddle)
# MAGIC into any Databricks environment from the CSVs and files committed in this repo.
# MAGIC
# MAGIC **This repo loads DATA ONLY** -- the base tables, volumes, and unstructured files. During the
# MAGIC workshop, attendees build everything else (metric views, AI functions, Genie Agents, AI/BI
# MAGIC dashboards) LIVE with Genie Code. See each use case's User Guide for the tested prompts.
# MAGIC
# MAGIC **How to run:** clone this repo into Databricks as a Git folder, open this notebook on serverless
# MAGIC (or any Unity Catalog cluster), set the `catalog` widget to a catalog you can create schemas /
# MAGIC volumes / tables in, and Run All. Each section prints row counts so you can confirm the load.
# MAGIC
# MAGIC Schema/table/column names mirror the SLHS prod paths, so moving to prod is just the `catalog` widget.

# COMMAND ----------

dbutils.widgets.text("catalog", "healthcare_ai", "Target catalog")
dbutils.widgets.text("data_dir", "", "Path to synthetic-data/data (blank = auto-detect from repo)")
catalog = dbutils.widgets.get("catalog").strip()

import os
data_dir = dbutils.widgets.get("data_dir").strip()
if not data_dir:
    # Auto-detect: this notebook lives at <repo>/setup/00_load_all_data; data is at <repo>/synthetic-data/data
    nb_path = (dbutils.notebook.entry_point.getDbutils().notebook()
               .getContext().notebookPath().get())
    repo_root = "/".join(nb_path.split("/")[:-2])          # strip /setup/<notebook>
    data_dir = f"/Workspace{repo_root}/synthetic-data/data"
print(f"catalog  = {catalog}")
print(f"data_dir = {data_dir}")
assert os.path.isdir(data_dir), f"data_dir not found: {data_dir}. Set the data_dir widget to <repo>/synthetic-data/data"

# COMMAND ----------

# MAGIC %md ### Helpers

# COMMAND ----------

def ensure_schema(schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

def ensure_volume(schema, volume):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

def stage_csv(schema, csv_relpath, volume="landing"):
    """Copy a repo CSV into a UC Volume so read_files can load it with explicit types."""
    src = f"file:{data_dir}/{csv_relpath}"
    fname = csv_relpath.split("/")[-1]
    dst = f"/Volumes/{catalog}/{schema}/{volume}/{fname}"
    dbutils.fs.cp(src, dst)
    return dst

def stage_dir(schema, dir_relpath, volume):
    """Recursively copy a repo folder of unstructured files into a UC Volume."""
    src = f"file:{data_dir}/{dir_relpath}"
    dst = f"/Volumes/{catalog}/{schema}/{volume}/"
    dbutils.fs.cp(src, dst, recurse=True)

def count(fqn):
    return spark.table(fqn).count()

# COMMAND ----------

# MAGIC %md ### 1. CKD  (clinical)

# COMMAND ----------

ensure_schema("clinical")
ensure_volume("clinical", "landing")
stage_csv("clinical", "ckd/ckd_patient_registry.csv")
stage_csv("clinical", "ckd/clinical_notes.csv")

spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.clinical.ckd_patient_registry AS
SELECT CAST(patient_id AS STRING) patient_id, CAST(patient_num AS INT) patient_num, CAST(sex AS STRING) sex,
 CAST(dob AS DATE) dob, CAST(age AS INT) age, CAST(assigned_provider_name AS STRING) assigned_provider_name,
 CAST(chronic_conditions AS STRING) chronic_conditions, CAST(chronic_condition_count AS INT) chronic_condition_count,
 CAST(ckd_in_problem_list AS BOOLEAN) ckd_in_problem_list, CAST(documented_ckd AS STRING) documented_ckd,
 CAST(creatinine_1 AS DECIMAL(5,2)) creatinine_1, CAST(creatinine_2 AS DECIMAL(5,2)) creatinine_2, CAST(creatinine_3 AS DECIMAL(5,2)) creatinine_3,
 CAST(gfr_1 AS INT) gfr_1, CAST(gfr_2 AS INT) gfr_2, CAST(gfr_3 AS INT) gfr_3,
 try_cast(microalbumin_value AS DECIMAL(6,1)) microalbumin_value, CAST(microalbumin_category AS STRING) microalbumin_category,
 try_cast(microalbumin_date AS DATE) microalbumin_date, CAST(has_ckd AS BOOLEAN) has_ckd,
 CAST(actual_ckd_stage AS STRING) actual_ckd_stage, CAST(sees_nephrology AS BOOLEAN) sees_nephrology,
 CAST(notes AS STRING) notes, CAST(key_meds AS STRING) key_meds
FROM read_files('/Volumes/{catalog}/clinical/landing/ckd_patient_registry.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.clinical.clinical_notes AS
SELECT CAST(patient_id AS STRING) patient_id, CAST(note_id AS STRING) note_id, CAST(note_date AS DATE) note_date,
 CAST(author AS STRING) author, CAST(note_type AS STRING) note_type, CAST(note_text AS STRING) note_text
FROM read_files('/Volumes/{catalog}/clinical/landing/clinical_notes.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")

# Unstructured notes for Genie-on-Volumes / ai_parse_document
ensure_volume("clinical", "clinical_notes_files")
stage_dir("clinical", "ckd/notes_files", "clinical_notes_files")
print("CKD registry:", count(f"{catalog}.clinical.ckd_patient_registry"), "| notes:", count(f"{catalog}.clinical.clinical_notes"))

# COMMAND ----------

# MAGIC %md ### 2. Medication Diversion  (med_diversion)

# COMMAND ----------

ensure_schema("med_diversion")
ensure_volume("med_diversion", "landing")
for f in ["medication_activity.csv", "employee_risk.csv", "peer_group.csv"]:
    stage_csv("med_diversion", f"diversion/{f}")

spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.med_diversion.medication_activity AS
SELECT CAST(transaction_id AS STRING) transaction_id, CAST(event_datetime AS TIMESTAMP) event_datetime, CAST(shift AS STRING) shift,
 CAST(employee_id AS STRING) employee_id, CAST(employee_name AS STRING) employee_name, CAST(employee_role AS STRING) employee_role,
 CAST(department AS STRING) department, CAST(unit AS STRING) unit, CAST(patient_id AS STRING) patient_id,
 CAST(medication AS STRING) medication, CAST(med_class AS STRING) med_class, CAST(dea_schedule AS STRING) dea_schedule,
 CAST(event_type AS STRING) event_type, CAST(dose_amount AS DECIMAL(8,2)) dose_amount, CAST(dose_unit AS STRING) dose_unit,
 CAST(order_id AS STRING) order_id, try_cast(order_datetime AS TIMESTAMP) order_datetime, try_cast(admin_datetime AS TIMESTAMP) admin_datetime,
 try_cast(pain_score_before AS INT) pain_score_before, try_cast(pain_score_after AS INT) pain_score_after,
 CAST(witness_id AS STRING) witness_id, CAST(off_shift_flag AS BOOLEAN) off_shift_flag, CAST(out_of_department_flag AS BOOLEAN) out_of_department_flag
FROM read_files('/Volumes/{catalog}/med_diversion/landing/medication_activity.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.med_diversion.employee_risk AS
SELECT CAST(employee_id AS STRING) employee_id, CAST(employee_name AS STRING) employee_name, CAST(role AS STRING) role,
 CAST(department AS STRING) department, CAST(peer_group_id AS STRING) peer_group_id,
 CAST(iris_score AS DECIMAL(5,2)) iris_score, try_cast(scored_month AS DATE) scored_month
FROM read_files('/Volumes/{catalog}/med_diversion/landing/employee_risk.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.med_diversion.peer_group AS
SELECT CAST(peer_group_id AS STRING) peer_group_id, CAST(role AS STRING) role, CAST(unit AS STRING) unit, CAST(description AS STRING) description
FROM read_files('/Volumes/{catalog}/med_diversion/landing/peer_group.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
# Unstructured policy / SOP PDFs for Genie-on-Volumes / ai_parse_document
ensure_volume("med_diversion", "policy_docs")
stage_dir("med_diversion", "diversion/docs", "policy_docs")
print("med activity:", count(f"{catalog}.med_diversion.medication_activity"),
      "| employees:", count(f"{catalog}.med_diversion.employee_risk"))

# COMMAND ----------

# MAGIC %md ### 3. HTM Equipment Planning  (htm)

# COMMAND ----------

ensure_schema("htm")
ensure_volume("htm", "landing")
stage_csv("htm", "htm/medical_assets.csv")
stage_csv("htm", "htm/work_orders.csv")

spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.htm.medical_assets AS
SELECT CAST(asset_number AS STRING) asset_number, CAST(asset_description AS STRING) asset_description,
 CAST(manufacturer AS STRING) manufacturer, CAST(model_number AS STRING) model_number, CAST(serial_number AS STRING) serial_number,
 CAST(facility AS STRING) facility, CAST(department AS STRING) department,
 CAST(purchase_date AS DATE) purchase_date, CAST(install_date AS DATE) install_date, CAST(support_end_date AS DATE) support_end_date,
 CAST(operating_system AS STRING) operating_system, CAST(ip_address AS STRING) ip_address, CAST(mac_address AS STRING) mac_address,
 CAST(device_status AS STRING) device_status, CAST(replacement_cost AS DECIMAL(12,2)) replacement_cost, CAST(risk_score AS DECIMAL(5,2)) risk_score
FROM read_files('/Volumes/{catalog}/htm/landing/medical_assets.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.htm.work_orders AS
SELECT CAST(work_order_id AS STRING) work_order_id, CAST(asset_number AS STRING) asset_number, CAST(work_order_type AS STRING) work_order_type,
 CAST(request_date AS DATE) request_date, try_cast(completion_date AS DATE) completion_date, CAST(technician_id AS STRING) technician_id,
 CAST(labor_hours AS DECIMAL(5,2)) labor_hours, CAST(status AS STRING) status
FROM read_files('/Volumes/{catalog}/htm/landing/work_orders.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
# Unstructured vendor EOL bulletin PDFs for Genie-on-Volumes / ai_parse_document
ensure_volume("htm", "vendor_bulletins")
stage_dir("htm", "htm/docs", "vendor_bulletins")
print("assets:", count(f"{catalog}.htm.medical_assets"), "| work_orders:", count(f"{catalog}.htm.work_orders"))

# COMMAND ----------

# MAGIC %md ### 4. AI Huddle Management  (huddle)

# COMMAND ----------

ensure_schema("huddle")
ensure_volume("huddle", "landing")
for f in ["patient_demographics.csv", "transcript_extractions.csv", "physician_inputs.csv"]:
    stage_csv("huddle", f"huddle/{f}")

spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.huddle.patient_demographics AS
SELECT CAST(pat_id AS STRING) pat_id, CAST(pat_name AS STRING) pat_name, CAST(birth_date AS DATE) birth_date,
 CAST(home_phone AS STRING) home_phone, CAST(age AS INT) age, CAST(sex AS STRING) sex,
 CAST(clinic AS STRING) clinic, CAST(huddle_date AS DATE) huddle_date, CAST(scheduled_visit_reason AS STRING) scheduled_visit_reason
FROM read_files('/Volumes/{catalog}/huddle/landing/patient_demographics.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.huddle.transcript_extractions AS
SELECT * FROM read_files('/Volumes/{catalog}/huddle/landing/transcript_extractions.csv', format=>'csv', header=>true, inferSchema=>true, mode=>'PERMISSIVE')
""")
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.huddle.physician_inputs AS
SELECT CAST(pat_id AS STRING) pat_id, CAST(huddle_date AS DATE) huddle_date, CAST(provider_id AS STRING) provider_id,
 CAST(provider_name AS STRING) provider_name, CAST(provider_patient_relationship_score AS INT) provider_patient_relationship_score,
 CAST(patient_complexity_score AS INT) patient_complexity_score, CAST(physician_notes AS STRING) physician_notes,
 CAST(optimal_team_member AS STRING) optimal_team_member, CAST(assigned_team_member AS STRING) assigned_team_member,
 CAST(non_optimal_assignment_notes AS STRING) non_optimal_assignment_notes, try_cast(created_at AS TIMESTAMP) created_at
FROM read_files('/Volumes/{catalog}/huddle/landing/physician_inputs.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
""")
# Raw Teams huddle transcripts for AI extraction / Genie-on-Volumes
ensure_volume("huddle", "transcripts")
stage_dir("huddle", "huddle/transcripts", "transcripts")
print("patients:", count(f"{catalog}.huddle.patient_demographics"),
      "| physician_inputs:", count(f"{catalog}.huddle.physician_inputs"))

# COMMAND ----------

# MAGIC %md ## Done -- data-only load complete
# MAGIC All base tables, volumes, and unstructured files (clinical notes, huddle transcripts, and the
# MAGIC Diversion policy / HTM vendor-bulletin PDFs) are loaded into your catalog.
# MAGIC
# MAGIC This repo intentionally loads **data only**. During the workshop, you build everything else -- the
# MAGIC metric views, AI functions, Genie Agents, AI/BI dashboards -- **live with Genie Code**. See your
# MAGIC use case's User Guide for the tested prompts.
