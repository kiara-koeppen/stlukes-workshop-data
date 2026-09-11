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
SELECT CAST(pat_id AS STRING) pat_id, CAST(huddle_date AS DATE) huddle_date,
 CAST(source_transcript_file AS STRING) source_transcript_file,
 CAST(visit_complexity_projection AS STRING) visit_complexity_projection,
 CAST(provider_identified_issues AS STRING) provider_identified_issues,
 CAST(medical_drivers AS STRING) medical_drivers,
 CAST(patient_identified_issues AS STRING) patient_identified_issues,
 CAST(history_of_job_modifications AS STRING) history_of_job_modifications,
 CAST(psychosocial_complexity_projection AS STRING) psychosocial_complexity_projection,
 CAST(social_determinants_of_health AS STRING) social_determinants_of_health,
 CAST(hidden_contextual_factors AS STRING) hidden_contextual_factors,
 CAST(negotiability AS STRING) negotiability,
 CAST(relationship_context AS STRING) relationship_context,
 CAST(relationship_equity_with_care_team AS STRING) relationship_equity_with_care_team
FROM read_files('/Volumes/{catalog}/huddle/landing/transcript_extractions.csv', format=>'csv', header=>true, mode=>'PERMISSIVE')
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

# MAGIC %md ### 5. Metadata for AI performance -- comments + primary/foreign keys
# MAGIC Table and column **comments** plus **PRIMARY KEY / FOREIGN KEY** constraints (informational
# MAGIC `RELY`) give Genie, Genie Code, and AI/BI dashboards the semantics they need to write correct
# MAGIC SQL and infer joins. This runs after the base tables are (re)created. Constraints are not
# MAGIC enforced by Databricks (data was validated at generation time) -- `RELY` tells the optimizer
# MAGIC and the AI tools to trust them.

# COMMAND ----------

def _q(s):
    return s.replace("'", "''")

# --- Table comments ---------------------------------------------------------
TABLE_COMMENTS = {
    "clinical.ckd_patient_registry": "CKD (chronic kidney disease) population-health registry. One row per patient, combining EHR problem-list documentation with lab-derived kidney function (serial creatinine and eGFR, microalbumin) so undocumented or under-managed CKD can be surfaced for outreach and nephrology referral.",
    "clinical.clinical_notes": "Free-text clinical notes for registry patients. Used for AI extraction of CKD signals not captured in the structured registry. One row per note.",
    "med_diversion.medication_activity": "Controlled-substance medication events (dispense, administer, waste, and related actions) used to detect possible drug diversion by staff. One row per medication transaction.",
    "med_diversion.employee_risk": "Staff who handle controlled substances, with peer-group assignment and a periodic IRIS diversion-risk score. One row per employee.",
    "med_diversion.peer_group": "Reference table defining peer groups (by role and unit) used to benchmark an employee's medication activity against similar staff. One row per peer group.",
    "htm.medical_assets": "Inventory of medical devices managed by Healthcare Technology Management (HTM / biomed), with lifecycle dates, replacement cost, and risk score, for capital replacement planning. One row per asset.",
    "htm.work_orders": "Maintenance work-order history for medical assets (corrective and preventive), used to analyze maintenance burden and forecast future volume. One row per work order.",
    "huddle.patient_demographics": "Patients scheduled for a care-team daily huddle, with basic demographics and the huddle date. One row per patient per huddle.",
    "huddle.physician_inputs": "Physician-provided huddle inputs per patient: complexity and relationship scores and the optimal vs actually-assigned care-team member. One row per patient / provider / huddle date.",
    "huddle.transcript_extractions": "Structured factors extracted from care-team huddle meeting transcripts (barriers to care, social determinants, complexity signals). One row per patient per huddle.",
}

# --- Column comments --------------------------------------------------------
COLUMN_COMMENTS = {
    "clinical.ckd_patient_registry": {
        "patient_id": "Primary key. Stable patient identifier used across the clinical schema.",
        "patient_num": "Secondary EHR patient number (integer).",
        "sex": "Patient sex (M/F).",
        "dob": "Date of birth.",
        "age": "Patient age in years.",
        "assigned_provider_name": "Primary care provider / clinic assigned to the patient.",
        "chronic_conditions": "Comma-separated list of documented chronic conditions.",
        "chronic_condition_count": "Number of documented chronic conditions.",
        "ckd_in_problem_list": "Whether CKD appears on the patient's EHR problem list (boolean).",
        "documented_ckd": "Documented CKD status in the EHR: 'Yes', 'No', or 'None'. Compare against has_ckd to find undocumented CKD (the care gap).",
        "creatinine_1": "Most recent serum creatinine (mg/dL).",
        "creatinine_2": "Second most recent serum creatinine (mg/dL).",
        "creatinine_3": "Third most recent serum creatinine (mg/dL).",
        "gfr_1": "Most recent estimated GFR (mL/min/1.73m2). Lower values indicate worse kidney function.",
        "gfr_2": "Second most recent estimated GFR.",
        "gfr_3": "Third most recent estimated GFR.",
        "microalbumin_value": "Urine microalbumin value.",
        "microalbumin_category": "KDIGO albuminuria category (A1/A2/A3).",
        "microalbumin_date": "Date of the microalbumin measurement.",
        "has_ckd": "Ground-truth CKD flag derived from labs/KDIGO staging (boolean). A patient with has_ckd=true but documented_ckd not 'Yes' is an undocumented-CKD care gap.",
        "actual_ckd_stage": "Lab-derived KDIGO CKD stage (e.g., 1, 2, 3a, 3b, 4, 5). Stage 4-5 are advanced.",
        "sees_nephrology": "Whether the patient is under nephrology care (boolean). Advanced-stage patients not seeing nephrology are high-risk.",
        "notes": "Short free-text note summary on the registry row.",
        "key_meds": "Key nephrology-relevant medications for the patient.",
    },
    "clinical.clinical_notes": {
        "patient_id": "Foreign key to clinical.ckd_patient_registry.patient_id.",
        "note_id": "Primary key. Unique clinical note identifier.",
        "note_date": "Date the note was authored.",
        "author": "Note author (provider).",
        "note_type": "Type of note (e.g., progress, consult).",
        "note_text": "Free-text clinical note content used for AI extraction of CKD/nephrology signals.",
    },
    "med_diversion.medication_activity": {
        "transaction_id": "Primary key. Unique medication transaction identifier.",
        "event_datetime": "Timestamp of the medication event.",
        "shift": "Shift during which the event occurred (day/evening/night).",
        "employee_id": "Foreign key to med_diversion.employee_risk.employee_id. The staff member performing the action.",
        "employee_name": "Name of the staff member.",
        "employee_role": "Role of the staff member (e.g., RN).",
        "department": "Department where the event occurred.",
        "unit": "Unit where the event occurred.",
        "patient_id": "Patient associated with the medication event.",
        "medication": "Medication name.",
        "med_class": "Medication class.",
        "dea_schedule": "DEA controlled-substance schedule (e.g., CII). CII are the most tightly controlled.",
        "event_type": "Action type: dispense, administer, waste, etc. A 'waste' with a null witness_id is unwitnessed waste (a key diversion signal).",
        "dose_amount": "Dose amount.",
        "dose_unit": "Dose unit (e.g., mg).",
        "order_id": "Associated medication order id.",
        "order_datetime": "Timestamp the order was placed.",
        "admin_datetime": "Timestamp the medication was administered.",
        "pain_score_before": "Patient pain score before administration (0-10).",
        "pain_score_after": "Patient pain score after administration (0-10). No improvement after a controlled-substance admin is a diversion signal.",
        "witness_id": "Employee id of the waste witness. NULL on a waste event = unwitnessed waste (a primary diversion signal).",
        "off_shift_flag": "True if the event occurred outside the employee's normal shift (a diversion signal).",
        "out_of_department_flag": "True if the event occurred outside the employee's normal department (a diversion signal).",
    },
    "med_diversion.employee_risk": {
        "employee_id": "Primary key. Unique staff identifier.",
        "employee_name": "Staff member name.",
        "role": "Staff role (e.g., RN).",
        "department": "Staff member's home department.",
        "peer_group_id": "Foreign key to med_diversion.peer_group.peer_group_id. The peer cohort used for benchmarking.",
        "iris_score": "Periodic IRIS diversion-risk score (higher = higher modeled risk).",
        "scored_month": "Month the IRIS score applies to.",
    },
    "med_diversion.peer_group": {
        "peer_group_id": "Primary key. Unique peer-group identifier.",
        "role": "Role that defines the peer group (e.g., RN).",
        "unit": "Unit that defines the peer group.",
        "description": "Human-readable description of the peer group.",
    },
    "htm.medical_assets": {
        "asset_number": "Primary key. Unique medical asset (device) identifier.",
        "asset_description": "Description / type of the device (e.g., infusion pump).",
        "manufacturer": "Device manufacturer.",
        "model_number": "Model number.",
        "serial_number": "Serial number.",
        "facility": "Facility where the asset is located.",
        "department": "Department where the asset is located.",
        "purchase_date": "Date the asset was purchased.",
        "install_date": "Date the asset was installed / placed in service.",
        "support_end_date": "Manufacturer end-of-support (end-of-life) date. Assets past or near this date are replacement candidates.",
        "operating_system": "Embedded operating system, where applicable (cybersecurity relevance).",
        "ip_address": "Network IP address, where applicable.",
        "mac_address": "Network MAC address, where applicable.",
        "device_status": "Lifecycle status (e.g., active, retired).",
        "replacement_cost": "Estimated replacement cost in USD. Sum for capital planning.",
        "risk_score": "Composite device risk score (higher = higher priority for replacement).",
    },
    "htm.work_orders": {
        "work_order_id": "Primary key. Unique work-order identifier.",
        "asset_number": "Foreign key to htm.medical_assets.asset_number.",
        "work_order_type": "Work order type: corrective (repair) or preventive (scheduled maintenance).",
        "request_date": "Date the work order was requested.",
        "completion_date": "Date the work order was completed (null if open).",
        "technician_id": "Technician assigned to the work order.",
        "labor_hours": "Labor hours spent on the work order.",
        "status": "Work order status (e.g., open, closed).",
    },
    "huddle.patient_demographics": {
        "pat_id": "Primary key. Stable patient identifier used across the huddle schema.",
        "pat_name": "Patient name.",
        "birth_date": "Date of birth.",
        "home_phone": "Home phone number.",
        "age": "Patient age in years.",
        "sex": "Patient sex (M/F).",
        "clinic": "Clinic the patient is seen at (e.g., Nampa).",
        "huddle_date": "Date of the care-team huddle the patient is scheduled for.",
        "scheduled_visit_reason": "Reason for the scheduled visit.",
    },
    "huddle.physician_inputs": {
        "pat_id": "Part of the primary key and a foreign key to huddle.patient_demographics.pat_id.",
        "huddle_date": "Part of the primary key. Date of the huddle.",
        "provider_id": "Part of the primary key. Provider giving the input.",
        "provider_name": "Provider name.",
        "provider_patient_relationship_score": "Physician-rated provider-patient relationship strength (-10 to 10).",
        "patient_complexity_score": "Physician-rated patient complexity (higher = more complex).",
        "physician_notes": "Free-text physician notes for the huddle.",
        "optimal_team_member": "The care-team member the physician judges is the best fit for this patient.",
        "assigned_team_member": "The care-team member actually assigned. When it differs from optimal_team_member, the assignment is non-optimal.",
        "non_optimal_assignment_notes": "Explanation when the assignment is not the optimal team member.",
        "created_at": "Timestamp the input was recorded.",
    },
    "huddle.transcript_extractions": {
        "pat_id": "Part of the primary key and a foreign key to huddle.patient_demographics.pat_id.",
        "huddle_date": "Part of the primary key. Date of the huddle.",
        "source_transcript_file": "Filename of the huddle transcript the factors were extracted from.",
        "visit_complexity_projection": "Extracted projection of visit complexity.",
        "provider_identified_issues": "Issues the provider raised in the huddle.",
        "medical_drivers": "Medical drivers of complexity discussed.",
        "patient_identified_issues": "Issues the patient raised.",
        "history_of_job_modifications": "Any history of work/job modifications discussed.",
        "psychosocial_complexity_projection": "Extracted projection of psychosocial complexity.",
        "social_determinants_of_health": "Social determinants of health (SDOH) mentioned (e.g., transportation, housing).",
        "hidden_contextual_factors": "Hidden contextual factors surfaced in discussion.",
        "negotiability": "Extracted assessment of care-plan negotiability.",
        "relationship_context": "Context on the patient's relationship with the care team.",
        "relationship_equity_with_care_team": "Extracted assessment of relationship equity with the care team.",
    },
}

# --- Primary keys: (table, [key columns]) -----------------------------------
PRIMARY_KEYS = [
    ("clinical.ckd_patient_registry", ["patient_id"]),
    ("clinical.clinical_notes", ["note_id"]),
    ("med_diversion.medication_activity", ["transaction_id"]),
    ("med_diversion.employee_risk", ["employee_id"]),
    ("med_diversion.peer_group", ["peer_group_id"]),
    ("htm.medical_assets", ["asset_number"]),
    ("htm.work_orders", ["work_order_id"]),
    ("huddle.patient_demographics", ["pat_id"]),
    ("huddle.physician_inputs", ["pat_id", "provider_id", "huddle_date"]),
    ("huddle.transcript_extractions", ["pat_id", "huddle_date"]),
]

# --- Foreign keys: (child, [child cols], parent, [parent cols]) --------------
FOREIGN_KEYS = [
    ("clinical.clinical_notes", ["patient_id"], "clinical.ckd_patient_registry", ["patient_id"]),
    ("med_diversion.medication_activity", ["employee_id"], "med_diversion.employee_risk", ["employee_id"]),
    ("med_diversion.employee_risk", ["peer_group_id"], "med_diversion.peer_group", ["peer_group_id"]),
    ("htm.work_orders", ["asset_number"], "htm.medical_assets", ["asset_number"]),
    ("huddle.physician_inputs", ["pat_id"], "huddle.patient_demographics", ["pat_id"]),
    ("huddle.transcript_extractions", ["pat_id"], "huddle.patient_demographics", ["pat_id"]),
]

# Apply table + column comments
for tbl, c in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE `{catalog}`.{tbl} IS '{_q(c)}'")
for tbl, cols in COLUMN_COMMENTS.items():
    for col, c in cols.items():
        spark.sql(f"ALTER TABLE `{catalog}`.{tbl} ALTER COLUMN {col} COMMENT '{_q(c)}'")

# Apply primary keys (columns must be NOT NULL first). RELY = trusted by optimizer + AI tools.
for tbl, cols in PRIMARY_KEYS:
    for col in cols:
        spark.sql(f"ALTER TABLE `{catalog}`.{tbl} ALTER COLUMN {col} SET NOT NULL")
    cname = tbl.split(".")[-1] + "_pk"
    spark.sql(f"ALTER TABLE `{catalog}`.{tbl} DROP CONSTRAINT IF EXISTS {cname}")
    spark.sql(f"ALTER TABLE `{catalog}`.{tbl} ADD CONSTRAINT {cname} PRIMARY KEY ({', '.join(cols)}) RELY")

# Apply foreign keys (parent PKs now exist). RELY so Genie/optimizer infer the joins.
for child, ccols, parent, pcols in FOREIGN_KEYS:
    cname = child.split(".")[-1] + "_" + ccols[0] + "_fk"
    spark.sql(f"ALTER TABLE `{catalog}`.{child} DROP CONSTRAINT IF EXISTS {cname}")
    spark.sql(f"ALTER TABLE `{catalog}`.{child} ADD CONSTRAINT {cname} FOREIGN KEY ({', '.join(ccols)}) "
              f"REFERENCES `{catalog}`.{parent} ({', '.join(pcols)}) RELY")

print("Applied table/column comments + PRIMARY KEY / FOREIGN KEY (RELY) constraints to all base tables.")

# COMMAND ----------

# MAGIC %md ## Done -- data-only load complete
# MAGIC All base tables, volumes, and unstructured files (clinical notes, huddle transcripts, and the
# MAGIC Diversion policy / HTM vendor-bulletin PDFs) are loaded into your catalog.
# MAGIC
# MAGIC This repo intentionally loads **data only**. During the workshop, you build everything else -- the
# MAGIC metric views, AI functions, Genie Agents, AI/BI dashboards -- **live with Genie Code**. See your
# MAGIC use case's User Guide for the tested prompts.
