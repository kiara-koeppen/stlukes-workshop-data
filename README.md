# St. Luke's AI Workshop — Synthetic Data (Data-Only Loader)

This repo has **one job**: load the workshop's synthetic data + files for all four use cases into a
Databricks catalog. It is intentionally **data only** — the base tables, volumes, and unstructured
files. During the workshop, attendees build everything else (metric views, AI functions, Genie Agents,
AI/BI dashboards) **live with Genie Code**. See each use case's User Guide for the tested prompts.

> Building the full "answer key" (metric views, AI functions, agents, dashboards, apps) lives in a
> separate internal repo. This repo is the clean starting point you hand to each workshop team.

## What it loads

| Schema | Tables | Volumes (files) |
|---|---|---|
| `clinical` (CKD) | ckd_patient_registry, clinical_notes | landing, clinical_notes_files (45 note .txt) |
| `med_diversion` | medication_activity, employee_risk, peer_group | landing, policy_docs (4 policy/SOP PDFs) |
| `htm` | medical_assets, work_orders | landing, vendor_bulletins (4 vendor EOL PDFs) |
| `huddle` | patient_demographics, transcript_extractions, physician_inputs | landing, transcripts (18 .txt) |

No metric views, AI-function tables, agents, dashboards, or apps are created here — those are built in
the workshop with Genie Code.

## How to load it — two ways

### Option A — Git folder + notebook (simplest)
1. Add this repo to your Databricks workspace as a **Git folder** (Repos), so the CSVs + files come with it.
2. Open `setup/00_load_workshop_data.py` on serverless (or any Unity Catalog cluster).
3. Set the **`catalog`** widget to a catalog you can create schemas / volumes / tables in. Leave
   `data_dir` blank to auto-detect from the repo.
4. **Run All.** Each section prints row counts so you can confirm the load.

### Option B — Databricks Asset Bundle (DAB)
Edit `databricks.yml` (set your workspace `host` + CLI `profile`, and the `catalog` variable), then:
```bash
databricks bundle deploy -t workshop
databricks bundle run load_workshop_data -t workshop
```
Known CLI gotcha: if the bundle's Terraform download fails with an expired signing key, point it at a
local terraform binary: `export DATABRICKS_TF_EXEC_PATH=$(which terraform) DATABRICKS_TF_VERSION=$(terraform version -json | python3 -c 'import sys,json;print(json.load(sys.stdin)["terraform_version"])')`, then re-run.

## Prerequisites
- A Unity Catalog catalog you can create schemas/volumes/tables in.
- A serverless SQL warehouse (or any UC cluster) to run the loader.

## Expected row counts (to confirm the load)
CKD: ckd_patient_registry **2000**, clinical_notes **1018** · Diversion: medication_activity **45000**,
employee_risk **50** · HTM: medical_assets **8000**, work_orders **52104** · Huddle:
patient_demographics **18**, physician_inputs **34**.

## Repo structure
```
setup/00_load_workshop_data.py   # the data-only loader notebook
synthetic-data/data/             # the committed CSVs + unstructured files (notes, transcripts, PDFs)
databricks.yml + resources/      # DAB for the one-command load
```
