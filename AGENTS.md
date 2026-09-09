# AGENTS.md — St. Luke's Workshop Data Loader (context for AI assistants)

This repo has ONE job: load the synthetic data + files for the St. Luke's x Databricks workshop's four
use cases (CKD, Medication Diversion, HTM, AI Huddle) into a Databricks Unity Catalog. It is **data
only** — base tables, volumes, and unstructured files (clinical notes, huddle transcripts, and the
Diversion policy / HTM vendor PDFs). During the workshop, attendees build everything else (metric views,
AI functions, Genie Agents, AI/BI dashboards) LIVE with **Genie Code**.

How to help the user: see the README for the two load paths (Git folder + notebook `setup/00_load_workshop_data.py`,
or the DAB). Expected row counts and the required workspace previews are in the README.

For the FULL workshop context (all 4 use cases, the guides, the verified Genie Code prompts, the reference
assets, and the gotchas), see the **answer-key repo and its AGENTS.md**:
https://github.com/kiara-koeppen/stlukes-ai-workshop  (see `AGENTS.md` there), and the **Master Index**
Google Doc: https://docs.google.com/document/d/1E1gFzNvkswDZdYoC_MErBpN__x1SCEA01MehG_1UGk4/edit
