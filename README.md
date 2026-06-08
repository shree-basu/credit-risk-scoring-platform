# Credit Risk & Loan Default Prediction Platform

A production-style **batch machine-learning platform on Google Cloud** that scores loan
applicants for probability of default. Raw loan data is feature-engineered at scale with
**PySpark on Dataproc**, a default-risk model is trained, versioned, and served with
**Vertex AI**, and the whole workflow is orchestrated on a daily schedule by
**Cloud Composer (Airflow)**.

> Built as a portfolio project. The code is written exactly as it would run against real
> GCP infrastructure; cloud resources are defined in Terraform but not deployed, to avoid cost.

---

## Why this project

Lending institutions must estimate the **probability that a borrower defaults** before
approving credit. This platform automates that decision support end to end:

- ingests raw applicant & loan records,
- engineers predictive features at scale,
- trains and serves a calibrated risk model,
- writes per-applicant risk scores back to the warehouse for analysts and dashboards,
- monitors the model for data/prediction drift over time.

---

## Architecture
                      ┌──────────────────────────────────────────┐
                      │        Cloud Composer (Airflow DAG)        │
                      │        orchestrates + schedules daily      │
                      └──────────────────────────────────────────┘
                                      │ triggers each step
                                      ▼
Loan Simulator ──► GCS (raw) ──► Dataproc / PySpark ──► BigQuery (feature table)
(feature engineering) │
▼
Vertex AI Training ──► Model Registry
│
▼
Vertex AI Batch Prediction ──► BigQuery (scored)
│
┌──────────────────────────┴───────────┐
▼ ▼
Vertex AI Model Monitoring Looker Studio dashboard
(drift alerts) (risk reporting)

---
## Tech stack
| Layer | Service / Tool |
|---|---|
| Ingestion (synthetic) | Python loan simulator → Cloud Storage |
| Data lake | Google Cloud Storage (GCS) |
| Feature engineering | **Dataproc** (Apache Spark / PySpark) |
| Feature & scored store | BigQuery |
| ML lifecycle | **Vertex AI** — Training, Model Registry, Batch Prediction, Model Monitoring |
| Orchestration | **Cloud Composer** (Apache Airflow) |
| Infrastructure as Code | Terraform |
| CI/CD | GitHub Actions |
| Testing | pytest |
| Reporting | Looker Studio |
---
## Repository structure
credit-risk-scoring-platform/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│ └── simulator/
│ └── generate_loans.py # synthetic applicant + loan data → GCS
├── infra/
│ └── terraform/
│ ├── variables.tf
│ ├── main.tf
│ ├── gcs.tf
│ ├── bigquery.tf
│ ├── dataproc.tf
│ └── vertex.tf
├── spark/
│ └── feature_engineering.py # PySpark job executed on Dataproc
├── vertex/
│ ├── train.py # train model → Vertex AI Model Registry
│ ├── batch_predict.py # batch scoring job
│ └── pipeline.py # (optional) Vertex AI Pipeline definition
├── dags/
│ └── credit_risk_dag.py # Composer DAG: simulate → spark → train → predict
├── bigquery/
│ ├── feature_store.sql # feature table DDL
│ └── scored_output.sql # scored results DDL
├── tests/
│ ├── test_feature_engineering.py
│ └── test_train.py
├── monitoring/
│ └── drift_alerts.yaml # Vertex AI model-monitoring config
└── .github/
└── workflows/
└── ci.yml # lint + tests on every push

---
## Machine learning approach
- **Target:** binary label `defaulted` (1 = borrower defaulted, 0 = repaid).
- **Features:** applicant demographics, income & employment, loan amount/term/rate,
  derived ratios (e.g. debt-to-income, loan-to-income), and behavioural aggregates
  engineered in PySpark.
- **Model:** gradient-boosted classifier trained on Vertex AI, evaluated with
  ROC-AUC and precision/recall; output is a calibrated probability of default.
- **Serving:** batch prediction writes a `risk_score` per applicant to BigQuery.
- **Monitoring:** Vertex AI Model Monitoring watches for feature drift between
  training and serving distributions.
---
## Pipeline schedule
Cloud Composer runs the DAG **daily at 03:00**:
1. Generate / land new raw loan data in GCS.
2. Submit the PySpark feature-engineering job to Dataproc.
3. Load engineered features into the BigQuery feature table.
4. Trigger Vertex AI training (or skip to scoring on non-retrain days).
5. Run Vertex AI batch prediction and write risk scores to BigQuery.
6. Refresh monitoring metrics.
---
## Getting started (conceptual)
```bash
# 1. Provision infrastructure (not deployed in this portfolio build)
cd infra/terraform && terraform init && terraform plan
# 2. Generate sample loan data
python data/simulator/generate_loans.py
# 3. Run feature engineering locally (or submit to Dataproc)
spark-submit spark/feature_engineering.py
# 4. Train and register the model
python vertex/train.py
# 5. Score applicants
python vertex/batch_predict.py
Status
🚧 Built incrementally — see commit history for daily progress.
