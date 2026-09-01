CREATE SCHEMA IF NOT EXISTS `credit_risk_scoring`
OPTIONS(location = "us-central1");

CREATE SCHEMA IF NOT EXISTS `credit_risk_audit`
OPTIONS(location = "us-central1");

CREATE TABLE IF NOT EXISTS `credit_risk_scoring.risk_scores` (
  loan_id STRING NOT NULL,
  borrower_id STRING NOT NULL,
  score_date DATE NOT NULL,
  probability_of_default NUMERIC NOT NULL,
  risk_band STRING NOT NULL,
  model_resource STRING NOT NULL,
  model_version STRING NOT NULL,
  feature_version STRING NOT NULL,
  pipeline_run_id STRING NOT NULL,
  scored_at TIMESTAMP NOT NULL
)
PARTITION BY score_date
CLUSTER BY model_version, risk_band, loan_id
OPTIONS(require_partition_filter = TRUE);

CREATE TABLE IF NOT EXISTS `credit_risk_audit.training_runs` (
  run_id STRING NOT NULL,
  training_snapshot DATE NOT NULL,
  model_resource STRING,
  model_version STRING NOT NULL,
  feature_version STRING NOT NULL,
  status STRING NOT NULL,
  metrics JSON NOT NULL,
  quality_gates JSON NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP
)
PARTITION BY training_snapshot
CLUSTER BY status, model_version;

CREATE TABLE IF NOT EXISTS `credit_risk_audit.model_assignments` (
  model_version STRING NOT NULL,
  feature_version STRING NOT NULL,
  status STRING NOT NULL,
  effective_from TIMESTAMP NOT NULL,
  effective_to TIMESTAMP,
  promoted_by STRING NOT NULL,
  promotion_run_id STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(effective_from)
CLUSTER BY status, model_version;

CREATE TABLE IF NOT EXISTS `credit_risk_audit.drift_metrics` (
  run_id STRING NOT NULL,
  feature_name STRING NOT NULL,
  reference_period DATE NOT NULL,
  current_period DATE NOT NULL,
  metric STRING NOT NULL,
  metric_value FLOAT64 NOT NULL,
  threshold FLOAT64 NOT NULL,
  status STRING NOT NULL,
  measured_at TIMESTAMP NOT NULL
)
PARTITION BY current_period
CLUSTER BY status, feature_name, metric;
