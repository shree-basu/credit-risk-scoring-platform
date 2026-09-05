CREATE SCHEMA IF NOT EXISTS `credit_risk_features`
OPTIONS (location = "us-central1");

CREATE SCHEMA IF NOT EXISTS `credit_risk_audit`
OPTIONS (location = "us-central1");

CREATE TABLE IF NOT EXISTS `credit_risk_features.training_features` (
    loan_id STRING NOT NULL,
    borrower_id STRING NOT NULL,
    feature_date DATE NOT NULL,
    batch_id STRING NOT NULL,
    feature_version STRING NOT NULL,
    annual_income NUMERIC NOT NULL,
    employment_years INT64 NOT NULL,
    credit_score INT64 NOT NULL,
    existing_debt NUMERIC NOT NULL,
    home_ownership STRING NOT NULL,
    loan_amount NUMERIC NOT NULL,
    loan_term_months INT64 NOT NULL,
    interest_rate NUMERIC NOT NULL,
    loan_purpose STRING NOT NULL,
    debt_to_income NUMERIC NOT NULL,
    loan_to_income NUMERIC NOT NULL,
    estimated_monthly_payment NUMERIC NOT NULL,
    payment_to_income NUMERIC NOT NULL,
    credit_score_band STRING NOT NULL,
    employment_stability STRING NOT NULL,
    defaulted INT64 NOT NULL,
    published_at TIMESTAMP NOT NULL
)
PARTITION BY feature_date
CLUSTER BY loan_id, feature_version
OPTIONS (require_partition_filter = TRUE);

CREATE TABLE IF NOT EXISTS `credit_risk_features.scoring_features` (
    loan_id STRING NOT NULL,
    borrower_id STRING NOT NULL,
    feature_date DATE NOT NULL,
    batch_id STRING NOT NULL,
    feature_version STRING NOT NULL,
    annual_income NUMERIC NOT NULL,
    employment_years INT64 NOT NULL,
    credit_score INT64 NOT NULL,
    existing_debt NUMERIC NOT NULL,
    home_ownership STRING NOT NULL,
    loan_amount NUMERIC NOT NULL,
    loan_term_months INT64 NOT NULL,
    interest_rate NUMERIC NOT NULL,
    loan_purpose STRING NOT NULL,
    debt_to_income NUMERIC NOT NULL,
    loan_to_income NUMERIC NOT NULL,
    estimated_monthly_payment NUMERIC NOT NULL,
    payment_to_income NUMERIC NOT NULL,
    credit_score_band STRING NOT NULL,
    employment_stability STRING NOT NULL,
    published_at TIMESTAMP NOT NULL
)
PARTITION BY feature_date
CLUSTER BY loan_id, feature_version
OPTIONS (require_partition_filter = TRUE);

CREATE TABLE IF NOT EXISTS `credit_risk_audit.quarantine_records` (
    run_id STRING NOT NULL,
    batch_id STRING NOT NULL,
    business_date DATE NOT NULL,
    entity STRING NOT NULL,
    loan_id STRING,
    reason_code STRING NOT NULL,
    reason_text STRING NOT NULL,
    rejected_at TIMESTAMP NOT NULL
)
PARTITION BY business_date
CLUSTER BY batch_id, reason_code
OPTIONS (require_partition_filter = TRUE);

CREATE TABLE IF NOT EXISTS `credit_risk_audit.dq_results` (
    run_id STRING NOT NULL,
    business_date DATE NOT NULL,
    check_name STRING NOT NULL,
    status STRING NOT NULL,
    observed_value NUMERIC,
    expected_value NUMERIC,
    checked_at TIMESTAMP NOT NULL
)
PARTITION BY business_date
CLUSTER BY run_id, status
OPTIONS (require_partition_filter = TRUE);
