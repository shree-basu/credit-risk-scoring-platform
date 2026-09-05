MERGE `credit_risk_scoring.risk_scores` AS target
USING `credit_risk_staging.risk_scores_batch` AS source
ON target.loan_id = source.loan_id
  AND target.score_date = source.score_date
  AND target.model_version = source.model_version
WHEN MATCHED THEN UPDATE SET
  borrower_id = source.borrower_id,
  probability_of_default = source.probability_of_default,
  risk_band = source.risk_band,
  model_resource = source.model_resource,
  feature_version = source.feature_version,
  pipeline_run_id = source.pipeline_run_id,
  scored_at = source.scored_at
WHEN NOT MATCHED THEN
  INSERT (
    loan_id, borrower_id, score_date, probability_of_default, risk_band,
    model_resource, model_version, feature_version, pipeline_run_id, scored_at
  )
  VALUES (
    source.loan_id, source.borrower_id, source.score_date,
    source.probability_of_default, source.risk_band, source.model_resource,
    source.model_version, source.feature_version, source.pipeline_run_id, source.scored_at
  );
