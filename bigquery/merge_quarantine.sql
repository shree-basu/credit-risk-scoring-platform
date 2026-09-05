MERGE `{{ params.project_id }}.credit_risk_audit.quarantine_records` AS target
USING `{{ params.project_id }}.credit_risk_audit.{{ params.quarantine_staging_table }}` AS source
ON target.run_id = source.run_id
AND target.batch_id = source.batch_id
AND target.business_date = source.business_date
AND COALESCE(target.loan_id, "") = COALESCE(source.loan_id, "")
AND target.reason_code = source.reason_code
WHEN MATCHED THEN
    UPDATE SET
        entity = source.entity,
        reason_text = source.reason_text,
        rejected_at = source.rejected_at
WHEN NOT MATCHED THEN
    INSERT (
        run_id, batch_id, business_date, entity, loan_id,
        reason_code, reason_text, rejected_at
    )
    VALUES (
        source.run_id, source.batch_id, source.business_date, source.entity,
        source.loan_id, source.reason_code, source.reason_text, source.rejected_at
    );
