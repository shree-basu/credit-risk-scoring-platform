MERGE `{{ params.project_id }}.credit_risk_features.scoring_features` AS target
USING `{{ params.project_id }}.credit_risk_features.{{ params.staging_table }}` AS source
ON target.loan_id = source.loan_id
AND target.feature_date = source.feature_date
AND target.batch_id = source.batch_id
AND target.feature_version = source.feature_version
WHEN MATCHED THEN
    UPDATE SET
        borrower_id = source.borrower_id,
        annual_income = source.annual_income,
        employment_years = source.employment_years,
        credit_score = source.credit_score,
        existing_debt = source.existing_debt,
        home_ownership = source.home_ownership,
        loan_amount = source.loan_amount,
        loan_term_months = source.loan_term_months,
        interest_rate = source.interest_rate,
        loan_purpose = source.loan_purpose,
        debt_to_income = source.debt_to_income,
        loan_to_income = source.loan_to_income,
        estimated_monthly_payment = source.estimated_monthly_payment,
        payment_to_income = source.payment_to_income,
        credit_score_band = source.credit_score_band,
        employment_stability = source.employment_stability,
        published_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        loan_id, borrower_id, feature_date, batch_id, feature_version,
        annual_income, employment_years, credit_score, existing_debt,
        home_ownership, loan_amount, loan_term_months, interest_rate,
        loan_purpose, debt_to_income, loan_to_income,
        estimated_monthly_payment, payment_to_income, credit_score_band,
        employment_stability, published_at
    )
    VALUES (
        source.loan_id, source.borrower_id, source.feature_date,
        source.batch_id, source.feature_version, source.annual_income,
        source.employment_years, source.credit_score, source.existing_debt,
        source.home_ownership, source.loan_amount, source.loan_term_months,
        source.interest_rate, source.loan_purpose, source.debt_to_income,
        source.loan_to_income, source.estimated_monthly_payment,
        source.payment_to_income, source.credit_score_band,
        source.employment_stability, CURRENT_TIMESTAMP()
    );
