"""Versioned feature contract shared by Spark, training and prediction."""

from typing import Final

FEATURE_VERSION: Final = "v1"

NUMERIC_FEATURES: Final = (
    "annual_income",
    "employment_years",
    "credit_score",
    "existing_debt",
    "loan_amount",
    "loan_term_months",
    "interest_rate",
    "debt_to_income",
    "loan_to_income",
    "estimated_monthly_payment",
    "payment_to_income",
)

CATEGORICAL_FEATURES: Final = (
    "home_ownership",
    "loan_purpose",
    "credit_score_band",
    "employment_stability",
)

# Identity and policy-sensitive audit fields, including age, are intentionally absent.
MODEL_FEATURE_ALLOWLIST: Final = (*NUMERIC_FEATURES, *CATEGORICAL_FEATURES)
