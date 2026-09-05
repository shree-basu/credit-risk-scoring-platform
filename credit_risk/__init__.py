"""Shared contracts for the credit-risk reference implementation."""

from .feature_contract import (
    CATEGORICAL_FEATURES,
    FEATURE_VERSION,
    MODEL_FEATURE_ALLOWLIST,
    NUMERIC_FEATURES,
)

__all__ = [
    "CATEGORICAL_FEATURES",
    "FEATURE_VERSION",
    "MODEL_FEATURE_ALLOWLIST",
    "NUMERIC_FEATURES",
]
