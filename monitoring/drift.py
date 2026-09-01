"""Deterministic feature and score drift metrics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DriftMetric:
    feature_name: str
    reference_period: date
    current_period: date
    metric: str
    metric_value: float
    threshold: float
    status: str
    measured_at: datetime

    def as_record(self) -> dict[str, object]:
        return asdict(self)


def population_stability_index(
    reference: pd.Series, current: pd.Series, *, bins: int = 10, epsilon: float = 1e-6
) -> float:
    reference_values = pd.to_numeric(reference, errors="coerce").dropna().to_numpy()
    current_values = pd.to_numeric(current, errors="coerce").dropna().to_numpy()
    if not len(reference_values) or not len(current_values):
        raise ValueError("PSI requires non-empty numeric reference and current values")
    edges = np.unique(np.quantile(reference_values, np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:
        edges = np.array([-np.inf, np.inf])
    else:
        edges[0], edges[-1] = -np.inf, np.inf
    reference_count = np.histogram(reference_values, bins=edges)[0].astype(float)
    current_count = np.histogram(current_values, bins=edges)[0].astype(float)
    reference_rate = np.maximum(reference_count / reference_count.sum(), epsilon)
    current_rate = np.maximum(current_count / current_count.sum(), epsilon)
    return float(np.sum((current_rate - reference_rate) * np.log(current_rate / reference_rate)))


def categorical_total_variation(reference: pd.Series, current: pd.Series) -> float:
    ref = reference.fillna("__NULL__").astype(str).value_counts(normalize=True)
    cur = current.fillna("__NULL__").astype(str).value_counts(normalize=True)
    categories = ref.index.union(cur.index)
    difference = ref.reindex(categories, fill_value=0) - cur.reindex(categories, fill_value=0)
    return float(0.5 * difference.abs().sum())


def standardized_mean_shift(reference: pd.Series, current: pd.Series) -> float:
    ref = pd.to_numeric(reference, errors="coerce").dropna()
    cur = pd.to_numeric(current, errors="coerce").dropna()
    if ref.empty or cur.empty:
        raise ValueError("Mean shift requires non-empty numeric values")
    scale = float(ref.std(ddof=0))
    difference = abs(float(cur.mean()) - float(ref.mean()))
    return difference / scale if scale else (0.0 if difference == 0 else float("inf"))


def relative_std_shift(reference: pd.Series, current: pd.Series) -> float:
    ref = pd.to_numeric(reference, errors="coerce").dropna()
    cur = pd.to_numeric(current, errors="coerce").dropna()
    if ref.empty or cur.empty:
        raise ValueError("Standard-deviation shift requires non-empty numeric values")
    reference_std = float(ref.std(ddof=0))
    difference = abs(float(cur.std(ddof=0)) - reference_std)
    return (
        difference / reference_std if reference_std else (0.0 if difference == 0 else float("inf"))
    )


def build_drift_metric(
    *,
    feature_name: str,
    reference: pd.Series,
    current: pd.Series,
    reference_period: date,
    current_period: date,
    measured_at: datetime,
    metric: str,
    threshold: float,
) -> DriftMetric:
    if metric == "PSI":
        value = population_stability_index(reference, current)
    elif metric == "STANDARDIZED_MEAN_SHIFT":
        value = standardized_mean_shift(reference, current)
    elif metric == "RELATIVE_STD_SHIFT":
        value = relative_std_shift(reference, current)
    elif metric == "TOTAL_VARIATION":
        value = categorical_total_variation(reference, current)
    else:
        raise ValueError(f"Unsupported drift metric: {metric}")
    return DriftMetric(
        feature_name=feature_name,
        reference_period=reference_period,
        current_period=current_period,
        metric=metric,
        metric_value=value,
        threshold=threshold,
        status="ALERT" if value >= threshold else "OK",
        measured_at=measured_at,
    )
