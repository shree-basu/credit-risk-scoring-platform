# Model lifecycle and evidence boundary

## Scope

This repository implements a local-first logistic-regression candidate trainer, a Vertex-compatible
prediction container, explicit candidate/champion governance, lineage fields, and transparent drift
metrics. All sample data is synthetic. The repository is a production-pattern reference, not a
deployed lending decision system.

The score is decision support. The predictor emits probability of default and a transparent risk
band; it never emits an approve/decline decision. `loan_id`, `borrower_id`, and `age` are excluded
from the model feature allowlist. Fair-lending validation, explainability review, calibration on
representative data, human policy controls, and regulatory approval remain mandatory before any
real use.

## Training contract

- Training consumes an accepted feature snapshot produced by the governed Spark pipeline.
- The allowlist in `credit_risk/feature_contract.py` is shared with Spark and is the sole model input.
- The split and estimator use a recorded random seed. Metrics include ROC AUC, average precision,
  threshold precision/recall, and Brier score.
- Passing minimum metrics produces `CANDIDATE`; failing metrics produces `REJECTED`. Training can
  never produce `ACTIVE`.
- The trainer writes `model.joblib`, `metadata.json`, and `metrics.json`. It honors Vertex custom
  training's `AIP_MODEL_DIR`; a `gs://` write additionally requires the literal `GCS` confirmation.

## Registration and promotion

Model Registry upload is separate from training and requires the literal `VERTEX` confirmation.
When an existing parent model is supplied, the uploaded version receives the `candidate` alias and
is not made the default version. Promotion to the internal `ACTIVE`/champion assignment requires the
literal `PROMOTE`, an eligible candidate, an actor, and an effective timestamp. No automatic
promotion path exists.

Aliases are mutable pointers, while immutable model/version and feature-version fields are retained
in scoring and audit records. Historical backfills must resolve the model assignment effective for
their score date rather than silently using today's champion; orchestration for that rule is Day 4.

## Prediction contract

The Flask app implements configurable Vertex health and predict routes and listens on port 8080 in
the container. At container startup it reads local artifacts or downloads the exact artifacts from
Vertex's `AIP_STORAGE_URI`. It expects a non-empty `instances` list, rejects missing allowlisted features, and
returns `loan_id`, `borrower_id`, probability, risk band, model version, and feature version.

This design is for Vertex batch prediction with BigQuery input/output. It does not provision or
claim an online endpoint. BigQuery score publication is replay-safe on
`(loan_id, score_date, model_version)` and retains the pipeline run id.

## Drift

Numeric features and output scores use PSI with reference-derived quantile bins. Numeric features
also support standardized mean shift and relative standard-deviation shift; categorical features
use total variation distance. Thresholds and measurement timestamps are explicit inputs;
results are persisted with reference/current periods and `OK` or `ALERT` status. An alert is a review
signal, not an automatic retraining or promotion trigger.

## Cloud and cost safety

Day 3 tests train and serve locally and construct no GCP client. They do not authenticate, upload
artifacts, register models, run BigQuery, create endpoints, or submit Vertex jobs. Container files
and SQL definitions are inert until an operator separately builds/pushes an image or executes cloud
commands. No such operation was performed as evidence for this repository.

The Spark suite is defined for Python 3.11, matching the documented managed runtime. It passed during
Day 2 validation. The Day 3 Windows host exposes only Python 3.12/3.13, so its attempted Spark rerun
could not start compatible Python workers; this is an environment limitation, not passing evidence.
