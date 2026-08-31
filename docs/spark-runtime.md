# Managed Spark runtime contract

This reference implementation targets the Managed Service for Apache Spark runtime
line `2.3`, which currently provides Apache Spark 3.5.3, Python 3.11, Java 17 and the
Spark BigQuery connector 0.42.3. The exact `2.3.x` sub-version is intentionally not
hardcoded because Google updates sub-versions within the supported runtime line.

The cloud batch configuration must request runtime `2.3`. It must not package a
separate connector JAR while that runtime supplies a compatible connector. Local tests
pin PySpark 3.5.3 to match the Spark engine; CI will run them on Python 3.11.

The feature job uses built-in Spark expressions, projects only documented source
columns and rejects duplicate natural keys before joins. Borrower profiles are small
enough in the synthetic reference workload to be candidates for a broadcast join, but
the code deliberately leaves the decision to Spark adaptive query execution until
real size statistics exist. `spark.sql.shuffle.partitions` is configurable. No scale,
cost or speed improvement is claimed without a measured cloud run.

The accepted and quarantine frames are persisted because each is both counted for
reconciliation and subsequently written. Callers release them after publication. The
implementation never collects a complete production dataset on the driver.

BigQuery writes land in a deterministic batch staging table with overwrite semantics.
An Airflow BigQuery task then runs the relevant `MERGE` template. The curated feature
key is `(loan_id, feature_date, batch_id, feature_version)`, so retrying the same
logical batch updates the same rows rather than appending duplicates. The connector
and BigQuery operations are implemented but have not been executed against GCP.

Age is retained only in the raw/profile contract for later synthetic audit analysis.
It is intentionally absent from `MODEL_FEATURE_ALLOWLIST` and from published model
features. Loan and borrower identifiers are lineage keys, not model inputs.
