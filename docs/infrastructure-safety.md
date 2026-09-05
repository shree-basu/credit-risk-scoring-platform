# Infrastructure and cost safety

Terraform is intentionally a deployment blueprint with a zero-resource default. With the checked-in
defaults, all resource `for_each` collections are empty. Merely cloning the repository, running
tests, running `terraform fmt`, initializing the provider, or running `terraform validate` cannot
create a cloud resource.

Core resources require both `deployment_enabled=true` and the exact confirmation string `DEPLOY`.
Composer is deliberately more difficult to enable because it is a continuously running managed
service: it additionally requires `enable_composer=true` and the exact confirmation `COMPOSER`.
Closing either confirmation gate after Terraform has managed resources would propose deletion;
therefore operators must never treat a gate as a temporary switch. Persistent buckets, datasets,
tables, service accounts, Artifact Registry and Composer carry `prevent_destroy`; buckets use
`force_destroy=false`, datasets use `delete_contents_on_destroy=false`, and tables enable deletion
protection. Removing managed infrastructure requires an explicit reviewed decommission change that
temporarily changes those protections. Disabling APIs on destroy is also disabled.

No workflow in this repository authenticates to GCP or runs `terraform apply`. Infrastructure
changes are local-review material only. A future real deployment would require a separately approved
state backend, identity federation, budget/alert controls, explicit non-placeholder project and
globally unique bucket names, reviewed variables, and an operator-issued apply outside CI.

The IAM design uses separate Spark, Vertex and optional Composer service accounts. Project roles are
limited to job submission/runtime roles; bucket and dataset data permissions are resource-scoped.
Composer may impersonate only the two workload service accounts. Owner, primitive Editor and
service-account keys are intentionally absent. The Composer service agent receives its required
`roles/composer.ServiceAgentV2Ext` binding only on the optional environment service account.

Terraform models Managed Spark as ephemeral batches submitted by Airflow, not as an always-on
Dataproc cluster. Artifact Registry uses immutable tags. GCS public access prevention, uniform
bucket access, versioning and seven-day soft delete reduce accidental exposure or loss. The optional
Composer resource supplies the `AIRFLOW_VAR_*` values consumed by both DAGs.
