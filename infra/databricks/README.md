# Existing Spark Gold job on Databricks

Local Gold tests and real Delta output were verified before this execution
adapter was added. Business logic remains in `dsp_spark`; the adapter invokes the existing social activity or court supply job with the managed session.
No notebook copy, new cluster definition, warehouse or Unity Catalog names are
introduced. Remote execution and cloud S3 have not been verified in this
workspace: no workspace/authentication, cluster, remote wheel or cloud target
configuration was available.

## Package and runtime

```sh
make databricks-build-wheel
```

The ignored output is `.local/databricks/dist/dsp391_spark_jobs-0.2.0-py3-none-any.whl`.
Poetry builds a standard wheel with:

- Existing `dsp_spark` modules, including the same Gold transform/job.
- The small `dsp_databricks` managed-session adapter.
- Authoritative JSON contracts and the versioned location catalog copied byte-for-byte as `dsp_contracts` resources.
- Only the collector configuration module needed for the shared topic constant.
- `jsonschema` as a Python dependency. Managed compute supplies Spark and Delta;
  the wheel does not install the local PySpark/Delta distributions or connectors.

Choose an existing cluster with Python 3.12, Spark 4.0.x, compatible managed Delta,
and access to the explicitly configured S3A data paths. The adapter rejects
other Spark major/minor versions. It gets the managed SparkSession, sets the
session timezone to UTC, calls the same Gold job, and leaves the session running.
Managed S3 authorization belongs in cluster/role configuration; no credentials
are passed in job arguments. Compatibility is prepared, not remotely proven.

Upload the built wheel to a supported Workspace file, Volume or S3 location
using your configured deployment tooling. This repository does not invent an
upload destination or upload automatically. Set `DSP_DATABRICKS_WHEEL_URI` to
that exact remote `.whl` path. Do not use deprecated DBFS root library storage.
Bump the distribution version for subsequent deployments so library caches do
not reuse a different wheel under the same version.

## Explicit configuration

| Variable | Purpose |
| --- | --- |
| `DATABRICKS_CLUSTER_ID` | Existing user-selected compute |
| `DSP_DATABRICKS_WHEEL_URI` | Already-uploaded wheel in `/Workspace/`, `/Volumes/`, or `s3://` |
| `DSP_LAKEHOUSE_BASE_URI` | Explicit `s3a://` base with existing Silver |
| `DSP_DELTA_SOCIAL_POSTS_PATH` | Optional Silver override |
| `DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH` | Optional Gold override |
| `DATABRICKS_CONFIG_PROFILE` | Optional CLI OAuth/profile authentication |
| `DATABRICKS_HOST` | Workspace host for CLI environment authentication |
| `DATABRICKS_TOKEN` | Optional CLI environment token; never printed by repository code |

No default workspace, bucket, cluster, upload path or credentials are supplied.
Missing settings fail before submission. Table paths derive from the same
`LakehousePaths` configuration as local execution. Gold and Silver must be
separate explicit S3A paths.

## Validate and submit

After setting the required variables:

```sh
make databricks-validate
```

This validates configuration and renders
`infra/databricks/jobs/social_activity_daily.json` locally. It requires neither
credentials nor the CLI and makes no remote request. It cannot verify remote
permissions, runtime compatibility or artifact existence.

For actual submission, install/configure the Databricks CLI 0.205+ and supply
environment authentication or a profile:

```sh
make databricks-submit-social-gold
```

The shell wrapper invokes the Python renderer, then `databricks jobs submit`
with an exact JSON body file and `--no-wait`. It submits one run on the specified
existing cluster; it does not create a persistent job or new cluster. A successful
submission response means a run request was accepted, not that Gold completed.
Inspect the returned run ID and logs. Existing stopped compute may be started
by Databricks when a run is submitted.

Reference: [Python wheel tasks](https://docs.databricks.com/aws/en/jobs/tasks/python-wheel),
[library locations](https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies),
and [CLI jobs commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/jobs-commands).

## Court supply entrypoint

The same wheel and managed-session adapter expose `court_supply_by_location`.
Its template is `infra/databricks/jobs/court_supply_by_location.json`.
`make databricks-validate-court-supply` renders it locally. Optional overrides:
`DSP_DELTA_COURTS_PATH`, `DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH`, and
`DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH`; all must be distinct S3A targets.
The adapter calls the existing supply job, including its quality output, without
copying transformations or replacing/stopping the managed session.
For an explicitly configured submission use
`sh scripts/submit_databricks_job.sh --job court_supply_by_location`.
No remote execution or AWS verification occurred during local preparation.
