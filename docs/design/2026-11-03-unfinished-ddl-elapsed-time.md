# Unfinished DDL elapsed-time monitoring

Tracking issue: https://github.com/pingcap/tidb/issues/72628

`tidb_ddl_unfinished_job_max_elapsed_seconds{type="..."}` is a gauge published
by the DDL owner. It reports the maximum elapsed time, in seconds, among eligible
unfinished DDL jobs of each DDL type. It has no job ID, database, table, or SQL
labels. Existing deployment labels identify the cluster and keyspace.

## Eligibility and timing

Only new submissions opt in, and only when the cluster's global state is normal
and every registered TiDB advertises support for DDL elapsed-time metadata.
Missing capabilities or a failed discovery exclude the submission from this
metric without rejecting the DDL. Jobs submitted by old versions or during an
upgrade remain excluded after the upgrade, after resume, and after owner changes.
Their historical pause intervals cannot be reconstructed reliably.

Elapsed time starts at the existing job submission timestamp, `StartTS`. It
includes queueing, execution, retries, cancellation/rollback processing, and final
schema synchronization. A `done` job waiting for synchronization is still
monitored. Synced, cancelled, and rollback-done jobs are excluded.

Jobs in `paused` state are excluded. Jobs in `pausing` state remain monitored
until they actually become paused. A monitored job persists the start of each
pause and accumulates the paused duration when it resumes or is cancelled. All
paused intervals are deducted; resume does not reset the pre-pause elapsed time.
These fields are committed with job state, so transaction retries and owner
changes do not lose or double-count them. Jobs already opted in retain their
pause accounting during subsequent upgrades.

The owner reads active jobs in snapshot-consistent pages every 10 seconds,
independently of scheduler and worker progress. It publishes only complete,
successful scans. Failed scans retain the last values and log a warning. When no
eligible job remains for a type, its series is removed on the next successful
scan. The retiring owner clears its series.

## Alerting

For a 10-minute threshold, an example PromQL expression using the labels in TiDB
Grafana dashboards is:

```promql
max by (k8s_cluster, tidb_cluster, keyspace_name, type) (
  tidb_ddl_unfinished_job_max_elapsed_seconds
) > 10 * 60
```

Adapt cluster/keyspace labels to the deployment. Use `max` across instances so
owner changes do not add durations together. Configure the threshold in the
monitoring rule; no TiDB system variable is required. Detection and resolution
are subject to the 10-second collection period, Prometheus scrape interval, rule
evaluation interval, and any configured alert `for` duration.

Inspect `ADMIN SHOW DDL JOBS` to locate affected jobs after an alert. Since the
metric aggregates by DDL type, it does not identify an individual job.
