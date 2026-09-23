# Statement and command duration metrics

`tidb_session_statement_duration_seconds` measures completed, non-restricted SQL
statement executions. It is a Prometheus histogram with `sql_type` (for example,
`Select`, `Insert`, or `Update`) and `resource_group` labels. It does not retain SQL
text or digests, and does not depend on statement-summary retention or slow-log
thresholds.

## Timing and accounting

The observation uses the same statement timer as slow log and statement summary:
`time.Since(SessionVars.StartTime) + SessionVars.DurationParse`. It includes
compilation, execution, lock waits, and statement finalization, including an
autocommit transaction's commit. A later explicit `COMMIT` is a separate statement;
client idle time between statements is not included. For result sets, the sample
is recorded when the result set is closed, not when execution first returns it;
pauses while a cursor/result set remains open can therefore contribute.
Execution errors that reach statement finalization are included. Parsing or
compilation failures that do not reach that hook are not included.

For a multi-statement `COM_QUERY`, each executed statement contributes its own
sample. Parsing is performed once for the entire request; its duration is included
in the first statement's sample, not divided among the statements. Request-level
work before the individual statement timer, such as multi-statement key prefetch,
is not included. For ordinary DML, writing the final OK packet occurs after the
statement sample is recorded.

Binary-protocol `COM_STMT_PREPARE` does not execute a statement and does not
contribute a sample. Each `COM_STMT_EXECUTE` is labeled with the underlying SQL
statement type, not `Execute`. Text-protocol SQL `PREPARE` is itself a statement
and can contribute a `Prepare` sample. Non-SQL commands such as `PING` and
`COM_STMT_CLOSE` do not contribute samples. Restricted internal SQL is excluded.
The metric follows execution finalization: retries inside an execution that do not
invoke this hook separately do not add samples, while separate re-executions that
reach finalization can each contribute a sample.

## Compatibility with request metrics

`tidb_server_handle_query_duration_seconds` retains its existing request-level
timing (including an entire multi-statement request), DB-label fanout, and DDL
exclusions. For a `COM_QUERY` that parses into more than one statement, its
`sql_type` label is `MultiStmt`, not the last statement's type. This also applies
when a parsed multi-statement request is rejected or stops on an execution error.
Single-statement requests and other protocol commands retain their existing label
behavior; a parse failure cannot be classified as multi-statement. The RPC and
processed-key histograms still describe the final statement context and are not
relabeled by this change.
Do not add the two histograms: their timed intervals overlap, and their counts have
different meanings.

For example, a request containing a two-second INSERT followed by a three-second
UPDATE contributes one sample to each statement type in the new histogram. The
existing server histogram receives an approximately five-second sample labeled
`sql_type="MultiStmt"` for each applicable DB label.
Adding statement histogram buckets cannot reconstruct a request-latency
distribution.

## Grafana dashboards

The TiDB, TiDB-KeyspaceName, and TiDB-Worker dashboards include a separate collapsed
**Statement** section. The existing Query Summary and Query Detail sections retain
their command/executor metrics and remain available for older TiDB versions.

The Statement section shows:

- Overall P999/P99/P95/P80 latency.
- Completed statement executions per second, by SQL type and in total.
- Average statement latency by SQL type, weighted by execution count.
- Accumulated statement wall time per second by SQL type (not CPU utilization;
  overlapping executions can accumulate more than one second per second).
- P999/P99/P95/P80 latency by SQL type and by instance.

The panels honor the existing cluster and instance selectors, and the keyspace
selector on TiDB-KeyspaceName. Resource groups are aggregated rather than creating
an instance/type/group cross product. Rates use a one-minute window, consistent
with the existing duration panels; low-volume percentiles, especially P999, can
be noisy and require enough scrapes.

There is deliberately no automatic fallback to the server query-duration metric.
A command histogram is not a statement histogram: a multi-statement command has
one request latency but several statement latencies, and non-SQL commands also
contribute to the server metric. Silently switching would change the meaning of
counts, averages, and percentiles. In a rolling upgrade, combining old command
samples with new statement samples would mix incompatible populations.

On old versions or instances that have not emitted this metric, the new panels
show no data. During a mixed-version rollout they cover only instances reporting
the new metric, not necessarily the whole selected cluster. Use the original
Query sections for request-level monitoring throughout the rollout. If a separate
compatibility view is added later, it must clearly identify its active timing
semantics and handle metric availability per instance, not just use an `or` after
cluster-level aggregation.

## PromQL examples

Average statement latency in seconds, grouped by SQL type (add instance/cluster
selectors appropriate for the deployment):

```promql
sum by (sql_type) (rate(tidb_session_statement_duration_seconds_sum[5m]))
/
sum by (sql_type) (rate(tidb_session_statement_duration_seconds_count[5m]))
```

P99 statement latency in seconds:

```promql
histogram_quantile(0.99,
  sum by (le, sql_type) (
    rate(tidb_session_statement_duration_seconds_bucket[5m])
  )
)
```

Include `resource_group` in the aggregation keys to retain that dimension. The
histogram uses the same bucket boundaries as the server query-duration histogram.
