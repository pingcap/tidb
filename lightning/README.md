# TiDB-Lightning

**TiDB-Lightning** is a tool used for importing data at TB scale to TiDB clusters.

## Documentation

[中文文档](https://docs.pingcap.com/zh/tidb/stable/tidb-lightning-overview)

[English Document](https://docs.pingcap.com/tidb/stable/tidb-lightning-overview)

[Import Into Statement](https://docs.pingcap.com/tidbcloud/sql-statement-import-into) is used to import data files or result from select statement into an empty table in TiDB via the Physical Import Mode of TiDB Lightning.

## Building

To build binary:

```bash
$ cd ../tidb
$ make build_lightning
```

Notice TiDB-Lightning supports building with Go version `Go >= 1.16`

When TiDB-Lightning is built successfully, you can find binary in the `bin` directory.

## Running tests

See [this document](../lightning/tests/README.md) for how to run integration tests.

## Quick start
See [Quick Start for TiDB Lightning](https://docs.pingcap.com/tidb/stable/get-started-with-tidb-lightning).

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](../CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiDB-Lightning is under the Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.


## Import result summary

Lightning emits a structured `Import Summary` for each table and for the whole logical task on success, failure, and cancellation. Checkpoint recovery retains earlier confirmed outcomes. The summary describes source records; a later replacement does not subtract an earlier confirmed insertion from `imported-records`.

| Field | Meaning |
| --- | --- |
| `total-records` | Distinct source records actually read, including identifiable conversion failures. Headers, schema statements, sampling, duplicate prechecks, and checkpoint replay are excluded. |
| `imported-records` | Confirmed ordinary target insertions. |
| `replaced-records` | Confirmed replacement inputs, counted once per source even when several target rows or unique indexes conflict. |
| `skipped-records` | Inputs deliberately omitted by the conflict policy. |
| `error-records` | Inputs definitively rejected by conversion or writing. A resumed successful retry moves the source to its new category. |
| `unresolved-records` | Read inputs whose final category cannot be confirmed. |
| `statistics-complete` | True when the total is known and no source remains unresolved. |
| `status` | `Success` or `Failed`, preserving the existing task outcome. |

Whenever the total is known:

```text
total-records = imported-records + replaced-records + skipped-records + error-records + unresolved-records
```

Conflict diagnostics remain in their existing logs; they overlap skipped/replaced outcomes and are not an additional category. A post-import ANALYZE failure can produce `status=Failed` while all sources have confirmed successful outcomes.

The `local` and `tidb` backends retain their existing supported conflict strategies. Local sorted-file writes become confirmed outcomes only after target data/index ingestion and applicable conflict handling. A parallel-import task that delegates remote conflict checks to another task leaves its own unproven rows unresolved. Local pre-deduplication counts omitted inputs as skipped and retained replacement inputs as replaced. If remote conflicts cannot be mapped back to source inputs, conflict detection is disabled, or winner metadata is unavailable, unprovable outcomes remain unresolved. No extra source scan or target `COUNT(*)` is performed for statistics.

The TiDB backend reads optional versioned `dml_result` telemetry from `@@tidb_last_query_info` on the same physical connection as the preceding DML. Prepared statements are cached per writer connection. Older servers, unsupported statement features, and diagnostic read failures leave those inputs unresolved. Statistics queries never retry a successful target write. A commit whose acknowledgement is lost remains unresolved even if replay succeeds.

IMPORT INTO exposes optional versioned JSON through `information_schema.PKDB_IMPORT_JOB_STATISTICS(JOB_ID, STATISTICS)`. Visibility follows `SHOW IMPORT JOB`: users see their own jobs, and `IMPORT_JOB_VIEW` grants broader visibility. Existing SHOW columns and messages are unchanged. Successful subtask attempts contribute actual source reads; scheduler retries recompute absolute results. Capped conflict enumeration preserves uncertainty instead of counting index keys as source rows.

File checkpoints reference an optional versioned ledger in a `.pkdb-import-statistics/<generation>/` directory next to the checkpoint. Source-batch fences and acknowledgements persist only changed chunks in atomic journal records; periodic snapshots compact the journal. Keep this directory with an active checkpoint when copying it. On normal checkpoint backup, the ledger is embedded into the `.bak` file so that the backup is self-contained. MySQL checkpoints use `pkdb_import_statistics_v1` in the checkpoint schema, updating only changed source chunks. A durable pre-write fence separates confirmed outcomes from ambiguous replay. Older checkpoints and an unclean exit with an unpersisted read-ahead tail can lack a historical total; `total-records` and `unresolved-records` are then `null`, confirmed outcomes remain available, and `statistics-complete=false`. Write roles observed during replay of incomplete process history stay unknown, including when a failed pre-write statistics fence could not be persisted. Statistics storage failures during imports warn and preserve the original import/checkpoint success decisions. Explicit per-table checkpoint removal or error destruction also discards that table's statistics and source frontiers, retaining other tables. If this maintenance reset cannot update statistics coherently, it fails instead of reporting a checkpoint-only reset; older checkpoints without statistics remain supported. Deploy the server before Lightning for precise SQL telemetry; older targets retain partial summaries. This release does not include the newer Lightning `import-into` backend or import SDK; SQL IMPORT INTO statistics are exposed directly by the server.
