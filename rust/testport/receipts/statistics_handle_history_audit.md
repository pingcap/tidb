# `pkg/statistics/handle/history` — complete package receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 22 | `0e7955cfddd870ea2be55f54976a424491d38cb7` | dependency and visibility ownership mapped |
| `history_stats.go` | 210 | `2b76d7e7a7aee981fdb78e09afa860b2d1aee3a3` | every production declaration and branch mapped below |

All 232 lines were read. The package has no `doc.go`, package-local test,
benchmark, fuzz target, example, fixture, generated input/output,
platform/build-tag variant, or other support artifact.

## Production behavior map

| Go behavior | Rust production owner | Receipt |
| --- | --- | --- |
| `NewStatsHistory` composition over the ordinary handle | `ClusterHistoricalStatsHandle` installed with the domain historical worker | one node-owned handle supplies the live cache, catalog, global variables, and transaction opener |
| `RecordHistoricalStatsToStorage` table/partition choice | `ClusterHistoricalStatsHandle::historical_json` and its `StatsHandle` implementation | a partition target dumps one physical table; an ordinary table dumps itself; a partitioned logical target dumps definition-order partitions plus optional `global` |
| nil JSON warning/no-op | the same handle returns `Ok(0)` when the physical storage dump is absent | no payload transaction is opened and the worker reports success, as Go does |
| `RecordHistoricalStatsMeta` target selection | ANALYZE, LOAD STATS, delta-flush, slow-save, and DDL production callers | version zero is skipped; enforced writes bypass cache filtering; non-enforced writes require a nonzero, resident, initialized physical table; the switch is read once before the selected writes |
| enable check | `ClusterHistoricalStatsHandle::check_historical_stats_enable` | reads the live global `tidb_enable_historical_stats` value and propagates lookup failure |
| standalone metadata writer | `plan_historical_stats_meta_lock` plus `plan_historical_stats_meta_replace` in one pessimistic transaction | selects the exact `(table_id, version)` row `FOR UPDATE`, rejects absent/invalid rows with Go's error, copies both counters, and replaces `(table_id, version, source, create_time)` |
| metadata best-effort loop | source-specific production coordinators | every selected table receives an independent wrapped transaction; one failure is logged/discarded without stopping later table IDs or failing the successful statistics write |
| `maxColumnSize = 5 << 20` and JSON framing | `historical_stats_data_blocks` and `json_table_to_blocks` | canonical JSON is gzip-compressed once and split into ordered five-megabyte blocks |
| payload version | `historical_stats_data_blocks` | uses the table version when `partitions` is absent/empty and otherwise the maximum nonnil partition/global version |
| payload persistence | `ClusterHistoricalStatsHandle::record_historical_stats_to_storage` and `plan_historical_stats_data_block` | every block is one insert/upsert statement inside one wrapped pessimistic transaction, keyed by `(table_id, version, seq_no)`, with one shared creation time; an error returns no version |

The package's dependencies are integrated through native Rust crate
boundaries rather than a second umbrella handle hierarchy. Historical reads,
fallback, and garbage collection belong to the separate pinned
`pkg/statistics/handle/storage` inventory and are not claimed by this receipt.

## Root parity correction

The pinned metadata SQL deliberately uses `NOW(6)`, with an explicit source
comment explaining that second precision makes same-second rows
indistinguishable. The payload writer likewise formats `time.Now()` with six
fractional digits. Rust previously supplied `fsp = 6` while hard-coding the
microsecond field to zero, and its production payload, ANALYZE, LOAD STATS,
delta-flush, and schema-change history paths all inherited that mismatch.

Rust now constructs the local `DATETIME(6)` wall clock used by both pinned
writers. A deterministic history-clock regression supplies the fixed fraction
`654321`; with the former hard-coded-zero conversion it fails (`left: 0`,
`right: 654321`) and with the production correction it passes. The separate
column-usage writer remains second-precision, matching its Go `TimeFormat`
contract.

## Removed non-parity carrier

The earlier `historical_stats_version` helper exposed only the local
version-selection expression over pre-extracted integers. It bypassed JSON
generation, partition entries, gzip framing, block ordering, timestamps,
transactions, partial failures, and SQL upsert behavior. The helper and its
three source-absent tests remain removed; the production paths above are the
only implementation.

## Validation

WIP evidence for the microsecond correction:

- fail-before: `cargo test --locked -p tidb-exec mysql_bootstrap::rows::tests::historical_timestamp_retains_six_fractional_digits -- --exact --nocapture` with the former zero-fraction conversion (`left: 0`, `right: 654321`)
- pass: `cargo test --locked -p tidb-exec mysql_bootstrap::rows::tests::historical_timestamp_retains_six_fractional_digits -- --exact --nocapture`
- `cargo test --locked -p tidb-exec --test all historical_stats -- --nocapture` (4 passed)
- `cargo test --locked -p tidb-server cluster_session_node::tests::unistore_cop::analyze_records_historical_stats_through_the_domain_worker -- --exact --nocapture` (1 passed with host access; the sandboxed run was blocked only by `sysctl hw.memsize`)

Ready profile:

- `cargo check --locked -p tidb-exec -p tidb-server`
- `cargo fmt --all -- --check`
- `git diff --check`
- `make lint` (passed with host network access after the sandbox could not
  resolve `proxy.golang.org` for the pinned `revive` tool)

No Go source, Go import section, Bazel file, module dependency, or top-level Go
test changed, so `make bazel_prepare` is not required.
