# `pkg/statistics/handle/types` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `9d9f114c895299ceddddb54287b1836f4142c2b5` | `0d278a37052b6cb281e3ed696e306a4debeaeb4c91b7fe3cc90f835a9017b267` | build metadata inventoried; no generated target |
| `interfaces.go` | 537 | `6ad61fdcb4c10cc9609c951f0e6a5016578f71d6` | `f7b32771914b985d226d623881092a83c532500bdfd8ab36ea71157eec9821ea` | every declaration inventoried below; package closure remains unclaimed |

The package has no generated, platform-specific, test, benchmark, fixture, or
other support artifacts.

## Package behavior and blockers

This package is the shared contract for the entire ordinary statistics
handle. It owns the GC, usage, history, analyze, cache, lock, read/write,
synchronous-load, global-statistics, and DDL interfaces plus their shared
request/result data structures, then composes them into `StatsHandle`. The
interfaces use common info-schema, session context, statement context,
statistics graph, storage JSON, notifier, pool, and SQL-executor types.

Rust maps this Go contract package across the crates that own each concrete
operation. A standalone umbrella crate would add a second, unused interface
hierarchy rather than Go-observable behavior. The package nevertheless remains
explicitly unclaimed: several embedded implementation packages are themselves
unclaimed, so the composite `StatsHandle` contract cannot yet be proven over a
dependency-closed implementation.

## Complete declaration map

| Go declaration | Rust owner | Audit result |
| --- | --- | --- |
| `ColStatsTimeInfo` | `tidb-exec::cluster_predicate_column::ColumnStatsTimeInfo` | exact optional timestamp payload; the consuming predicatecolumn package owns conversion behavior |
| `PriorityQueueSnapshot` | `tidb-stats-handle-autoanalyze-priorityqueue` | exact two JSON arrays; jobs sort by descending weight and retry IDs retain unspecified map/set order |
| `AnalysisJobJSON`, `IndicatorsJSON` | `tidb-stats-handle-autoanalyze-priorityqueue` | exact field order/names and formatted strings; integer map keys and floating-point JSON now follow Go `encoding/json` |
| `CacheUpdate`, `UpdateOptions` | `tidb-stats-handle-cache` | exact nested update/options shape; quota mode observes `SkipMoveForward`, map mode ignores it like Go |
| `StatsLockTable` | `tidb-stats::stats_lock_table` | exact full name plus nil-versus-allocated partition map payload |
| `PartitionStatisticLoadTask`, `PersistFunc`, `MetaUpdate` | storage/load owners in `tidb-exec` | represented at the native task/callback/write boundaries; package behavior remains gated by the unclaimed storage package |
| `NeededItemTask` | private `tidb-executor::driver::catalog::sync_load::NeededItemTask` | exact deadline, item, retry, and result-delivery role; loader/cache ownership are native equivalents of the receiver-bound Go handle |
| `GlobalStatsInfo` | `tidb-stats::global_stats` plus `tidb-exec::real_tikv_analyze` | fields pass directly to item merge/commit boundaries; globalstats package closure remains open |

| Go interface | Rust owner(s) | Closure status |
| --- | --- | --- |
| `StatsGC` | statistics storage/GC paths | blocked on complete `pkg/statistics/handle/storage` |
| `StatsUsage`, `IndexUsage` | usage collector, predicatecolumn, indexusage, and execution seams | complete root and child-package receipts own the behavior |
| `StatsHistory` | `tidb-domain::historical_stats` and storage/execution seams | blocked on complete storage runtime |
| `StatsAnalyze` | priorityqueue, analyze execution, and domain worker owners | complete root/exec/refresher/priorityqueue receipts own the interface behavior |
| `StatsCache` | `tidb-stats-handle-cache` and shared `stats_watch` publication | complete root cache receipt; its separately pinned LFU dependency remains unclaimed |
| `StatsLock` | `tidb-stats::lock_stats` and executor/session/server seams | exact operations are covered by the lockstats receipt |
| `StatsReadWriter` | cluster stats read/write, LOAD STATS, history, and analyze storage seams | blocked on complete storage package closure |
| `StatsSyncLoad` | `tidb-executor::driver::catalog::sync_load` and catalog lifecycle | complete syncload receipt owns the behavior |
| `StatsGlobal` | `tidb-stats::global_stats` and analyze commit seams | blocked on complete globalstats package closure |
| `DDL` | statistics DDL notifier/execution seams | blocked on complete handle/ddl package closure |
| `StatsHandle` | composed domain/catalog/session statistics owners | cannot be claimed before all embedded owners above are complete |

## Removed non-parity surface

The one concrete shared payload currently consumed by Rust lock execution is
`StatsLockTable`. Its fields directly preserve Go's full name and nil-versus-
allocated partition map distinction, so callers retain it and now construct
it as the plain struct Go defines. Rust's public `new` convenience API and two
source-absent tests were removed; neither exists in the pinned package.

The cache payload had also flattened `CacheUpdate.Options.SkipMoveForward` into
a Rust-only `skip_move_forward` field and documented the type as reduced. The
existing cache can express Go's structure directly, so `UpdateOptions` is now
restored and all production and benchmark construction sites use the nested
field.

## Corrected Go JSON contract

Go's `AnalysisJobJSON` is serialized by `encoding/json`. Deriving Serde over a
Rust `HashMap<i64, _>` emitted keys in randomized iteration order, while Go
sorts integer map keys by their decimal text. Serde also emitted integral
weights as `1.0` and non-finite values as `null`; Go emits `1` and rejects
NaN/infinities. The Rust field serializers now use Go's float cutovers
(`1e-6`, `1e21`), exponent spelling, non-finite rejection, declaration order,
and decimal-text key ordering. Empty vectors/maps remain `[]`/`{}`, matching
the queue's non-nil Go allocations.

## Validation

WIP profile: the contract corrections are checked through their affected
owners. Fail-before evidence for the JSON regression first observed
`partition_index_ids` as `{"2":[22],"10":[101]}`, then, after key ordering was
fixed, the exact-object assertion observed `"weight":1.0` instead of Go's
`"weight":1`.

- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-priorityqueue tests::source_sql_and_json_shapes -- --exact --nocapture` (failed before each JSON correction, passes after)
- `cargo check --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache -p tidb-exec`
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`

Integrated Ready profile, all passed. The first sandboxed lint invocation could
not resolve `proxy.golang.org`; the identical approved-network rerun passed.

- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-priorityqueue`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache`
- `cargo test --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all analyze_commit_size_source::predicate_column_ -- --nocapture`
- `cargo check --locked --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-priorityqueue -p tidb-stats-handle-cache -p tidb-exec`
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `make lint`
