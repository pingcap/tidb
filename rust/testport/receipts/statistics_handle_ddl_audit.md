# `pkg/statistics/handle/ddl` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 48 | `34f701e7488bb6e0843b6609856a46e4c393d443` | build metadata inventoried |
| `ddl.go` | 171 | `520486c48817d5327b7df52f5c02e93c20dd7e30` | unclaimed: ordinary handle owner absent |
| `ddl_test.go` | 1621 | `dd3a930a2807b630d73be0e9490ad80ff9812863` | 24 integration tests inventoried; not ported |
| `subscriber.go` | 682 | `d2d3406ff3358b45d2863950e99bfffd71f86523` | unclaimed: runtime dependencies absent |

The package has no generated, platform-specific, benchmark, fixture, or other
support artifacts.

## Package behavior and blockers

The package owns a buffered notifier-event channel and a subscriber connected
to the ordinary statistics handle. It decodes every supported schema-change
event, reads global prune and historical-stats variables, resolves schemas,
and performs ordered writes against the real statistics storage. Table and
column creation initializes statistics and conditionally records historical
metadata. Truncate, drop, partition reorganization, partition exchange,
partitioning conversion, flashback, and schema drop update versions, counts,
modify counts, global IDs, lock-aware deltas, and delayed deletion with the
source error and best-effort semantics. The handler also suppresses subscriber
errors after the source's test-only classification.

The 24 Go tests drive these paths through a mock TiDB domain, DDL notifier,
transactional system sessions, infoschema, statistics cache, storage tables,
ANALYZE, partition pruning, historical metadata, and lock-aware updates. Rust
does not yet have the dependency-closed ordinary statistics-handle owner, and
the required `handle/types`, `storage`, `history`, and `lockstats` packages are
not complete. A decoded event enum plus caller-implemented traits cannot
provide this package's observable integration behavior.

## Removed non-parity carriers

The `ddl_subscriber` module replaced Go's notifier, sessions, storage,
infoschema, cache, and logging with recording ports and mock-effect tests. Its
`ddl_physical_ids` and `ddl_stats_delta` siblings exposed two extracted helper
APIs rather than the atomic package. They had no production consumer. All
three modules and their tests were removed, together with an ignored empty
handletest function that referred to the delta leaf.

The source-absent `ddl_queue_gate` compatibility module and its duplicate tests
were also removed. A later whole-package audit established that the
caller-injected `auto_analyze_runtime` was itself not the live Go
`priorityqueue` owner and removed it too. The integrated auto-analyze DDL
behavior remains unclaimed pending the ordinary handle/session dependencies.

The root DDL package remains explicitly unclaimed until it can be wired through
the ordinary dependency-complete handle path and validated with the complete
source test surface.

## Validation

WIP profile: removal of disconnected carriers is checked through the affected
statistics crate.

- `cargo check --locked -p tidb-stats`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- changed-file `rustfmt --edition 2021 --check`
- `git diff --check`
