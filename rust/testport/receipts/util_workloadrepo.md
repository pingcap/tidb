# `pkg/util/workloadrepo` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains `BUILD.bazel`, seven production files
(`const.go`, `housekeeper.go`, `sampling.go`, `snapshot.go`, `table.go`,
`utils.go`, and `worker.go`), and `worker_test.go`. It has no `doc.go`, package
fixture, generated source, benchmark, fuzz target, example, platform variant,
or build-tagged production variant. The checkout package is byte-identical to
the pin.

`worker_test.go` supplies the package's fifteen workload-repository tests. Its
SQL-variable integration case belongs to the ordinary session/sysvar boundary
in Rust; the remaining fourteen package tests live in `tidb-workloadrepo` and
exercise the same worker authority. `BUILD.bazel` maps to the explicit Cargo
workspace member and its ordinary cross-crate consumers.

## Rust ownership and integration

`rust/crates/tidb-workloadrepo` owns the repository worker, sampling,
snapshotting, partition housekeeping, table definitions, timers, owner
campaign, worker control, and status. It uses the completed `tidb-owner`
package and canonical `tidb-pd-client` etcd operations. Repository tables are
created and maintained through the cluster DDL executor, and sampling and
snapshot SQL run through ordinary sessions rather than a package-private SQL
engine.

The ordinary planner and executor now carry catalog source tables through
`LogicalMemTable`, `PhysicalMemTable`, and the common physical-plan builder.
That path serves the workload repository's Go dependencies rather than a
workload-specific runner: index usage, statement summary, client error
summary, process list, transaction, memory usage, deadlock, and data lock wait
providers all use their live shared owners and Go visibility rules.

`TIDB_INDEX_USAGE` reads the node-global seven-bucket collector and catalog
indexes, including Go's synthetic integer-primary-key index ID zero.
`TIDB_STATEMENTS_STATS` reads the cumulative statement-summary authority.
Client error tables and `FLUSH CLIENT_ERRORS_SUMMARY` share the packet-layer
error/warning counters; both text and prepared responses record warning codes
at the wire boundary. `TIDB_TRX` reads live process transactions, including
the native mutation-buffer footprint, state/waiting time, related table IDs,
and capped statement-digest history, while current SQL text is resolved from
the global statement summary as in Go. `DATA_LOCK_WAITS` queries every PD
store with TiKV's `GetLockWaitInfo`, tolerates individual store failures,
appends the node's resolving-lock registry, and decodes key and digest
metadata through the ordinary information-schema path.

The source's five-attempt snapshot retry, source-ordered error aggregation,
missing-key recovery, worker-control serialization, owner-loss handling,
unarmed housekeeper timer, retention changes, partition maintenance, manual
snapshot initiation, stop/restart, two-worker election handoff, and snapshot
ID recovery are retained. Rust-only constant/vector documentary tests and
snapshot edge tests were removed.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/workloadrepo` — passed.
- `GOTOOLCHAIN=go1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/workloadrepo -count=1` — passed in 111.688 seconds; the wrapper enabled and restored failpoints.
- `cargo test -p tidb-workloadrepo --lib -- --test-threads=1` — passed; fourteen tests.
- `cargo test -p tidb-session --lib tests_system_schemas` — passed; fourteen tests.
- `cargo test -p tidb-session --lib data_lock_waits_reads_the_storage_provider_with_go_privilege_and_encoding` — passed.
- `cargo test -p tidb-session --lib tidb_trx_reads_live_transactions_with_go_visibility_and_digest_history` — passed.
- `cargo test -p tidb-server --lib error_packets_apply_configured_suffixes_and_preserve_raw_bytes` — passed.
- `cargo test -p tidb-exec --test all cluster_ddl_source` — passed; 56 tests.
- `cargo check -p tidb-txnkv -p tidb-session -p tidb-server` — passed.
- `git diff --check` — passed.

No Go source, Go test, Bazel metadata, or Go module file changed, so the
`tidb-bazel-prepare-gate` determined that `make bazel_prepare` is not required.

The `tidb-txnkv` aggregate integration target cannot currently compile because
unrelated existing tests require a `BatchScheduler` completion type annotation
and refer to the absent `BatchCommandTag::ALL`; the production crate compiles
and the workload-repository session/server paths above pass. An initial direct
test-target invocation was corrected to the repository's generated aggregate
`all` target.

## Risk

- Correctness: improved; repository control, DDL, information-schema reads,
  packet counters, transaction state, and lock waits now use their ordinary
  shared production owners instead of constants, no-ops, or special runners.
- Compatibility: SQL table schemas, visibility, error handling, timers, etcd
  state, and TiKV lock-wait RPC behavior follow the pinned Go package and its
  consumed server interfaces.
- Performance: information-schema providers read live bounded snapshots;
  lock waits make one concurrent-capable RPC per discovered store with Go's
  30-second request bound. No Rust-only crossover or cache policy was added.
- Not verified locally: the blocked pre-existing `tidb-txnkv` aggregate test
  binary and a live external multi-store TiKV cluster. The canonical RPC and
  all production consumers compile; deterministic session/provider tests cover
  encoding, visibility, and merged resolving-lock behavior.
