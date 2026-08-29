# `pkg/util/sli` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full:

- `sli.go` — the transaction accumulator, validity and small-transaction
  rules, metric selection, reset, and string rendering;
- `BUILD.bazel` — one production library target and no package-local test.

There is no `doc.go`, README, test, fixture, benchmark, generated/platform
variant, ownership file, or additional harness. The checkout is byte-identical
to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sli.rs` is the package owner. It preserves Go's
zero-valued accumulator, signed `time.Duration` nanoseconds, native-width
`int` counters, wrapping accumulation, validity/small-transaction rules,
metric selection, exact metric metadata/buckets, reset, string rendering, and
`CheckTxnWriteThroughput` failpoint. Rust-only derives, accessors, observation
values, synthetic size fixtures, and supplementary package-local tests remain
removed.

The required integration is implemented through ordinary owners rather than a
separate SLI runner:

- `tidb-session` owns one accumulator beside its transaction and invalidates
  INSERT/REPLACE SELECT at execution;
- `tidb-exec` and cluster storage expose actual final encoded mutation
  bytes/keys and actual snapshot-processed keys; rows answered from the
  transaction's own buffer are not counted as TiKV processed keys;
- cluster and real-TiKV server sessions add those details only at successful
  statement/commit completion and only add processed keys for affected writes;
- the common MySQL text/prepared dispatch finalizes elapsed time, affected
  rows, and current transaction state for cached and fresh plans alike.

No affected-row byte estimate, backend threshold, or cache-specific execution
path was added.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `GOCACHE=/private/tmp/tidb-go-build-cache go test ./pkg/util/sli` — blocked
  before the package compiled by the workspace's existing
  `google.golang.org/grpc/internal/transport` / `http2.TrailerPrefix`
  dependency mismatch.
- `cargo test --offline --locked -q -p tidb-server --features failpoints
  --lib txn_write_throughput_sli_matches_source -- --nocapture` — passed the
  complete Go external source-test scenario and exact state strings.
- `cargo test --offline --locked -q -p tidb-executor --lib
  cluster_storage::tests` — passed.
- `cargo test --offline --locked -q -p tidb-exec --lib
  real_tikv_dml::tests` — passed.
- `cargo check --offline --locked -q -p tidb-session --lib` — passed.
- `cargo check --offline --locked -q -p tidb-server --lib --features
  failpoints` — passed.
- scoped `cargo fmt` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: exact encoded mutation and snapshot evidence replaces the
  previous missing production integration; the Go source regression pins all
  accumulator states, including failed-commit cleanup.
- Compatibility: the accumulator is internal session state and the added
  report fields are propagated through every existing Rust consumer.
- Performance: write detail collection walks the final mutation set already
  committed, and read-key collection remains disabled outside tracked write
  statements; metric initialization remains one-time.
