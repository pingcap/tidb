# `pkg/util/paging` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `paging.go`,
`paging_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package doc,
README, fixture, generated or platform variant, benchmark, or ownership file.
The local Go package is unchanged from the pin.

Production behavior consists of four constants, two private policy constants,
the paging-growth cap calculation, and the piecewise seek-count calculation.
The source suite has exactly `TestGrowPagingSize` and `TestCalculateSeekCnt`;
`TestMain` only installs the repository's common Go test environment and leak
check.

## Rust ownership and audit result

`rust/crates/tidb-util/src/paging.rs` owns the complete production package and
the two source test translations. The formulas preserve Go `uint64` wrapping
where Rust debug arithmetic would otherwise panic.

The audit removed three supplemental boundary/overflow tests and the duplicate
copies of both Go tests from `tidb-distsql`. It also removed Rust-only
`must_use` diagnostics from the two functions. The independent DistSQL paging
configuration-default test remains with its consumer package and is not part
of this package claim.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/paging` — passed.
- `cargo test -q -p tidb-util paging::tests --lib --locked --
  --test-threads=1` — passed; exactly two tests ran.
- `cargo test -q -p tidb-distsql --test all
  paging_source::paging_config_defaults_consume_the_policy_authority --locked
  -- --test-threads=1` — passed; exactly one consumer-owned test ran.
- `cargo check -p tidb-util -p tidb-distsql --all-targets --locked`,
  `cargo fmt --all --check`, and `git diff --check` — passed. The all-target
  check emitted only pre-existing warnings outside the changed files.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production calculations are unchanged; the exact source tests
  remain authoritative.
- Compatibility: only compiler diagnostics and redundant test coverage are
  removed.
- Performance: unchanged.
