# `pkg/util/tikvutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `tikvutil.go` and
`BUILD.bazel`. There is no package doc, test, test harness, benchmark, fixture,
generated input/output, platform variant, README, or ownership file. The local
Go package is byte-identical to the pin.

Production behavior is one public sequentially consistent atomic signed
32-bit integer, `CommitterConcurrency`, initialized to 128. The pinned tree has
three consumers: config loads it into TiKV client config, and the committer
concurrency sysvar stores and loads it.

## Rust ownership and audit result

`rust/crates/tidb-tikvutil/src/lib.rs` owns the atomic. The audit replaced a
private atomic plus Rust-only public default/getter/setter APIs with the single
public atomic object Go exposes. Config and sysvar consumers now load and store
that owner directly with sequential consistency. The standalone Rust-only unit
test was removed; the Go-derived config and sysvar consumer tests remain.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/tikvutil` — passed.
- `go test ./pkg/util/tikvutil -count=1` — passed (`[no test files]`).
- `cargo check --offline --locked -p tidb-tikvutil -p tidb-config -p tidb-session` — passed with existing warnings in dependencies and consumer crates.
- `cargo test --offline --locked -p tidb-config config_tree::config::tests::test_get_tikv_config_uses_the_runtime_committer_concurrency --lib -- --exact` — passed.
- `cargo test --offline --locked -p tidb-session tests_global_vars::committer_concurrency_updates_the_process_authority --lib -- --exact` — passed.
- `rustfmt --edition 2021 --check crates/tidb-tikvutil/src/lib.rs crates/tidb-config/src/config_tree/config.rs crates/tidb-session/src/vars.rs crates/tidb-session/src/tests_global_vars.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the value, width, initialization, atomicity, and memory ordering
  are unchanged; ownership now matches Go.
- Compatibility: removes Rust-only helper APIs in favor of the source-shaped
  public atomic.
- Performance: removes wrapper calls; atomic ordering remains sequentially
  consistent.
