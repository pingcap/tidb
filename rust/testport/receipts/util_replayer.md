# `pkg/util/replayer` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `replayer.go` and
`BUILD.bazel`. There is no package doc, test, test harness, benchmark, fixture,
generated input/output, platform variant, README, or ownership file. The local
Go package is byte-identical to the pin.

Production behavior consists of the comparable two-digest task key, creation
of a writer under the fixed `replayer` directory, context-bound write and close
forwarding, random padded URL-base64 filename generation with its three prefix
rules, and the public path plus one-time initialization globals.

## Rust ownership and audit result

`rust/crates/tidb-domain/src/replayer.rs` is the sole owner. Its `Storage` and
`ObjectWriter` traits are native boundaries for the two Go interfaces used by
the package; the captured Go context is carried by the boundary implementation
rather than exposed as a second Rust API. `RwLock<String>` preserves safe Rust
mutation of Go's public package string, and `Once` maps directly to `sync.Once`.

The audit removed `PlanReplayerTaskKey` from the domain consumer, including the
Rust-only ordering traits and convenience constructor. Domain and session
callers now import the canonical package owner. The two existing source-derived
`TestDumpGCFileParseTime` ports now call the real generator for all eight flag
combinations, and the external-storage GC test uses the real `replayer`
directory name instead of the legacy `plan_replayer` literal.

Go's filename-time failpoint is test instrumentation rather than production
behavior. Rust retains fixed timestamp fixture data only where the upstream
domain GC test needs exact file ages; it does not add a production override.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/replayer` — passed.
- `go test ./pkg/util/replayer -count=1` — passed (`[no test files]`).
- `cargo test --offline --locked -p tidb-domain --no-run` — passed with existing warnings.
- `cargo test --offline --locked -p tidb-session --no-run` — passed with existing warnings.
- `cargo test --offline --locked -p tidb-domain plan_replayer::tests::dump_gc_file_parse_time -- --exact` — passed, 1 test.
- `cargo test --offline --locked -p tidb-session tests_domain_plan_replayer_source::dump_gc_file_parse_time -- --exact` — passed, 1 test.
- `rustfmt --edition 2021 --check crates/tidb-domain/src/replayer.rs crates/tidb-domain/src/lib.rs crates/tidb-domain/src/plan_replayer.rs crates/tidb-session/src/tests_domain_plan_replayer_source.rs crates/tidb-session/src/tests_domain_plan_replayer_handle_source.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: restores the exact directory and filename branches and one
  canonical task-key owner. Randomness and nanosecond time come from native
  Rust facilities with the same 16-byte padded URL-base64 output contract.
- Compatibility: removes the non-Go `plan_replayer::PlanReplayerTaskKey`
  ownership path, ordering traits, and constructor; all in-tree users were
  migrated to the Go-shaped owner and struct fields.
- Performance: filename generation and writer forwarding remain constant-time;
  no policy or caching behavior was introduced.
