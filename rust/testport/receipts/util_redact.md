# `pkg/util/redact` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `redact.go`,
`redact_test.go`, and `BUILD.bazel`. They define mode-based string and
stringer redaction, line-oriented marker removal, file de-redaction, the
process redaction flag, value/key/writer helpers, stream-backup task credential
scrubbing, and exactly three direct tests. There is no package doc, README,
fixture, benchmark, generated or platform variant, or ownership file. The
checkout is byte-identical to the pin and is also unchanged from the older
source revision previously used by the Rust implementation.

## Rust ownership and audit result

`rust/crates/tidb-util/src/redact.rs` owns the package and
`rust/crates/tidb-util/src/redact/compact_text.rs` supplies the behavior of the
Go gogo-protobuf dependency for `StreamBackupTaskInfo`. The latter is required
production dependency behavior, not an additional redact feature.

The production audit retained every Go API and policy branch. It made the
stringer adapter type private, matching Go's unexported `redactStringer`, and
uses the native I/O error type for the Go reader error path rather than an
extra public Rust error. The test module now contains exactly the three Go
source tests. Nine supplemental Rust tests, the obsolete historical ExecPlan,
and the custom semantic-manifest artifact were removed because they are not
artifacts of this Go package. The stream-backup consumer test belongs to its
own Go package and is not claimed here.

A strict API re-audit replaced Rust's narrowed `de_redact(&str) -> String`
convenience with Go's general reader/writer operation. It now scans arbitrary
input line by line, writes to the caller's output, preserves Go's ignored
scanner/flush errors and checked unwrapped-span copy, and `de_redact_file`
uses that same ordinary path. The existing `TestDeRedact` translation exercises
the corrected signature, so no supplemental test was added.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test --offline --locked -p tidb-util --lib redact::tests:: --no-fail-fast` — passed, exactly 3 tests.
- `cargo test --offline --locked -p tidb-util --no-run` — passed.
- `cargo fmt -p tidb-util -- --check` and `git diff --check` — passed.
- `go test ./pkg/util/redact -run '^(TestRedact|TestDeRedact|TestRedactInitAndValueAndKey)$' -count=1` — blocked before package execution by the existing Go dependency compile error `google.golang.org/grpc/internal/transport/handler_server.go: undefined: http2.TrailerPrefix`.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production behavior is retained; the direct source test set
  passes in Rust.
- Compatibility: one artificial public Rust error and one artificial public
  adapter type are no longer exposed.
- Performance: unchanged.

## Rust follow-up: Go-discardable redact helpers

The complete three-artifact inventory and source behavior above remain
unchanged. The Rust owner carried five explicit `#[must_use]` annotations on
the direct Go-shaped `String`, `Stringer`, `NeedRedact`, `Value`, and `Key`
helpers. Go permits callers to discard all of these results, so the
annotations were Rust-only diagnostics and are now removed without changing
redaction mode, marker, key, or value semantics.

The focused unit regression
`redact::tests::return_values_may_be_ignored_like_go` discards every affected
result under `#[deny(unused_must_use)]`. On the pre-fix owner it failed to
compile with five unused-return errors; the fixed test passes.

Ready validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib redact::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib redact::tests:: -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util -- --check`, repository `make lint`, and `git diff --check` — passed.

No Go, generated, fixture, platform, Bazel, or module artifact changed, so
`make bazel_prepare` is not required.
