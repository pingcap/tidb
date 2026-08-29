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

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test -p tidb-util --locked 'redact::tests::'` — passed (3 tests).
- `cargo test -p tidb-util --locked` — passed (649 unit tests and all integration/doc tests; 3 ignored helpers).
- `cargo fmt --all --check` and `git diff --check` — passed.
- `go test ./pkg/util/redact -run '^(TestRedact|TestDeRedact|TestRedactInitAndValueAndKey)$' -count=1` — blocked before this package compiled by the workspace's existing gRPC `http2.TrailerPrefix` dependency mismatch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production behavior is retained; the direct source test set
  passes in Rust.
- Compatibility: one artificial public Rust error and one artificial public
  adapter type are no longer exposed.
- Performance: unchanged.
