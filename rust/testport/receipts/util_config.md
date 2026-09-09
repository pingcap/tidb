# `pkg/util/config` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `config.go` and
`BUILD.bazel`. There is no package doc, test, test harness, benchmark, fixture,
generated input/output, platform variant, README, or ownership file. The local
Go package is byte-identical to the pin.

Its only production API decodes a TOML `map[string]string`, ignores
`innodb_lock_wait_timeout`, looks up and validates every other session system
variable, stores the normalized value, logs skipped assignments, and returns
the unknown or rejected names without turning individual assignment failures
into the function error.

## Rust ownership and integration decision

`rust/crates/tidb-session/src/util_config.rs` is the sole owner because
`SessionVars` and the system-variable registry live in that crate. A Rust
`Read` boundary replaces Go's `io.ReadCloser`; as in Go, closing belongs to the
caller. The native `LoadError` preserves the two function-level failures—input
I/O and TOML decoding—while individual variable failures remain entries in the
returned vector. A standard hash map retains Go's unspecified map iteration
order.

The function reuses the ordinary session validator and setter rather than
duplicating a plan-replayer-specific variable catalog. Warning messages and
the structured `error` field go through the existing `logutil.BgLogger`
transcreation.

The pinned package has one consumer:
`pkg/executor/plan_replayer.go::loadVariables`. Rust has no executor-side plan
replayer ZIP load path, so there is no existing caller to wire. This is an
explicit `pkg/executor` integration gap; the package API itself is complete,
and no cache-only, test-only, or fabricated load pipeline was introduced.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/config` — passed.
- `go test ./pkg/util/config -count=1` — blocked before package execution by existing build failures: missing `checkMapABI` in `pkg/util/hack` and missing `http2.TrailerPrefix` in gRPC transport.
- `cargo check --offline --locked -p tidb-session` — passed with existing warnings.
- `cargo test --offline --locked -p tidb-session --no-run` — passed with existing warnings.
- `rustfmt --edition 2021 --check crates/tidb-session/src/util_config.rs crates/tidb-session/src/lib.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the implementation uses the canonical session-variable
  registry and setter, including Go's second validation pass and typed session
  hooks. The sole end-to-end ZIP consumer remains absent and is not claimed.
- Compatibility: adds the missing public function and its native error type;
  no existing API is removed or redirected.
- Performance: one TOML decode and one hash-map pass, matching Go; no caching
  or ordering policy was introduced.
