# `pkg/lightning/log` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly six artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 47 | `a7af1092e14c83db953bdf3cfb83b3f0d7678d81` | `tidb-util::lightning_log`, `tidb-log`, one Cargo benchmark target, and native dependency metadata |
| `filter.go` | 69 | `4f1fc2ed86b9f0f3ff51a2cd87b3d09d4e234b3c` | complete level check, context cloning, caller-package filtering, and delegated write errors |
| `filter_test.go` | 111 | `aacdab568d24f1c14da4750f15c35dc8fd13ce71` | one functional source test and both source benchmarks |
| `log.go` | 314 | `aa26fe5719335bbdf48b914d846e31b12aec9dea` | complete config, initialization, global state, errors, cancellation, logger wrapper, and timed tasks |
| `log_test.go` | 113 | `c3e3aeac024f510f0a39279127d7ab2f20e78f11` | four functional source tests |
| `testlogger.go` | 37 | `5fad50adc0d96407caefddad4814aeb91656ee28` | exact JSON test encoder and trailing-newline buffer behavior |

There is no package doc, fixture, testdata, generated source, platform source
variant, README, or ownership artifact. Bazel's short/flaky/five-shard
scheduling metadata has no Cargo runtime behavior to port.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_log.rs` owns the complete package and
composes the already canonical `tidb-log` and `pkg/util/logutil` owners. Config
fields retain target-sized Go integers and exact JSON names. `adjust` applies
only the source defaults and `warning` alias. Initialization keeps `-` as
stdout, rejects a directory with the exact source error, sets `GRPC_DEBUG`
only in diagnose mode, initializes the TiDB library logger at fatal, installs
the same Lightning logger into package and process globals, preserves the
DPanic stacktrace threshold, file rotation inputs, and shared atomic level,
and uses the exact five default allow-package substrings.

The caller filter checks the level before filtering, preserves context fields
through `With`, tests only the caller's package-qualified function rather than
message fields, and propagates direct core write errors. Rust caller file/module
paths are normalized to the equivalent TiDB, BR, ingestor, Lightning, main, or
PD-client package path before applying the source substring rule. The same
predicate lives in `tidb-log::Logger`, so calls through `tidb_log` globals
cannot bypass the filter. The canonical dependency gained only zap behaviors
used here: arbitrary-level logging, enabled checks, named children, and a
caller predicate; all its existing 22 tests remain green.

The wrapper retains global replacement and level mutation, child fields and
names, short errors without verbose stacks, the nil-error skip, direct and
wrapped cancellation, tonic canceled statuses, Smithy canceled/operation
wrappers, start/completed/canceled/failed messages, source level selection,
extra-field suppression on failure, elapsed `takeTime`, short-error `End`, and
full-error `End2`. Task dereferencing corresponds to Go's embedded `Logger`.
Smithy carrier errors are the native boundary for the Go SDK dependency; their
source chains and cancellation identity are the only behavior consumed.

The test logger emits exactly `$lvl`, `$msg`, context fields, and entry fields
in source order and removes only trailing newlines from `Stripped`. Exactly
`TestFilter`, `TestConfigAdjust`, `TestTestLogger`, `TestInitStdoutLogger`, and
`TestIsContextCanceledError` remain as snake-case Rust test identities.
`rust/crates/tidb-util/benches/lightning_log.rs` carries exactly
`BenchmarkFilterStringsContains` and `BenchmarkFilterRegexMatchString` with
the source inputs and filters. No supplemental test, benchmark, Rust-only
filter policy, alternate logger pipeline, prior owner, or duplicate remains.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/log
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/log
rg -n 'failpoint\.|testfailpoint\.|failpoint' pkg/lightning/log
```

The targeted source baseline was attempted from the repository root:

```text
go test -run '^(TestFilter|TestConfigAdjust|TestTestLogger|TestInitStdoutLogger|TestIsContextCanceledError)$' -tags=intest,deadlock ./pkg/lightning/log -count=1
```

The host dependency stack failed before compiling this package: cached gRPC
`internal/transport` refers to the unavailable HTTP/2 `TrailerPrefix` symbol.

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-log
cargo test --quiet --offline -p tidb-util lightning_log --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util --bench lightning_log
cargo clippy --quiet --offline -p tidb-log --lib --no-deps -- -D warnings
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
cargo clippy --quiet --offline -p tidb-util --bench lightning_log --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
git diff --check
```

The package has no failpoint use or dependency. No Go, Bazel, module, or
generated artifact changed, so `make bazel_prepare` is not required. The two
source-sized benchmarks were compile-checked rather than executed.
Cross-platform execution, workspace-wide tests, the blocked Go package
baseline, and the Ready-profile `make lint` were not verified locally. Cargo
emitted only the existing `tidb-model` `unused_mut` and vendored TiKV-client
`private_bounds` warnings.

## Risk

- Correctness: all six artifacts, five test identities, production branches,
  and two benchmark identities are mapped; the Rust tests and dependency suite
  pass, while the Go baseline is blocked before package compilation.
- Compatibility: tonic cancellation uses the workspace's canonical status;
  Smithy wrappers retain the Go SDK error-chain contract without importing an
  unrelated Rust AWS client. The global logger now shares Lightning's filter.
- Performance: filtering remains level-gated substring matching. Caller-path
  normalization is one allocation per enabled entry, corresponding to Go's
  already-materialized caller function string; no regex or alternate policy is
  used in production.
