# `pkg/ttl/sqlbuilder` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package batch.

## Purpose / Big Picture

`pkg/ttl/sqlbuilder` owns TTL scan/delete SQL formatting and the continuation
state machine that walks key ranges. Its Rust owner is
`rust/crates/tidb-ttl/src/sql_builder.rs`. This batch restores Go's nil-versus-
empty continuation key contract without changing SQL formatting boundaries
that require the unavailable parser-driver/byte-preserving stack.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all four Go artifacts
  (1,510 lines), including BUILD metadata, test harness, and every test.
  Confirmed no doc, fixture, generated/platform, benchmark, fuzz, or ownership
  artifact.
- [x] (2026-09-02) Read the Rust SQL-builder source/test owner in full and
  compared every Go builder/generator branch. Isolated the empty continuation
  key mismatch as dependency-closed and retained arbitrary-byte/parser-driver
  behavior as explicit boundaries.
- [x] (2026-09-02) Added the focused empty-key regression; it failed before the
  fix with `id > 1` and passes after preserving `Some(&[])` in `set_stack`.
- [x] (2026-09-02) Ran the complete Rust SQL-builder suite and tagged Go
  package suite; Ready formatting, all-target compilation, lint, and diff gates
  are recorded in `receipts/ttl_sqlbuilder.md`.
- [ ] Publish one scoped commit, push it to `hparser-integration`, fetch and
  fast-forward pull the latest tip, verify local/remote SHAs, then continue the
  rolling package audit.

## Scope and decisions

The atomic unit is the complete Go package: `sql.go`, `sql_test.go`,
`main_test.go`, and `BUILD.bazel`. `set_stack` now maps `None` to the configured
range start and preserves `Some(empty)` as an empty stack, matching Go's slice
nilness. Existing Rust `String` and AST restore limitations remain named
boundaries; changing the public result type or inventing parser nodes would be
outside this package's dependency closure.

## Validation gate

Run from the repository root with the pinned local toolchains:

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    OPENSSL_DIR=<pinned OpenSSL dir> DYLD_LIBRARY_PATH=<pinned OpenSSL lib> \
      cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --all-targets
    OPENSSL_DIR=<pinned OpenSSL dir> DYLD_LIBRARY_PATH=<pinned OpenSSL lib> \
      cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --test sql_test -- --test-threads=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/ttl/sqlbuilder -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

No Go/Bazel artifact changed, so `make bazel_prepare` is not required.

## Decision log

- 2026-09-02: Treat nil-versus-empty continuation handling as the focused
  dependency-closed fix; preserve the arbitrary-byte and parser-driver seams as
  explicit boundaries.

## Outcomes and retrospective

After publication, the receipt will record the commit and remote SHA. The
source-shaped scan continuation behavior will be complete; formatter and AST
limitations remain explicit and unverified outside their owner tests.
