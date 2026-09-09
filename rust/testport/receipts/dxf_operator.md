# `pkg/dxf/operator` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 31 | `4f6230db9499f95837c70fc19ff2280e83609969` | workspace crate `tidb-dxf-operator` and its single package test target |
| `compose.go` | 60 | `cc5ba69b2062d4ce66720e92c0f1ebf15dd301bf` | `compose.rs`: source/sink seams, unbuffered shared channel, finish semantics, and composition |
| `operator.go` | 131 | `5724536d6ac5292c9f2d7c2d4bd1cc32c021c140` | `operator.rs`: operator and tuning contracts, first-error cancellation context, panic-safe workers, transforms, pool lifecycle, and resizing |
| `pipeline.go` | 89 | `179c2307ad7846b51c21aae1e3cd99e728d40e31` | `pipeline.rs`: ordered open, reverse cleanup after open failure, ordered close with first error, started state, string form, and four-stage reader/writer lookup |
| `pipeline_test.go` | 129 | `d4c71f55f87b4c5bb09dc66ff9072b7d2494d497` | exact success/error branches execute in `pipeline_test.rs`, including the source string, transformations, concurrent count, cancellation, and collected `hit` result |
| `wrapper.go` | 141 | `0953ea44123d3fadedc384ecc42c12741bd986ef` | `wrapper.rs`: simple data source, package-private sink and transforming operator, shared-context cancellation, drain, and close behavior |

There is no package doc, fixture, benchmark, generated artifact, platform
variant, or other test in the pinned directory.

## Native integration decision

Go implements `AsyncOperator` over `pkg/resourcemanager/pool/workerpool`.
Rust now uses the canonical `tidb_resourcemanager::workerpool` implementation
through the same package boundary. The earlier DXF-local context, worker,
panic, lifecycle, and tuning implementation was removed rather than retained
as a second execution path.

Go's channel type is bidirectional and closeable. `SimpleDataChannel` keeps one
shared close state around a zero-capacity native channel owned by the shared
resource-manager channel carrier. Every composed handoff is unbuffered, and a
second public finish panics like closing a closed Go channel. The native
`NoResult` marker is the spelling of external `workerpool.None`; ordinary
operators without a configured result consumer retain Go's blocking result
channel instead of silently discarding output.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo test --locked -p tidb-dxf-operator
git diff --check
```

## Follow-up closure — discardable constructor returns (2026-09-06)

The complete six-artifact, 581-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; all production,
test, and BUILD files remain byte-identical to the pinned source. The Rust
owner (`pipeline.rs`, `wrapper.rs`, and the existing source test) and every
constructor caller were read before editing.

Go permits discarding `NewAsyncPipeline`, `NewSimpleDataSource`, the private
`newSimpleSink`, and the private `newSimpleOperator` results. Rust had marked
the four direct counterparts `#[must_use]`, creating four Rust-only
`unused_must_use` diagnostics under a deny-on-discard caller. Those annotations
were removed without changing channels, worker-pool lifecycle, cancellation,
pipeline ordering, or error propagation.

The focused regression `pipeline_test::go_constructor_return_values_can_be_ignored`
invokes all four constructors under `#[deny(unused_must_use)]`. Before the
implementation edit it failed with exactly four diagnostics; after the edit it
passes. No Go, Bazel, Cargo dependency, or module file changed.

Ready validation for this Rust-only follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-dxf-operator --offline --locked go_constructor_return_values_can_be_ignored -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-dxf-operator --offline --locked -- --test-threads=1
PASS; 2 unit tests passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-dxf-operator --all-targets --offline --locked
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

The package remains otherwise covered by its existing pipeline behavior test;
the rolling audit continues.

## Follow-up closure — discardable pipeline query returns (2026-09-06)

The complete six-artifact, 581-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; all production,
test, and BUILD files remain byte-identical to the pinned source. The Rust
owner and all three pipeline tests were rechecked before editing.

Go permits callers to discard `AsyncPipeline.IsStarted`, `String`, and
`GetReaderAndWriter` results. Rust had marked the direct `is_started`,
`pipeline_string`, and `reader_and_writer` methods `#[must_use]`, imposing
three Rust-only `unused_must_use` diagnostics. The annotations were removed
without changing pipeline state, formatting, operator lookup, or lifecycle
behavior. `pipeline_test::go_pipeline_query_returns_can_be_ignored` invokes
all three methods under `#[deny(unused_must_use)]`; with the annotations
restored it failed with exactly three diagnostics, and after the edit it
passes.

Ready validation for this Rust-only follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-dxf-operator --lib go_pipeline_query_returns_can_be_ignored -- --test-threads=1
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-dxf-operator --lib --test-threads=1
PASS; 3/3 owner tests passed.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-dxf-operator --all-targets
PASS.

rustfmt +nightly-2026-08-22 --check --edition 2021 rust/crates/tidb-dxf-operator/src/pipeline.rs rust/crates/tidb-dxf-operator/src/pipeline_test.rs
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
dependency, or module file changed, so `make bazel_prepare` was not required.
