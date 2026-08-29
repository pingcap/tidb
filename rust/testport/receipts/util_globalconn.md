# `pkg/util/globalconn` — complete package transcreation

Pinned TiDB source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts, all read in full: `globalconn.go`,
`globalconn_test.go`, `pool.go`, `pool_test.go`, and `BUILD.bazel`. There is no
package doc, fixture, generated input/output, platform variant, README, or
ownership file. The local Go package is byte-identical to the pin.

Production behavior comprises GCID encoding and parsing, simple and global
allocators, 32-to-64-bit allocation transitions, reserved IDs, the
auto-increment pool, and the lock-free 32-bit circular pool. Go's global-kill
test build also injects alternate 32-bit server/local widths through three
linker values. The package has nine unit tests and two benchmark families.

## Rust ownership and audit result

`rust/crates/tidb-util/src/globalconn/` is the production owner. Its public
allocator and pool behavior follows the Go package. Rust atomics retain Go's
sequentially consistent operations; slot values are atomic because Rust does
not permit Go's plain racing store even though both are guarded by the same
sequence protocol. The source's explicit head/tail layout padding is retained.

The missing global-kill build configuration is now supplied by
`tidb-util/build.rs` through `TIDB_GLOBAL_KILL_TEST`,
`TIDB_GLOBAL_KILL_SERVER_ID_BITS32`, and
`TIDB_GLOBAL_KILL_LOCAL_CONN_ID_BITS32`. Defaults remain 0, 11, and 20; a
compile-time probe also passed with Go's integration-test values 1, 2, and 4.

The Rust-only `SimpleAllocator::default`, cache-line alignment policy, and five
supplemental allocator/pool tests were removed. The inline suite now maps the
exact nine Go tests. Native OS-thread counts are reduced in the two largest
concurrency tests to keep the same producer/consumer protocol practical
without pretending that OS threads are Go goroutines. `benches/globalconn.rs`
contains executable translations of both source benchmark families and no
additional benchmark family.

The ordinary server continues to consume the canonical allocator. Its
three-authenticated-session concurrency test passed through that path.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/globalconn` — passed.
- `go test ./pkg/util/globalconn -count=1` — blocked before package execution
  by the existing Go dependency compile error
  `google.golang.org/grpc/internal/transport/handler_server.go: undefined: http2.TrailerPrefix`.
- `cargo test --offline --locked -p tidb-util --lib globalconn::tests --no-fail-fast` — passed, 9 tests.
- `cargo test --offline --locked -p tidb-util --no-run` — passed.
- `cargo bench --offline --locked -p tidb-util --bench globalconn --no-run` — passed.
- `cargo test --offline --locked -p tidb-server --test all fixed_workers_hold_three_authenticated_sessions_concurrently_and_drain_all` — passed, 1 test.
- `cargo check --offline --locked -p tidb-server` — passed.
- `cargo fmt -p tidb-util -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: all nine Rust package tests and the ordinary server consumer
  test pass. The pinned Go package could not run locally because the existing
  Go dependency graph fails first.
- Compatibility: removes only Rust APIs and supplemental tests absent from Go;
  in-tree consumers compile unchanged.
- Performance: removes a Rust-only cache-line-alignment policy and restores the
  explicit source layout. The lock-free sequence and atomic ordering remain
  unchanged; benchmark executables compile but were not timed in WIP.
