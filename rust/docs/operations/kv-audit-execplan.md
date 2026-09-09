# `pkg/kv` parity audit ExecPlan

## Objective

Inventory the complete Go `pkg/kv` package before edits, keep its Rust owner
boundary explicit, and close concrete Rust-only behavior or missing Go
semantics only when the dependency-closed tests prove the change.

## Completed this batch

1. Read and inventoried all 30 Go production/test/Bazel artifacts (5,145
   lines), including the complete test harness and interface mocks. Confirmed
   there are no fixtures, generated files, benchmarks, or platform variants.
2. Read the `tidb-txnkv` KV contracts, transport owner, source-derived KV
   suite, batch wire/scheduler suites, and their build wiring. Kept the broad
   package claim at an explicit SEED/boundary because the SQL/session seam,
   TLS, several transaction options, and some MPP/storage implementations are
   not dependency-closed here.
3. Added `BatchCommandTag::ALL` in protobuf field-number order and concrete
   scheduler test types so the owner’s source suite compiles.
4. Fixed asynchronous and synchronous Coprocessor pending results to resolve
   publication receipts only for successful responses. Added a focused
   blocking-pull regression alongside the existing nonblocking elapsed-
   deadline regression; both preserve the original typed `Timeout`.
5. Recorded the full package parity receipt in `rust/testport/receipts/kv.md`.
6. Restored the missing Go `MaxCount`/`MinCount` pushdown cases and the two
   batch-task request flags, with focused Go regressions and fail-before/pass-
   after evidence.
7. Published the follow-up Go parity commit to `origin/hparser-integration`,
   fetched and fast-forward pulled, and verified the remote SHA.

## Validation gate

- [x] Focused batch source suite passes (76 tests), including the 11-test
  Coprocessor dispatch regression surface.
- [x] Full aggregated `tidb-txnkv` source suite passes with the documented
  stack setting (407 passed, 11 ignored).
- [x] Go `pkg/kv` focused unit suite passes.
- [x] `cargo fmt --all -- --check` passes.
- [x] Workspace Rust check passes offline and locked.
- [x] Ready profile `make lint` passes.
- [x] `make bazel_prepare` attempted for the Go/Bazel changes; blocked because
  the local `bazel` executable is unavailable.
- [x] Meaningful follow-up batch committed, pushed to
  `origin/hparser-integration`, and remote SHA verified after a fast-forward
  fetch.

## Remaining boundaries

The package remains an explicit SEED/boundary, not a complete transcreation
claim. The repository-wide package loop must continue with the next uncovered
package after this batch is committed and pushed. External etcd/live TiKV,
Bazel analysis, and omitted SQL/session integrations remain unverified.
