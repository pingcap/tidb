# `pkg/meta/autoid` — Go-master allocator-service parity receipt

Status: complete inventory; restored the dependency-closed service-client
behavior in the Go checkout and kept the corresponding Rust client slice
aligned. Go now carries the current allocation/rebase synchronization and
bounded RPC retry policy; the etcd-backed Go service owner and live gRPC
integration remain an explicit Rust boundary.

Comparison source: Go `origin/master` at
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02). The package has 11
tracked artifacts and 4,402 lines. It has no fixture/testdata directory,
generated production source, platform-specific variant, or nested Go package.

## Complete Go inventory

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 81 | allocator library/test targets and dependencies |
| `autoid.go` | 1,381 | persistent allocators, range arithmetic, sequence allocation, and stats |
| `autoid_service.go` | 645 | etcd discovery, RPC allocator, retry policy, and ownership transfer |
| `autoid_service_test.go` | 828 | RPC cancellation, retry, transfer, and concurrency regressions |
| `autoid_test.go` | 685 | signed/unsigned allocator and range tests |
| `bench_test.go` | 134 | allocator and sequence benchmarks |
| `errors.go` | 74 | allocator and auto-random error definitions |
| `main_test.go` | 34 | package setup and leak checks |
| `memid.go` | 167 | temporary-table in-memory allocator |
| `memid_test.go` | 129 | in-memory signed/unsigned allocation tests |
| `seq_autoid_test.go` | 244 | sequence cache and concurrent sequence tests |

The production files contain 102 function/method declarations; the tests and
benchmark harnesses contain 43 declarations. All artifacts were read before
editing, including the current-master additions from `f31b27fd75`
(count-and-duration RPC retry limits), `17c0dd0fe4` (keyspace request fields),
and `52920c5f6d` (concurrent ownership transfer and monotonic allocation).

## Implemented Go and Rust slices

The Go package was behind this complete Go-master source in the checkout.
This batch restores the three changed artifacts (`BUILD.bazel`,
`autoid_service.go`, and `autoid_service_test.go`) as one Go-package unit.
The service now serializes transfer/forced rebase against allocation, records
the greatest successful response monotonically (including unsigned ordering),
and stops repeated RPC failures at the source count-and-duration limit. The
expanded tests exercise out-of-order responses, transfer ordering and
rollback, forced versus monotonic rebases, retry-limit logging, and context
cancellation. The BUILD target now has the 17-shard metadata and all current
test dependencies.

- `tidb-exec::AutoIdServiceAllocator::alloc` now records `resp.max`, as Go's
  `updateLastAllocated` does, rather than the lower range endpoint. A CAS loop
  preserves the greatest value when concurrent RPC responses arrive out of
  order and compares the bit pattern as an unsigned value for unsigned tables.
- Non-forced `rebase` is monotonic; forced rebase intentionally sets the
  current value exactly, matching Go's `force` arm.
- Allocation and rebase each track RPC failures with Go's AND condition:
  minimum error count plus minimum elapsed time. The production defaults are
  ten errors over fifteen seconds. A typed `RpcRetryLimit` preserves the
  operation, count, elapsed interval, and final RPC error while existing
  cancellation/deadline checks still win before reset/backoff.
- `tidb-exec::AutoIdServiceAllocator::transfer` now serializes ownership
  transfer against allocation/rebase, refreshes the source base with
  `Alloc(0, 1, 1)`, rebases the destination to the greatest observed value,
  and restores the source binding when the destination RPC fails. This closes
  the cross-database rename case that prevents a cold allocator from reusing
  IDs after the source table moves.
- The focused regression drives out-of-order allocation responses, lower
  non-forced rebases, exact forced rebases, and a two-error zero-duration retry
  policy to prove the loop makes exactly two calls and does not retry forever.

No Rust-only allocator path was removed. The existing generation-safe
connection reset and cancellation-aware backoff remain the transport boundary;
the new transfer method is a typed Rust equivalent of Go's stateful service
operation.

## Validation

Profile: Ready for this code batch. Go production/test and Bazel files were
restored, so the required `make bazel_prepare` gate was attempted and is
blocked locally because the `bazel` executable is unavailable.

- Before the Go restoration, the Go-master transfer regression failed to
  compile with missing `stateMu`, `rpcRetryPolicy`, and related methods.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/meta/autoid -run '^(TestSinglePointAllocTransfer|TestAutoIDRPCRetry)$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/meta/autoid -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --lib cluster_auto_id -- --test-threads=1` — passed (10 tests, including source-base refresh, destination rebase, same-binding no-op, and rollback-on-error transfer cases).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed after the code and documentation edits.
- `make bazel_prepare` — attempted as required for the Go/BUILD changes; blocked locally because `bazel` is unavailable.

## Risks and unverified surfaces

- Correctness risk is concentrated in signed versus unsigned CAS ordering and
  the distinction between a monotonic and forced rebase; both are covered by
  the focused regression.
- The Rust error is typed rather than Go's wrapped marker interface. Callers
  that need Go-compatible error classification must match the new variant (no
  current caller performs an exhaustive match).
- The Go service's etcd leader discovery, server-side allocator, metrics/log
  emission, and live gRPC tests are not dependency-closed in this crate and
  remain unverified.
- Live cross-node ownership transfer and mixed-version upgrade behavior were
  not run locally.
- The current Rust owner test remains unverified until the unrelated
  `auto_pre_split.rs` `FieldType::default()` compile errors are resolved in
  that separate `pkg/ddl` boundary.
