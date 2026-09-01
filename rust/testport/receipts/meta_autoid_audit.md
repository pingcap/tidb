# `pkg/meta/autoid` — Go-master allocator-service parity receipt

Status: complete inventory; implemented the dependency-closed service-client
slice from Go master. Rust now records the greatest allocation response
monotonically (including unsigned ordering), distinguishes forced from
monotonic rebases, and stops repeated RPC failures at the source count-and-time
limit. The etcd-backed Go service owner and live gRPC integration remain an
explicit boundary.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has 11
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

## Implemented Rust slice

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
- The focused regression drives out-of-order allocation responses, lower
  non-forced rebases, exact forced rebases, and a two-error zero-duration retry
  policy to prove the loop makes exactly two calls and does not retry forever.

No Rust-only allocator path was removed. The existing generation-safe
connection reset and cancellation-aware backoff remain the transport boundary.

## Validation

Profile: Ready for this code batch. The production edit is confined to the
existing Rust service-client owner and its unit test; no Go or Bazel source was
changed, so `make bazel_prepare` is not required.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/meta/autoid -run '^(TestInMemoryAlloc|TestAllocCanceledRPCReturnsQuickly|TestRebaseCanceledRPCReturnsQuickly|TestBackoffCtxAware)$' -count=1` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --lib cluster_auto_id -- --test-threads=1` — 8 passed.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check crates/tidb-exec/src/cluster_auto_id.rs` — passed.
- `make lint` — passed.
- `git diff --check` — passed after the code and documentation edits.

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
