# `pkg/store/copr` Go-master parity receipt

Status: Ready for this focused package batch. The receipt covers the complete
root Go package inventory at Go `origin/master`
`a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7` (pulled 2026-09-02); it is not a
repository-wide parity claim.

## Complete inventory

All root production, test, build, and support artifacts were read before the
edit. The root package has 20 tracked artifacts and 11,165 lines. It has 61
top-level test/benchmark declarations and 177 function declarations in total.
There is no `doc.go`, generated Go source/input, platform-specific variant,
fixture, or benchmark fixture. The nested `copr_test` and `metrics`
directories are separate Go packages and remain separate receipt boundaries.

| Artifact | Lines | Surface |
| --- | ---: | --- |
| `BUILD.bazel` | 130 | library/test target and dependencies |
| `batch_coprocessor.go` | 1,739 | TiFlash batch task construction |
| `batch_coprocessor_test.go` | 587 | batch task and topology tests |
| `batch_request_sender.go` | 119 | region batch RPC sender |
| `coprocessor.go` | 3,270 | task building, workers, retries, runtime stats |
| `coprocessor_cache.go` | 224 | coprocessor cache |
| `coprocessor_cache_test.go` | 259 | cache tests |
| `coprocessor_test.go` | 1,518 | task, limiter, response, and retry tests |
| `ema.go` | 64 | paging EMA |
| `ema_test.go` | 183 | EMA tests |
| `key_ranges.go` | 165 | key-range representation |
| `key_ranges_test.go` | 126 | key-range tests |
| `main_test.go` | 47 | package test setup |
| `mpp.go` | 357 | MPP request plumbing |
| `mpp_probe.go` | 335 | MPP store probing |
| `mpp_probe_test.go` | 229 | MPP probe tests |
| `range_diagnostics.go` | 94 | range diagnostics |
| `region_cache.go` | 1,024 | region and bucket cache |
| `region_cache_test.go` | 539 | region-cache tests |
| `store.go` | 156 | coprocessor store wrapper |

The nested `copr_test` package contains its own `BUILD.bazel`, helper test
source, and test main; the nested `metrics` package contains its own build and
metrics source. Their callers were inventoried but are not folded into this
root-package line count.

## Go behavior restored

This batch brings the complete root package to the fetched Go-master behavior
after the earlier child-lock and bucket-version fixes:

- `CoprRequestLimiter` is attached to each TiKV physical RPC attempt through
  client-go's `RequestAttemptLimiter`. Query-scoped per-store limiters take
  precedence over the request-wide fallback, cancellation and iterator
  completion stop waits, and blocking wait time is exposed through
  `LimiterWaitStats`.
- Store batching now advertises `AllowBatchTaskDataMerge` and
  `ExecuteBatchTasksSerially`, accepts unhinted/non-DAG tasks only when the
  caller opts into the merged-response contract, and rebuilds a whole batch
  only for client-go's synthetic pre-dispatch region error. Responses with a
  context or any child data are reconciled task-by-task to avoid replaying
  successful work.
- A merged child response contributes execution details without emitting an
  empty result; unanswered or region/lock-failed children are counted as one
  fallback even when a retry fans out across multiple regions. Read-pool
  execution details and limiter waits are retained in the iterator runtime
  statistics, including nil-safe collection paths.
- The prior `StoreBatchTaskResponse` child-lock resolver remains intact and is
  covered with the existing RPC-count regression.

The focused root regressions cover limiter precedence/cancellation/wait stats,
merged and unanswered child responses, safe region-cache-miss rebuilding,
fallback accounting after a split, child-lock resolution, and bucket-version
updates. The nested integration tests additionally cover request/query limiter
concurrency, batch-store construction, and runaway-checker accounting.

## Rust boundary

Rust has no dependency-closed owner for Go's `pkg/store/copr` worker lifecycle,
region-cache retry orchestration, client-go request-attempt callback, or
TiDB-specific runtime-stat aggregation. The Rust `tidb-distsql`/`tidb-txnkv`
owners remain lower-level transport boundaries; no speculative coprocessor
facade or Rust-only execution path was added or removed here.

## Ready validation

- Pre-fix package compilation failed at the old `CoprRequestRateLimit`
  references after `pkg/kv` had restored Go master's typed limiter fields;
  this was the direct signal for the migration in this batch.
- Focused root regressions passed:

  ```text
  PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
  TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/store/copr \
    -run 'Test(CoprRequestLimiter|HandleBatchCopResponse|BuildTasks)$' \
    -count=1 -vet=off
  # passed
  ```

- Full root package suite passed with the same failpoint wrapper:

  ```text
  PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
  TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/store/copr \
    -count=1 -vet=off
  # passed (44.440s)
  ```

- The nested `pkg/store/copr/copr_test` run in the integration worktree is
  currently blocked by the separate executor/distsql migration still calling
  `SetCoprRequestRateLimit`; the coherent Go-master snapshot is the source
  reference for those tests until that owning package lands.
- `make bazel_prepare` was attempted with the pinned Go environment and is
  blocked because the local `bazel` executable is unavailable. The root and
  nested BUILD dependency/shard changes are included in this batch.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all --
  --check` and `git diff --check` passed. Repository `make lint` is a Ready
  gate for the final worktree; current integration lint remains coupled to
  unrelated in-progress migrations.

## Risks and remaining work

Correctness risk is concentrated in response-shape handling: only a proven
pre-dispatch fake region error permits whole-batch rebuild, while any possible
child result uses flat reconciliation. Compatibility risk is limited to the
client-go retry callback and the two advertised StoreBatch flags; older stores
continue to use per-task responses. The no-limiter and no-batch paths retain
their previous allocation and scheduling behavior. Live TiKV/PD behavior,
Bazel analysis, the nested package's current-branch test run, and a Rust
coprocessor worker implementation remain unverified or explicitly outside
this package boundary.

## Follow-up Go package batch: API-v2 StoreBatch lock keys

Go master `1c1a334d2b` (pulled 2026-09-02) advances client-go to
`v2.0.8-0.20260831103552-e4905600583b`, whose API-v2 response decoder returns
decoded bucket/lock boundaries before TiDB updates the region cache or resolves
child locks. The coprocessor package keeps that source contract and now has a
focused API-v2 StoreBatch lock regression using a keyspace codec; the test
asserts that the check-txn-status primary key is encoded exactly once. The
matching kvproto/PD imports are present in the package BUILD target, and the
Go module checksum/Bazel pin is updated as the dependency-closed package input.

Ready evidence for this package-level follow-up:

- failpoint-wrapped `TestHandleBatchCopResponse/API_V2_child_lock` and the full
  root `pkg/store/copr` package suite pass with the current client-go pin;
- `gofmt`, `git diff --check`, and `make lint` pass;
- `make bazel_prepare` is required for the BUILD/dependency changes but is
  blocked locally because the `bazel` executable is unavailable.

The complete 20-artifact root inventory remains the atomic Go package boundary;
the nested `copr_test` and `metrics` packages are unchanged separate claims.
