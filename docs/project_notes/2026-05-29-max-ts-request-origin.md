# Max-TS Request Origin Tracking

## Goal

Allow TiKV to distinguish TiDB server requests from other clients and enforce zero-drift max-ts validation for selected non-TiDB request paths:

- `ScanLock`
- backup range
- `CheckTxnStatus`
- `Cleanup`
- `CheckSecondaryLocks`

## Decisions

- Use a typed `kvrpcpb.Context.request_origin` field instead of parsing `request_source`.
- `client-go` owns a process-wide default request origin. Its default is `RequestOriginUnknown`.
- Only the TiDB server binary should set the client-go process default to `RequestOriginTiDB`.
- Plain client-go, BR, Lightning, and third-party callers remain `RequestOriginUnknown` unless their process explicitly opts in.
- TiKV should keep a single `ConcurrencyManager::update_max_ts` entry point; request-origin policy is carried by the source/context argument.
- TiKV should reuse existing max-ts machinery: cached TSO, PD double-check, `InvalidMaxTsUpdate`, logging, and `storage.max-ts.action-on-invalid-update`.
- Do not introduce a new TiKV user-facing mode/config.
- Do not implement CSE in phase 1; port from TiKV after TiKV merge.

## Implementation Notes

- kvproto: add `RequestOrigin` enum and `Context.request_origin = 29`, then regenerate outputs and `scripts/proto.lock`.
- client-go/TiDB: fill `Context.request_origin` from the client-go process default in `tikvrpc.NewRequest` and `tikvrpc.AttachContext`; set that default only from `cmd/tidb-server`.
- TiKV: preserve existing tolerant behavior for default `update_max_ts` callers. The five target paths pass a source wrapper derived from request context; non-TiDB origin validates against exact PD TSO without drift.
- Draft stack dependency references:
  - kvproto: https://github.com/pingcap/kvproto/pull/1479
  - client-go: https://github.com/tikv/client-go/pull/1975
  - TiKV: https://github.com/tikv/tikv/pull/19654
  - TiDB: https://github.com/pingcap/tidb/pull/68779
  - TiKV tracking issue closed by the TiKV PR: https://github.com/tikv/tikv/issues/19656
- TiDB temporarily uses fork `replace` directives for kvproto/client-go so the draft PR can compile before upstream dependency PRs merge. Remove the temporary replaces and the resulting `go.sum`/`DEPS.bzl` churn after kvproto/client-go are merged and normal dependency bumps are available.

## Validation Plan

- kvproto proto generation/check.
- client-go/TiDB tests proving TiDB-origin is attached when the process default is set and plain client-go defaults to unknown.
- TiKV concurrency-manager tests for drift-allowed and no-drift behavior.
- TiKV target-path tests for the five enforced operations.
- TiDB Ready-profile validation before claiming completion, including `make bazel_prepare` if Go import/proto/dependency changes require it.

## Progress

- 2026-05-29: Tracking doc created before code changes.
- 2026-05-29: `kvproto` added `RequestOrigin` enum and `Context.request_origin = 29`; regenerated Go/proto lock outputs. `make proto-fmt go` currently stops at the expected `scripts/proto.lock` dirty-check gate after generation.
- 2026-05-29: Reworked `client-go` origin propagation to a process-wide default in `tikvrpc`. `NewRequest` and `AttachContext` fill missing `Context.request_origin`; default remains `RequestOriginUnknown`.
- 2026-05-29: `TiDB` sets the client-go process default to `RequestOriginTiDB` in `cmd/tidb-server` only. Removed per-transaction/per-snapshot origin plumbing so BR, Lightning, and plain client-go are not accidentally marked as TiDB by importing TiDB packages.
- 2026-05-29: `TiKV` keeps the single `ConcurrencyManager::update_max_ts` entry and adds a `MaxTsUpdateSource` wrapper to disable drift for selected non-TiDB request paths. Updated paths: `scan_lock`, backup range, `CheckTxnStatus`, `Cleanup`, and `CheckSecondaryLocks`.
- 2026-05-29: Opened draft PRs for kvproto, client-go, and TiKV. Prepared TiDB as a stack branch with temporary fork replaces for kvproto and client-go.
- 2026-05-29: Re-ran TiDB validation after switching from temporary `go.work` to publishable fork replaces:
  - Passed: `make bazel_prepare`; `GOPRIVATE=github.com/ekexium/kvproto,github.com/ekexium/client-go ./tools/check/failpoint-go-test.sh pkg/session/test -run TestRequestSource`; `GOPRIVATE=github.com/ekexium/kvproto,github.com/ekexium/client-go gotestsum --format short-verbose -- ./cmd/tidb-server -run '^$'`; `GOPRIVATE=github.com/ekexium/kvproto,github.com/ekexium/client-go make lint`; `git diff --check`.
- 2026-05-30: Refined the `kvproto` `request_origin` comment so the field is a declared client-provided origin, not an intrinsically trusted/authenticated wire signal. Renamed enum values from `REQUEST_ORIGIN_*` to `RequestOrigin*` to follow the proto's CamelCase enum-value style while retaining a prefix that avoids top-level enum value collisions. The comment also states that `RequestOriginUnknown` means unset or unrecognized by this protocol version, and future non-TiDB components can add dedicated enum variants when they need origin-specific behavior. Updated client-go, TiKV, and TiDB temporary dependency pins to the new kvproto/client-go commits after force-pushing the stacked PR branches.
- 2026-05-30: Re-ran focused validation after the dependency pin update:
  - Passed: kvproto `scripts/proto_format.sh --check`, `go test ./pkg/kvrpcpb`, and `git diff --check`; client-go `GOPRIVATE=github.com/ekexium/kvproto gotestsum --format short-verbose -- ./tikvrpc`; TiKV `cargo nextest run -p concurrency_manager test_update_max_ts_exact`; TiDB `make bazel_prepare`, `./tools/check/failpoint-go-test.sh pkg/session/test -run '^TestRequestSource$' -count=1`, `GOPRIVATE=github.com/ekexium/kvproto,github.com/ekexium/client-go gotestsum --format short-verbose -- ./cmd/tidb-server -run '^$'`, `GOPRIVATE=github.com/ekexium/kvproto,github.com/ekexium/client-go make lint`, and `git diff --check`.
- 2026-05-29: Validation notes:
  - Passed: `scripts/proto_format.sh --check`; `go test ./pkg/kvrpcpb`; client-go `./tikvrpc` and compile checks with temporary `go.work`; TiDB `TestRequestSource` with `-tags=intest`; TiDB affected package compile checks with temporary `go.work`; TiDB `make bazel_prepare`; TiDB `GOWORK=/tmp/max-ts-request-origin-work/go.work make lint`; TiKV `cargo test -p concurrency_manager test_update_max_ts_exact --lib`; TiKV `cargo check -p tikv --lib`; TiKV `cargo check -p backup`; `git diff --check` in kvproto, client-go, TiDB, and TiKV.
  - Known local issues: Earlier broad client-go checks had unrelated local failures: `./txnkv/txnlock -run TestTryAsyncResolve` fails with semaphore metric mismatch, and full `./tikv` tests fail in existing mockstore/PD tests while `./tikv -run '^$'` compiles.
