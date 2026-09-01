# `pkg/executor/internal/testutil` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains six tracked artifacts and 909 lines. Every production
helper and Bazel target was read line by line before editing. The directory has
no `*_test.go` files, generated source, platform-specific variants, fixtures,
benchmarks, or fuzz targets; all helpers exist to support other Go executor,
planner, join, aggregation, sort, limit, and window tests.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 30 | `d71b7935f737c653a58937dabc29b523fd8997b8` | `1cd9666f50098d88b17b5dab6cce5acff470e780ebe2eb719a87bf122c77d347` | internal helper library target |
| `agg.go` | 69 | `1810f3249ca7527a6a2b49852276d767b6ddce15` | `d71069627cc19021507021b44a7b14ba3e9a37a3e30483b1d94b572bb6f1f0e0` | aggregate benchmark/test case defaults and schema |
| `limit.go` | 66 | `330bcc45732bafe9402055c73f8f779562127989` | `ee6276d97654b416f30657184fe637fe804aa1fb7f063466c04a3cef081f3627` | limit case defaults and memory setup |
| `sort.go` | 68 | `dd44248eee2adb3748af8ca64317bfa0df7db9ee` | `245a7bd2e2ebfa568743f5c6780142341b1451b2f6895a304279a4ba4523d4d6` | sort case defaults and memory-limit setup |
| `testutil.go` | 603 | `c7f8f1bbeb5ef1d609b16b79b7a420a7d50a85b7` | `a9e839705d6777699cc4acb5164297fb4e8b4ceb49497b54b3efe9a13fca2c78` | mock source/physical plan, chunk generation, OOM action, and random typed data |
| `window.go` | 73 | `d80029bd49bea68e46b82a01ed83afda407713d2` | `5082c685a53be9fd73a007d4603a3cc8bb388d2b7ec12dd3bf57ca17b8060a47` | window case defaults, frame, schema, and function settings |

`MockDataSource` generates typed datums (including nulls, NDV-controlled
values, ordering, JSON, temporal, enum/set, decimal, bit, and string forms),
materializes them into chunks, and implements the executor `Next` contract.
`MockDataPhysicalPlan` wraps a supplied executor for planner tests. The
aggregate, limit, sort, and window case structs establish fixed schemas and
deterministic defaults, while `MockActionOnExceed` records OOM-action triggers.
`GenRandomChunks` and its column generator provide broad type coverage for
join and executor tests.

## Rust ownership and explicit boundary

This package is test-only scaffolding rather than SQL production behavior.
Rust tests construct `tidb-chunk` values and executor fixtures directly; the
Rust planner explicitly documents that Go's `logicalop.MockDataSource` is
benchmark scaffolding, and no dependency-closed Rust package exposes a shared
mock physical-plan/data-source API or Go-compatible random generator. The
Rust `tidb-util::memory` action chain covers production OOM behavior, but the
Go `MockActionOnExceed` counter is only a test probe.

No Rust-only behavior was found to remove and no public compatibility helper
was invented for uncalled test scaffolding. The inventory is therefore an
explicit SEED/boundary, not a completed production package claim.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, Bazel metadata, or module files changed, so `make bazel_prepare` and
the Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/executor/internal/testutil
# passed; package has no test files and compiles cleanly
```

Not verified here: every downstream Go suite that consumes these helpers,
Bazel execution, Rust planner/executor fixture coverage, and full workspace
tests. Existing unrelated privilege/session worktree changes remain outside
this receipt.

