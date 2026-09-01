# `pkg/executor/internal/builder` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 123 lines. Every production
source file and Bazel target was read line by line. There are no package tests,
fixtures, generated files, benchmarks, fuzz targets, or platform variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `ca7a82c43ad3e5d7885352e03c1b98e5b4efc123` | `948431ecd444f00c2c6d6c816b269892bfbdbd41532fb22188ea58a85a83e556` | internal builder library target and dependency closure |
| `builder_utils.go` | 105 | `7757c0fe03fd857cf7d6647dd56883092cd175b6` | `b1c23d185a2ed749f1f77067e7a57026d0f0ff569a5a1fbbddef2de85e619e54` | DAG executor-list/tree construction and request metadata |

`builder_utils.go` defines five helpers. The tree branch calls a physical
plan's TiFlash `ToPB`; the list branch lowers each plan to TiKV and stops on
the first error; the non-natural-order variant stamps each executor's
`ParentIdx`; and `ConstructDAGReq` sets timezone name/offset, pushdown flags,
runtime-summary presence, non-default division precision, TiKV versus TiFlash
shape, and the session-selected result encoding. The final wrapper applies
the same parent indices to the request. The package itself has no test
harness; callers under `pkg/executor` provide integration coverage.

## Rust ownership and explicit boundary

The dependency-closed Rust owner is split across `tidb-exec::dag_request`,
`real_tikv_read`, and `cop_scan`. `dag_request` already implements the
bounded one-scan TiKV list form, including timezone/flags/summary/division
precision/encoding metadata, selections, limits, aggregates, and output
offset validation. Focused Rust source-derived DAG tests and the live TiKV
read path exercise that owner.

The Go package also owns a general physical-plan `ToPB` tree for TiFlash and
arbitrary list plans, plus the index-merge non-natural parent-index rewrite.
This Rust workspace has no TiFlash coprocessor/MPP DAG transport or
dependency-closed physical-plan-to-TiPB tree builder, and no Rust caller uses
the parent-index rewrite. Implementing either as an uncalled second planner
would invent behavior outside the current Rust execution path. The complete
Go package is therefore recorded as an explicit boundary; no Rust-only API or
behavior was changed in this batch.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No source was
changed, so `make bazel_prepare` and the Ready lint gate are not required for
this receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/executor/internal/builder
# passed; package has no test files and compiles cleanly
```

Not verified here: caller-level executor tests, Bazel execution, TiFlash
transport, and full workspace tests. Existing Rust warnings and unrelated
dirty `tidb-txnkv` files remain.
