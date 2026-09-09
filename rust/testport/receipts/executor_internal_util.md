# `pkg/executor/internal/util` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 161 lines. Every production
source file and Bazel target was read line by line. There are no package test
files, fixtures, generated files, benchmarks, fuzz targets, or platform
variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `66053518c629c4297e91650bc5444b94a816a725` | `fcb65c75fd00cf128b47260c1e9778bcdadf501e89339b7423c315569bf140ae` | internal utility library target and dependencies |
| `partition_table.go` | 78 | `cd6220feb43375d7de1b18d5ef31eb9adcd76dee` | `62c8cca2e5a748ff86bfda2ff13aa7e60873d836610c6d33464e538b785f5ea1` | recursive TiPB executor physical-table/partition ID rewrite |
| `util.go` | 65 | `776aa2395a24107689af72c4f1557b376f8b00b6` | `c80df067810a376fdfada03b3a96c0cc7f68d22c8ce56acf67153b7b49c892b4` | random test strings, caller-name lookup, spill-file leak assertion |

`UpdateExecutorTableID` handles table/index/partition scans and recursively
walks Selection, Aggregation, TopN, Limit, exchange, CTE, Join, Projection,
Window, Sort, and Expand children; unknown executor types return a traced
error. It also exposes a test-only context hook for the next partition update.
`util.go` is test support used by spill and join suites: it generates
alphanumeric strings, obtains the calling function's basename, and verifies
temporary-storage files do not leak a test prefix. `BUILD.bazel` marks the
library internal to executor subpackages.

## Rust ownership and explicit boundary

Rust has no single dependency-closed utility owner for this package. Physical
table IDs are selected before request construction in `tidb-exec`'s
`real_tikv_read` and `tidb-distsql` request builder, while partition routing
and scan metadata live in `tidb-executor`. Requests are built per physical
table, so there is no Rust caller that mutates an already-built recursive
TiPB executor tree like Go's `UpdateExecutorTableID`. The TiFlash MPP path
that invokes the helper in Go is not a Rust transport surface.

The random-string, runtime-caller, and filesystem-leak helpers are Go test
scaffolding rather than SQL behavior; Rust tests use local fixtures and direct
assertions instead. Adding a public Rust utility or a recursive protobuf
rewriter would be an uncalled compatibility layer. The complete inventory is
therefore recorded as an explicit boundary with no Rust-only behavior removed
and no speculative implementation added.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No source was
changed, so `make bazel_prepare` and the Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/executor/internal/util
# passed; package has no test files and compiles cleanly
```

Not verified here: spill/join caller tests, TiFlash MPP behavior, Bazel
execution, and full workspace tests. Existing Rust warnings and unrelated
dirty `tidb-txnkv` files remain.
