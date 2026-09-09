# `pkg/dxf/framework/taskexecutor/execute` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package has exactly three tracked artifacts and 517 lines. The complete
production interface, test, and BUILD files were read in full before this
receipt. There are no fixtures, `testdata`, generated files, platform
variants, benchmarks, fuzz targets, generator inputs, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `e925c4192bc245d9c3a8867a587e7d54f3f8789b` | `22b57606f2be19a3921e1bc84fc69ebe4428cb194d8bb8df551d4c06b531f327` | Go library/test target |
| `interface.go` | 340 | `b7a8839e7c916bc63de71b3c91fd87afd6d82d38` | `ef93438a284bf08f3899daa118f2772d3ac1642b30e73ef3e30901f025ec4dde` | StepExecutor, summary, progress, collector, framework-info contracts |
| `interface_test.go` | 154 | `1b5689040f4d1f59a8059aad3edeba85e5c753c9` | `385716a7e2fffd0749760d437338395fa6727a00a40cd8b1c22f4b3a6a0ecec8` | speed-window source test |

The package declares 17 production functions and one top-level test. The
current checkout is byte-identical to the pinned Go master for all three
artifacts, so no code or focused regression was needed in this audit.

## Rust ownership and parity decision

Rust has no dependency-closed StepExecutor runtime or object-store metering
summary owner. The existing `tidb-dxf` value types do not replace this Go
interface contract; no Rust-only behavior or ignored test was found to remove
and no speculative Rust trait was introduced.

## Validation

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/taskexecutor/execute -count=1
# ok github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute 0.454s
```

This is a read-only parity receipt; no Go/Bazel/Rust files changed and no
`make bazel_prepare` or Ready lint gate is required for this package-only
no-op. Repository-level Ready gates are run for each subsequent code batch.
