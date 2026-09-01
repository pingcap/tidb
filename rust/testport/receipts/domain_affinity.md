# `pkg/domain/affinity` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly four tracked artifacts and 706 lines: two
production files, one test file, and one Bazel build file. All artifacts were
read in full before this receipt. There is no `doc.go`, fixture directory,
`testdata`, generated source/input, platform variant, benchmark, fuzz target,
or `OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 34 | `2cdc315056ade396e7576ac8e9dae17478312415` | `569bda174ac72e5245792e969d52ff8e82525ca3fab4bdfb9b9126d1f2ec2967` | public affinity library and 11-shard flaky test target |
| `interface.go` | 115 | `8bfe5d1a5ed9fb94d64fca8982e53079a113770e` | `395dfebbe9d9073b3a8880442f29dc0c3c3c85f6c3041e8856f4f8e650a73f30` | package-level affinity operations, retry policy, and PD-client test seam |
| `manager.go` | 301 | `52bec846aa939fbc1dc2b94ca4a854cedd17e786` | `1feec5dc9dd643ee27ae1a11981f46cced0f8be9e6664c4b6468721322a34864` | PD manager, compatibility fallbacks, bounded query selection, and mock manager |
| `manager_test.go` | 256 | `f4a9d81b0008da4dcf0a1ebc429902e5819455ba` | `4d0228fddb86963ca25050ad3ae842d950943dfb8b50882cb0e209d3beb7bca1` | PD API fallback, filtering, query bounds, status parsing, and mock behavior tests |

The production inventory contains 33 declarations; the test inventory has 11
top-level tests. Current files are byte-identical to all pinned Go-master
artifacts. The package has no Rust behavior to align: its contracts are PD HTTP
affinity-group operations and TiDB-specific retry/fallback policy.

## Native integration decision

This package is Go-native domain infrastructure coupled to PD's HTTP client,
TiDB errno formatting, URL query limits, and package-global initialization.
Rust has no dependency-closed PD affinity manager, DDL integration, or owner
consumer. No Rust-only behavior was found to remove and no speculative Rust
facade was introduced.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete Go
package suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/domain/affinity -count=1
# ok github.com/pingcap/tidb/pkg/domain/affinity 0.444s
```

The shared Ready gates (`make lint`, Rust formatting, and `git diff --check`)
are run for this receipt batch. No Go or Bazel source changed, so
`make bazel_prepare` is not required.

## Outcome

The complete affinity package inventory and explicit Go-only ownership boundary
are recorded here; the rolling audit continues.
