# `pkg/dxf/importinto/jobhistory` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly three tracked artifacts and 408 lines. Every file
was read in full in a detached worktree at the pinned Go commit before this
receipt was written. There is no `doc.go`, `OWNERS`, fixture, benchmark,
generated source or generator input, platform-specific variant, or other
checked-in artifact.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 30 | `9dfeb91e207dc0e6bd4e808f2f25c3cbd1d4cc44` | `55777911ebcfe019e4b86f2f14300701fa82c949d1ff2ec5d52fbd5725f7945a` | public library and flaky history test target with DXF metadata, task-key, and failpoint dependencies |
| `history.go` | 257 | `6e7c8c39e365715c59ebd57128ad66e817c0f681` | `1e59bef53c3133f02cbc5b0399a7a22a9ecb4f206451eaafa049fde3856b5984` | `Info`/`Duration` JSON contract, history-table SQL queries, step aggregation, and byte/rate/duration formatters |
| `history_test.go` | 121 | `1f8eed7decf0896cf8b374d52ed9a19df11b7b37` | `d80e7b6d40783bde8003c37e190ec83085528beb4b8e2a8476af5a763e06591d` | one end-to-end mock-store test covering task-history lookup, large IDs, metadata extraction, step grouping, rates, durations, and missing-job errors |

The production inventory contains all five functions: `GetFromHistory`,
`formatDuration`, `formatBytes`, `formatBytesPerHour`, and
`formatBytesPerCoreHour` (the latter four are formatting helpers around one
exported query entry point). The query reads the history task by a
mode-sensitive ImportInto key, derives plan counts and file/row sizes, groups
subtask history by step and KV group, clamps negative elapsed times, and
formats throughput using Docker's `go-units`. The test deliberately allocates
task IDs above 2^53 to keep integer precision visible and verifies the exact
JSON-derived values and duration fields.

## Rust ownership and parity decision

Rust has system-table definitions for global-task and background-subtask
history and adjacent `tidb-dxf` task/step types, but no dependency-closed
ImportInto history API. There is no owner that combines mode-sensitive task-key
construction, SQL execution over the history tables, JSON plan/summary
decoding, step/KV aggregation, Docker-compatible byte/rate formatting, and
the user-facing `Info` JSON contract. The existing DDL-history and statistics
history owners are separate domains and do not consume these rows.

No Rust-only history implementation or ignored test was found to remove.
Adding only the data struct or formatter would leave the query and task
metadata semantics disconnected, so no speculative facade or partial port was
added. The complete Go package remains an explicit integration boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passed with failpoints enabled and disabled by the
repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/importinto/jobhistory -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dxf/importinto/jobhistory 2.181s
```

The no-test compile probe also passed in 1.482s. Repository formatting, lint,
and diff hygiene are run for this receipt batch (`cargo
+nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`). No Go source, import section, test, Bazel
target, or module dependency changed, so `make bazel_prepare` is not required.
Rust tests and a full workspace build are not run because no Rust source or
owning target changed. SQL history schema compatibility, task-key mode
behavior, JSON shape, large-ID precision, step/KV grouping, and throughput
formatting remain unverified on the Rust side; the receipt records those risks
without claiming parity.
