# `pkg/infoschema/perfschema` parity receipt

Status: Completed the missing-Go-behavior restoration and recorded the
complete package inventory. This receipt covers the virtual performance-schema
package; it is not a repository-wide parity claim.

Comparison source: Go `origin/master` at
`a85e0fd5dfa914e73eed97f17af584061252bc3c` (`executor: account statement RU for joins and aggregations
(#70682)`).
Rust comparison branch: `origin/hparser-integration` at the pre-fix commit.

## Complete Go inventory

Before editing, every tracked artifact was read or decoded in full: eight
artifacts and 1,597 counted lines in `pkg/infoschema/perfschema` (1,423 text
lines plus the two binary fixture payloads as reported by `wc -l`), including
two binary profile fixtures. There is no package `doc.go`, generated source,
platform variant, benchmark, fuzz target, README, or ownership artifact.

| artifact | lines/size | role |
| --- | ---: | --- |
| `BUILD.bazel` | 64 lines | virtual-table library/test target, fixtures, and dependencies |
| `const.go` | 581 lines | performance-schema table definitions and IDs |
| `init.go` | 81 lines | virtual-table registration/bootstrap |
| `tables.go` | 452 lines | virtual table implementation and remote profile fetch |
| `main_test.go` | 34 lines | common test setup/goleak harness |
| `tables_test.go` | 211 lines | virtual table and profile HTTP tests |
| `testdata/test.pprof` | 1,206 bytes gzip | PD CPU profile fixture (decoded with `go tool pprof`) |
| `testdata/tikv.cpu.profile` | 23,928 bytes | TiKV CPU profile fixture (binary profile payload) |

## Go-master synchronization

The earlier package batch restored `logTiDBProfileRequest`, which logs each
local performance-schema profile request with table, connection, user, and
client IP context. That production helper and its `logutil`/zap dependencies
remain present in current Go master. Go master no longer carries the global
logger-replacement regression `TestTiDBProfileRequestLog`; this batch removes
that stale/flaky test and its test-only `pingcap/log`, zap, and zapcore
dependencies from the Bazel target. Existing profile queries continue to call
the logger for all six TiDB profile tables; remote TiKV/PD profile paths retain
their existing behavior.

No Rust-only behavior or Rust source owner is changed by this synchronization;
the package remains Go-owned and the production logging contract is covered by
the upstream implementation.

## Rust ownership and boundary

No Rust crate owns the Go performance-schema virtual table registry, profile
HTTP fan-out, pprof parsing, session-variable views, or warning semantics.
Rust's diagnostics and executor metadata do not provide a dependency-closed
performance-schema owner. No Rust-only substitute was introduced.

## Validation

Profile: **Ready** for this restoration batch.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/infoschema/perfschema -tags=intest -count=1 -vet=off` — passed in the detached Go-master worktree; the package contains the `mockRemoteNodeStatusAddress` failpoint and the canonical wrapper enabled/disabled it.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make bazel_prepare` — attempted because production imports and BUILD dependencies were restored; unavailable locally (`bazel: No such file or directory`).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — repository Ready gate passed.
- `git diff --check` — passed.

## Risks and unverified scope

- Correctness risk is reduced: the package test/build surface now matches Go
  master without changing query results, production logging, or remote
  fetches.
- Compatibility risk is limited to restoring logging fields and dependencies;
  no SQL-visible schema or profile format changed.
- Performance impact is the original structured log allocation per local
  profile request, as in Go master; removing the test has no runtime impact.
- Not verified locally: Bazel generation, live PD/TiKV pprof endpoints, non-host
  platforms, and the full infoschema integration suite.

The rolling repository audit continues with the next unclaimed package.
