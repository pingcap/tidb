# `pkg/infoschema/perfschema` parity receipt

Status: Completed the missing-Go-behavior restoration and recorded the
complete package inventory. This receipt covers the virtual performance-schema
package; it is not a repository-wide parity claim.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust comparison branch: `origin/hparser-integration` at the pre-fix commit.

## Complete Go inventory

Before editing, every tracked artifact was read or decoded in full: eight
artifacts and 1,572 lines in `pkg/infoschema/perfschema`, including two binary
profile fixtures. There is no package `doc.go`, generated source, platform
variant, benchmark, fuzz target, README, or ownership artifact.

| artifact | lines/size | role |
| --- | ---: | --- |
| `BUILD.bazel` | 62 lines | virtual-table library/test target, fixtures, and dependencies |
| `const.go` | 581 lines | performance-schema table definitions and IDs |
| `init.go` | 81 lines | virtual-table registration/bootstrap |
| `tables.go` | 429 lines | virtual table implementation and remote profile fetch |
| `main_test.go` | 34 lines | common test setup/goleak harness |
| `tables_test.go` | 211 lines | virtual table and profile HTTP tests |
| `testdata/test.pprof` | 1,872 bytes gzip | PD CPU profile fixture (decoded with `go tool pprof`) |
| `testdata/tikv.cpu.profile` | 4,096 bytes | TiKV CPU profile fixture (binary profile payload) |

## Missing-Go behavior restored

The branch had removed `logTiDBProfileRequest`, which logs each local
performance-schema profile request with table, connection, user, and client IP
context. It also removed the `pkg/util/logutil` and zap BUILD dependencies.
The exact Go-master helper and dependencies were restored. Existing profile
queries continue to call the logger for all six TiDB profile tables; remote
TiKV/PD profile paths retain their existing behavior.

The focused regression `TestTiDBProfileRequestLog` replaces the global zap
logger, queries `tidb_profile_goroutines`, and asserts the restored message and
performance-schema table field.

## Rust ownership and boundary

No Rust crate owns the Go performance-schema virtual table registry, profile
HTTP fan-out, pprof parsing, session-variable views, or warning semantics.
Rust's diagnostics and executor metadata do not provide a dependency-closed
performance-schema owner. No Rust-only substitute was introduced.

## Validation

Profile: **Ready** for this restoration batch.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/infoschema/perfschema -count=1` — passed; the package contains the `mockRemoteNodeStatusAddress` failpoint and the canonical wrapper enabled/disabled it.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make bazel_prepare` — attempted because production imports and BUILD dependencies were restored; unavailable locally (`bazel: No such file or directory`).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate (run after this batch is staged).
- `git diff --check` — passed after this batch is staged.

## Risks and unverified scope

- Correctness risk is reduced: profile-request observability now matches Go
  master without changing query results or remote fetches.
- Compatibility risk is limited to restoring logging fields and dependencies;
  no SQL-visible schema or profile format changed.
- Performance impact is the original structured log allocation per local
  profile request, as in Go master.
- Not verified locally: Bazel generation, live PD/TiKV pprof endpoints, non-host
  platforms, and the full infoschema integration suite.

The rolling repository audit continues with the next unclaimed package.
