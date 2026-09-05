# Server connection and lifecycle parity receipt

Date: 2026-09-05

## Scope

This receipt covers the Go server connection lifecycle, prepared-statement
limits, profiling request observability, advertised-status checker, and
connection-event sysvar. It also records the explicit-disconnect regression
that exercises the behavior against a real TiKV playground. The Rust server
now owns the dependency-closed prepared long-data quota seam; the remaining
profiling, advertised-status, and connection-event items stay explicit Go
ownership boundaries rather than partial Rust package-completion claims.

## Complete pre-edit inventory

The package trees were read before editing, including every production and test
file, build input, fixture/testdata, generated/platform variant, benchmark,
fuzz target, and support artifact. No omitted fixture or generated variant was
found.

| Go package / surface | Artifacts / lines | Inventory result |
| --- | ---: | --- |
| `pkg/server` (root package) | 28 / 13,104 baseline | all root production and tests, BUILD and support files read; nested packages inventoried separately |
| `pkg/server/internal/advertisedstatus` | 3 / 754 | checker production, focused test, BUILD; no fixture/generated/platform variant |
| `pkg/server/handler/tests` | 5 / 3,632 | complete HTTP test package and BUILD; no generated/platform variant |
| `pkg/server/tests/commontest` | 4 / 4,487 | complete common server suite, fixtures, BUILD/support; no generated variant |
| `pkg/server/tests/tls` | 3 / 883 | complete TLS suite and BUILD/support; no generated/platform variant |
| `pkg/sessionctx/vardef` | 7 / 2,961 | variable definitions, generated/platform inputs, tests and BUILD read |
| `pkg/sessionctx/variable` | 29 / 17,820 | production/session variable files, tests, BUILD and support inputs read |
| `pkg/sessionctx/variable/tests` | 7 / 2,658 | complete variable test package and BUILD/support read |
| `tests/realtikvtest/pessimistictest` | 4 / 4,305 | all tests, BUILD, real-TiKV support and fixtures read; no generated artifact |

For this Rust-owned batch, the corresponding owner walk covered
`tidb-server/src/mysql_connection.rs`, `sql_node.rs`,
`pipeline_session.rs`, `cluster_session_node/mod.rs`, and
`real_tikv_multi_node.rs`, plus `tidb-session/src/identity.rs` and its
`tests_core/lifecycle.rs` regression. Their production, test, generated-test
aggregation, and fixture paths were checked before editing; no platform
variant or build artifact owns the prepared long-data tracker seam.

## Restored behavior and regressions

- Connection liveness is installed for prepared, traced, explained, DDL-adjacent
  and multi-statement paths with lock-prefetch interruption propagated instead
  of suppressed.
- `COM_CHANGE_USER` keeps the old session and identity on every authentication
  failure and moves resource-group accounting only after a successful reset.
- `COM_STMT_SEND_LONG_DATA` enforces `max_allowed_packet`, charges and releases
  query-memory quota, and reports the deferred protocol error on execute. Rust
  mirrors the same sticky refusal and cleanup through
  `ConnectionPreparedStatement` and the session memory root; a loopback TCP
  regression covers silent SEND commands, deferred 8175, post-EXECUTE reuse,
  and CLOSE release.
- Profiling and debug-zip endpoints emit structured request logs; the tests
  assert route, query fields, method, and remote address.
- Added `tidb_enable_connection_event_log` and the login/logout event path.
- Wired `SessionVars.SQLKiller` as the TiKV kill-signal handler. The focused
  regression `TestStatementsInterruptedOnDisconnect` covers autocommit,
  prepared, explicit pessimistic transactions, autocommit-off, row-returning
  locks, and multi-statement prefetch. The former failure left the blocked
  session in processlist; the current test passes and verifies cleanup.

## Validation

Ready-profile checks passed with failpoint enable/disable wrappers:

- `tools/check/failpoint-go-test.sh ./pkg/server/internal/advertisedstatus -run '^Test' -count=1`
- `tools/check/failpoint-go-test.sh ./pkg/server -run '<focused connection and long-data tests>' -count=1`
- `tools/check/failpoint-go-test.sh ./pkg/server/tests/commontest -run '^TestClientDisconnectKillsExplicitTxn$' -count=1`
- `tools/check/failpoint-go-test.sh ./pkg/server/handler/tests -run '^(TestDebugRoutes|TestDebugZip)$' -count=1`
- `tools/check/failpoint-go-test.sh ./pkg/server/tests/tls -run '^TestTLSBasic$' -count=1`
- `tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable/tests -run '^TestSetSysVar$' -count=1`
- `cargo test --offline --locked -p tidb-server --test all long_data -- --nocapture`
- `cargo test --offline --locked -p tidb-session long_data_uses_live_query_quota_and_releases_session_bytes -- --nocapture`
- `make lint`
- `git diff --check`
- `cargo fmt --all -- --check`

The real-TiKV regression was also run with the required playground lifecycle:
`go test -run '^TestStatementsInterruptedOnDisconnect$' -tags=intest,deadlock
./tests/realtikvtest/pessimistictest/... -count=1`; the playground was stopped,
its exact data directory moved to a recoverable `/tmp/tidb-codex` path, and no
TiUP process remained. `make bazel_prepare` was attempted with the required Go
and GOPATH environment and is blocked because `bazel` is not installed.

## Risks and ownership

The liveness changes affect cancellation timing for long-running SQL and
pessimistic locks; prepared-statement quota accounting affects memory pressure;
and the advertised-status checker depends on status-listener lifecycle. The
focused suites cover these paths, while Bazel graph validation remains a local
environment gap. Rust's prepared long-data implementation is intentionally
limited to the existing session-tracker and wire command owners; no profiling
or status-listener replacement is integrated or claimed.
