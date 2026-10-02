# Restore server-owned command admission

This living ExecPlan follows `PLANS.md`. It repairs N02 in the existing server
owner, not transcreation or acceptance of complete `pkg/server` or `pkg/util`.

## Purpose and context


All connections to one server must share the configured number of command
permits. Go `pkg/server/conn.go::dispatch` obtains a token before switching on
the command and releases it after response handling. Authentication and idle
sockets consume no token. `Server.getToken` records acquisition wait in
microseconds, then `releaseToken` returns the token and decrements the gauge.
The starting Rust implementation had only a metrics guard, recording execution
duration as token wait. `NodeConfig` discarded the parsed token-limit flag and
refused its TOML field, although the effective SourceConfig already carried it.

Both freshly fetched implementation branches are current: TiDB integration
38b6db0197b1f564310d9e483e62783d182ff41e and client-rust master
19a56ccda1e128218cd33c69709038219aced9bc. Go master remains
93a01d31f6da205ae4bf376825293903a6899fdb. Neither pkg/server nor pkg/util has
a doc.go. Source references are server.go getToken/releaseToken/NewServer,
conn.go dispatch, util/tokenlimiter.go, config.go Load, and main.go overrideConfig.

## Progress


- [x] Pull both repositories and trace configuration and command ownership.
- [x] Reproduce missing admission, lost configuration and misplaced metrics.
- [x] Replace the metrics-only guard with shared command permits.
- [x] Validate successful/error/panic/protocol paths, configuration and shutdown; record broader baseline failures.
- [x] Update audit, run lint and prepare publication through the required hook/build/push gates below.

## Milestones and design


First extend existing server concurrency/configuration fixtures. Hold a query's
result stream open on one socket, authenticate another socket and issue PING.
With limit one, PING must wait until the query retires. The unchanged server
must fail this regression. Check the effective flag/file value and require the
token histogram to record acquisition before the command finishes.

Then store one bounded token channel in the shared per-server connection
authority (ConnectionTracker), which already accompanies every socket and
both warm/dedicated workers. Default direct-connection callers share the same
authority; ConcurrentSqlNode initializes it from its own effective SourceConfig,
not mutable process-global configuration. A borrowed RAII permit returns its
token on normal return, error or unwind. Keep the acquisition at the common
dispatch boundary for every opcode and retain the permit through response
streaming. Go's token Get has no separate cancellation or timeout policy;
do not add one. Record wait upon acquisition, not upon release.

Carry the flag into effective SourceConfig and allow the now-owned TOML field.
Reuse the main-flag override policy; do not add another NodeConfig value or
pretend N03's remaining whitelist and default-policy gaps are fixed. Source
file loading normalizes zero to 1000 and caps oversized values; an explicit
CLI override is applied afterwards, exactly as Go does.

Finally exercise different commands and connection exits, and prove independent
server instances do not share permits. Keep the native client unchanged. Update
N02 and the affected N03 wording without replacing historical review evidence.

## Validation and acceptance


From rust/ run targeted server library/config tests and the existing integration
targets with the new command_token filter, followed by affected concurrency,
panic, wire and shutdown tests. Record exact commands and results below. Use
bounded test gates and always release blocked work before asserting failures.
Run cargo check --locked -p tidb-server --all-targets, targeted rustfmt --check,
and root make lint / git diff --check. No Go/Bazel source edits are planned;
bazel_prepare is not triggered. If executing original Go package tests, use
the failpoint-aware repository runner. Isolated source-helper oracle runs must
be identified as such, not complete original-package validation.

Commit via TERM=xterm git -c core.hooksPath=hooks commit; its locked server build
must pass. After the final commit, from repository root run:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify HEAD against git ls-remote and require a clean working tree. No full
package acceptance, live cluster or workload-performance claim is implied.

## Surprises & Discoveries


The initial wire test used `serve_connections(2)`. Reaching that artificial
accept limit begins drain and cancels the intentionally blocked query, masking
admission. Switching to `run()` with explicit shutdown gave the decisive
unchanged-code failure: PING bypassed the limit. Initial configuration fixtures
also needed the existing cluster-session option; fixture errors are not counted
as failure-before-fix evidence.

The error-path test then exposed a retained socket after panic. WatcherStop had
an explicit stop method but no Drop, so unwinding detached its socket-peeking
thread. Replacing that manual cleanup with Drop joins the watcher on all exits.
Go Run's deferred Close owns this lifetime; Rust cleanup must survive unwind.

A separate source mismatch remains: Go Run attempts writeError before Close.
Rust's outer catch has already unwound the framed writer and cannot do that.
N06 records this ownership gap. The admission regression accepts an ERR followed
by EOF or the current EOF, but rejects a timeout; it does not establish complete
panic wire parity or lock in the missing error response.

The complete server library has partition/statistics/timezone failures, and the
aggregate integration process stopped during authentication before the command
loop. Stack sampling showed idle worker queues, not a command-token wait.
Unchanged-commit controls reproduced the same ten library failures and the
authentication hang. Separate controls also reproduced all seven terminating
integration failures and the native-auth fixture hang. Several older fixtures
reject the existing handshake SET NAMES call before their intended command;
others retain outdated grant quoting and configured-table limits. They remain
explicit validation limitations, without modifying the production handshake.
The shutdown fixture must admit two connections through its separate connection
count limit before testing their shared one-command limit.

## Decision Log


Use the existing shared connection authority and crossbeam bounded channel to
match Go's token channel. Per-worker limits would miss dedicated connections;
a process-global limiter would incorrectly couple independent server instances.
Do not conflate connection-count admission with command concurrency.

## Outcomes & Retrospective


The focused admission boundary passes after five failure-before-fix observations:
three library configuration/metric failures, the wire admission failure, and the
watcher panic-retirement failure. This repairs N02 in the existing owner. Adding
newly observed N06 leaves 72 unresolved (66 open, six partial), 14 repaired and
86 tracked; N03's token-limit refusal is now historical. The native dependency
is already current and unchanged. No accepted whole package, live cluster,
performance improvement or full-suite success is claimed.

Validation results and exact commands are recorded below and in the linked
machine-readable receipt. Publication must use the mandatory gates below.

## Recovery and interfaces


Tests and checks are repeatable. Retain test gates and timeouts so a failed
assertion does not strand worker threads. Do not reset unrelated changes or
force push. Keep admission internal to the server, with one owner shared by
all dispatch callers; no storage protocol or dependency change is required.

## Validation evidence

All Rust commands run from `rust/`, unless stated otherwise. Serial cases avoid
existing process-global configuration/metric interference. Local logs are under
`/private/tmp/tidb-command-token-*` and are supplemental, not durable receipts.

- Before production edits:
  `cargo test --locked -p tidb-server --lib command_token -- --test-threads=1`
  failed all three initial cases: observed histogram count 0 versus 1, rejected
  token-limit TOML, and effective flag 1000 versus 1.
- With the corrected wire fixture and all candidate production files temporarily
  replaced by HEAD, then restored byte for byte:
  `cargo test --locked -p tidb-server --test all command_token_is_shared -- --test-threads=1`
  failed because PING bypassed admission. The fixed query is explicitly released
  before failure assertions, so this is an observed wrong result, not a timeout.
- After admission implementation but before watcher Drop, the error/panic test
  timed out on the panic socket instead of reaching EOF. It passed after Drop.
- `cargo test --locked -p tidb-server --lib --test all command_token -- --test-threads=1`:
  four library and two integration cases passed. Both warm and dedicated worker
  cases hold admission through streaming and keep independent servers separate.
  Configuration cases cover file default/cap and explicit 1, 0, and signed -1
  overrides; the negative case inspects configuration without allocating tokens.
- `cargo check --locked -p tidb-server --all-targets`: passed.
- Root `make lint`: passed, including proto checks and five sync-script tests.
- Isolated unchanged master helper oracle, not original complete pkg/util tests:
  copy `git show origin/master:pkg/util/tokenlimiter.go` into a temporary Go
  module together with the checked-in
  `parity/current-audit/command-token-oracle_test.go.txt` as `tokenlimiter_test.go`;
  run `GOTOOLCHAIN=go1.25.14 go test -race -count=1 .`: passed. The helper contains
  no failpoint imports. No Go/Bazel input changed, so bazel_prepare is not needed.

Final targeted commands:

    cargo test --locked -p tidb-server --lib -- command_token node_config::tests main_flags::tests --test-threads=1
    cargo test --locked -p tidb-server --test all -- command_token forced_shutdown_cancels_an_inflight_com_query --test-threads=1

These pass 21 library/configuration cases and three wire/shutdown cases. The
library uses a shared test lock for its two histogram producers; the wire helper
now bounds authentication reads. Shutdown cancels the active result producer,
returns its permit and retires the waiting command and both connections.

The [machine-readable validation receipt](parity/current-audit/command-admission-validation.json)
records every affected-suite command, result and failed case. Additional isolated
integration groups pass 101 cases in total; seven fail, and two groups time out.
The seven failures all reproduce with unchanged production. The full library
command `cargo test --locked -p tidb-server --lib --test all -- --test-threads=1`
stops after 479 passed/10 failed; the unchanged library control passes 475 and
fails the same ten cases. Thus it does not reach its integration phase.

The separate aggregate command
`cargo test --locked -p tidb-server --test all -- --test-threads=1` hangs in
command_dispatch_exports_success_and_error_counters authentication. A stack
sample shows no token wait. The unchanged aggregate control times out at the
same point; the separate native-auth group also stalls on unchanged code.
No full integration-suite success is claimed. Controls replace only this
change's production/test files with HEAD and restore their exact candidate bytes
in a finally block. No unrelated work is reset.

Formatting and whitespace gates, from rust/ and repository root respectively:

    rustfmt --check --edition 2021 crates/tidb-server/src/main_flags.rs crates/tidb-server/src/mysql_connection.rs crates/tidb-server/src/node_config.rs crates/tidb-server/src/server_metrics.rs crates/tidb-server/src/sql_node.rs crates/tidb-server/tests/concurrent_mysql_sessions_source.rs
    git diff --check

Audit validation verifies every one of 86 Markdown rows against its JSON fields,
unique IDs and recomputed dispositions (66 open, six partial, 14 repaired).

Files changed: server main flags, node configuration, shared connection authority,
command dispatch/retirement and metric documentation; the existing concurrency
fixture; this receipt, the overall ExecPlan, audit register, Go helper oracle and
validation JSON. Client-rust and dependency/generated inputs are unchanged.

Remaining risks/limits: N03 configuration and N06 panic error delivery remain;
full original Go packages, live TiKV and sysbench/TPC-C/TPC-H/YCSB were not run.
Explicit CLI zero still blocks acquisition like Go; no alternate cancellation,
fairness or unlimited-limit policy is introduced. The actual hook build and fresh
post-commit locked build must succeed before publication; final commit and remote
verification are reported in the task's publishing response.

## Publication integration

The first push was rejected because hparser-integration advanced to
`f5ec06278ecc4a12891a6f9902b5bcea9e1073ce` during validation. Its SET-assignment
binding fix touches cluster_session.rs and tidb-session variables.rs, with no
file overlap. The unpublished repair rebased cleanly; git range-diff confirms
the repair patch is unchanged. The 21 library/configuration cases and three
wire/shutdown cases pass again after rebase, and the all-target/lint gates
pass again for the integrated state. Broad suite/control results above refer to
the original 38b6db baseline; they were not reclassified as final full-suite
acceptance after rebase.

The initial actual pre-commit hook successfully ran the locked server build.
Its optional goword executable was missing; no spelling-check success is
claimed. The hook's gofmt check and required locked build succeeded. Updating
this receipt uses the hook again, followed by a fresh locked server build
immediately before the final non-force push.
