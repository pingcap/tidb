# Share DDL error conversion across execution and history

This living ExecPlan follows root PLANS.md and continues ddl-panic-owner-execplan.md.

## Purpose and scope


The same existing DDL refusal must retain its MySQL code, SQLSTATE and message
whether returned directly or saved in a durable job and read after owner restart.
Today sql_node.rs owns the direct error-code map, while cluster_ddl.rs stores
every non-admission error as 1105 and the history waiter forces HY000. Move this
policy beside DdlPlanError and use the shared MySQL SQLSTATE catalog everywhere.
Plain action errors must use Go toTError's DDL/CodeUnknown durable representation.

Starting integration is 5d0ee83ced20a1cee3d8f0c9bb99dda274c785e9. Fresh fetch
confirms Go master 93a01d31f6da205ae4bf376825293903a6899fdb and unchanged integration;
client-rust stays 6163ecfc. Source owners reviewed are pkg/ddl/job_worker.go
(countForError, toTError), pkg/parser/terror, pkg/parser/mysql/state.go,
pkg/infoschema/error.go and pkg/util/dbterror. This is maintenance of existing
live paths, not upstream package acceptance or activation of disabled actions.

## Progress


- [x] Fetch current refs and trace direct, durable and history error conversion.
- [x] Reproduce lost durable codes, history SQLSTATE and plain-error identity before behavioral edits.
- [x] Remove the server-owned DdlPlanError map and share the existing contract.
- [x] Verify all 21 variants through checkpoint/history, plus cancellation, panic and direct SQL callers (139 scoped tests).
- [x] Run all-target check, lint and diff review; verify audit row/count consistency.
- [x] Commit the reviewed implementation through the actual locked-build hook and verify a fresh post-commit server build. Final receipt publication repeats the gates below.

## Implementation and validation


Extend tidb-exec/tests/cluster_ddl_source.rs using its existing bootstrapped queue
fixture. Exercise the original-snapshot failure checkpoint with all coded plan
variants and verify counts, messages and history serialization. Plain encoding,
catalog and mutation failures retain unknown-error SQL behavior, but persist the
source DDL/CodeUnknown identity. Do not infer error classes from message strings.

Extract the existing history error adapter into a testable helper in
tidb-server/src/cluster_session_node/ddl.rs without changing behavior, then prove
its forced HY000 fails a catalog SQLSTATE regression. Centralize DdlPlanError's
code selection in tidb-exec/src/cluster_ddl.rs, delete the parallel match from
tidb-server/src/sql_node.rs, and route history conversion through the same shared
SQLSTATE catalog. Keep existing messages and the legacy coded-admission envelope.

The full error-identity migration remains open: DdlAdmissionError and several
storage/expression boundaries currently retain only numeric codes or strings.
Do not fabricate RFC classes for those values or claim full toTError parity.
The existing legacy job-error numeric representation must remain readable.
Correcting that source identity pipeline requires its complete production owners.
Likewise retry timing and rollback-transaction classification remain D05 work.

From rust/, run targeted red tests first, then:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::schema_changes
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From root run make lint and git diff --check. No Go/Bazel inputs change;
bazel_prepare and Go failpoints are not needed. Retain /private/tmp/tidb-ddl-error-*.log.

Commit via TERM=xterm git -c core.hooksPath=hooks commit so the real pre-commit
hook runs cd rust && cargo build --locked -p tidb-server. After the final commit
or amendment rerun that exact locked build, then git push origin HEAD:hparser-integration.
If the branch advances, inspect/rebase without force and repeat affected gates.
Compare git ls-remote with HEAD and check an empty git status --short before reporting.

## Surprises & Discoveries


The source rollbackTxnError currently originates in DROP schema/table external
notification paths. Those effects are absent from the live Rust persisted handlers;
adding a dormant marker alone would not repair their missing owner. The active
error-code split can be reproduced through the already-integrated failure checkpoint.

The scoped SQL suite's initial sandboxed run had 18 fixture setup failures at
node_fixture.rs:494 because sysctl hw.memsize was denied. Rerunning with that
host capability passed all 21 tests without code or expectation changes. This
is an environment failure, not a SQL behavior discrepancy.

Source errno alone cannot recover RFC identity: Go's duplicate-key-name error
can originate from ClassDDL or ClassSchema (index.go uses both). Retaining the
legacy numeric bridge is explicit compatibility, not full terror registry parity.

## Decision Log


On 2026-10-02 choose one DdlPlanError conversion owner for direct and persisted
execution, retaining the shared MySQL catalog for wire SQLSTATE. A server-only
SQLSTATE patch would leave durable error-code loss; assigning DDL class to every
admission error would invent identities for errors actually owned by schema,
table, expression and storage packages. Keep the remaining identity gap explicit.

## Outcomes & Retrospective


The server-specific plan-error switch and the job recorder's generic fallback
for coded variants are removed. One exhaustive enum mapping selects codes for
both direct execution and durable jobs; SQLSTATE always comes from the common
catalog. Plain action errors, rollback-limit diagnostics and panic-limit diagnostics
share the source DDL/CodeUnknown envelope. Existing legacy history remains readable.

Three pre-fix commands from rust/ failed as expected:

    cargo test --locked -p tidb-exec --test all persisted_errors_keep_the_same_codes_as_direct_ddl
    cargo test --locked -p tidb-server --lib persisted_history_preserves_source_sqlstate
    cargo test --locked -p tidb-exec --test all persisted_action_error_is_checkpointed_before_retry

The first observed 1105 instead of 1049, the second HY000 instead of 42000, and
the third class 0 instead of DDL class 2. See /private/tmp/tidb-ddl-error-before-
codes.log, before-state.log and before-unknown.log. The final scoped suites above
pass 106, seven, five and 21 cases respectively (139 tests, none ignored), including
all 21 DdlPlanError variants through durable queue/history and SQL projection.
Existing direct DDL diagnostics, undetermined transaction handling, worker pause,
owner loss, cancellation and panic regressions pass. All-target check and make lint
pass with existing warnings; lint includes five protobuf tests. The initial compile
caught isize::from(u16) being unavailable; the repaired conversion is checked.
Final logs use planner, transaction, worker, sql-unsandboxed, check and lint suffixes
under /private/tmp/tidb-ddl-error-*.log. No expected outputs were weakened.

Production files changed: rust/crates/tidb-exec/src/cluster_ddl.rs,
rust/crates/tidb-server/src/sql_node.rs and
rust/crates/tidb-server/src/cluster_session_node/ddl.rs. Tests extend the latter and
rust/crates/tidb-exec/tests/cluster_ddl_source.rs. This receipt, the full structural
plan and current-audit README/findings JSON/Markdown record the remaining scope.

D05 stays partial; counts remain 85 tracked, 75 unresolved (69 open, six partial),
ten repaired. Lost producer RFC/class metadata, complete source error registration,
retry and rollback-transaction policy remain open. Error conversion is outside the
successful statement hot path, but no performance improvement is measured. No
complete upstream package, Go original suite, mixed-cluster recovery or
sysbench/TPCC/TPCH/YCSB acceptance is claimed.

Publication evidence: TERM=xterm git -c core.hooksPath=hooks commit ran its
locked server build successfully (15.40 seconds), then the fresh root
cd rust && cargo build --locked -p tidb-server passed (15.00 seconds). Logs are
/private/tmp/tidb-ddl-error-commit.log and /private/tmp/tidb-ddl-error-prepush.log.
The receipt amendment uses the same real hook and must be followed by another
locked server build before the ordinary push. Final publication evidence belongs
in the final response after matching the remote SHA to HEAD and verifying an
empty worktree; local build success alone is not remote publication.

The first push was rejected because origin/hparser-integration advanced to
e7f6e670d1c4c34e105c737b388ced75f77d52ac (DDL warnings and catalog table-ID
allocation). Inspection found no overlapping edits; a clean rebase preserves
the incoming work. All four scoped suites (139 tests), all-target check and
make lint pass again on the combined tree. Logs use
/private/tmp/tidb-ddl-error-rebased-*.log. The final rebased amendment must run
the actual hook again and a fresh locked server build before an ordinary push;
no force push is used.

Revision note (2026-10-02): record the shared conversion owner, three red
reproductions, complete enum coverage and explicit identity/interoperability limits.
