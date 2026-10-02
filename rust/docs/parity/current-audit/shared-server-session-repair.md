# Shared server session and catalog ownership

Reviewed 2026-10-02 from integration
`44be2a9d756cc6e3003ab87ce0fd38a5d5e6348f`, freshly fetched Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`. Native client and dependency remain
`19a56ccda1e128218cd33c69709038219aced9bc`.

**S01 and S02 are repaired together. The register now has 67 unresolved findings
(59 open, eight partial), 19 repaired, and 86 tracked.** Other findings retain
their earlier evidence. This is removal of competing runtime owners, not complete
acceptance of cmd/tidb-server, pkg/server, session, planner or executor packages.
Their complete upstream artifact/test obligations remain in the package inventory.

## Shared cause and replacement

Go chooses a storage driver, bootstraps its Domain/session, and serves ordinary
sessions through TiDBDriver. Master `cmd/tidb-server/main.go` calls
`BootstrapSessionWithExternalWorkloadManager` and `server.NewTiDBDriver`;
`pkg/server/driver_tidb.go::OpenCtx` initializes each session, including its
handshake collation. Table count does not select another compiler or transaction
manager.

Rust previously selected a one-table or two-table server factory by static or
loaded descriptors. The latter dropped the loaded catalog and never installed
its schema/statistics reloaders. Both stores now use ClusterSessionFactory and
the ordinary Session over their existing storage adapters. The common catalog,
schema validator and reload lifecycle is preserved. No additional reloader,
planner or transaction engine was added.

Removed production owners and configuration:

- RealTiKvSessionFactory/RealTiKvServerSession and the complete two-table
  real_tikv_multi_node module, including its dispatcher and public entrypoints.
- LoadedCatalogAuthority and table-count dispatch, plus unistore's alternate
  factory and startup branch.
- The private query-observability wrapper, decoded transaction overlay, and
  restricted session SET dispatcher. Shared account/sysvar/schema/statistics
  bootstrap and ordered shutdown helpers remain.
- Static table descriptors, their parser/validation, and the private TopN cap.
  `--read-table`, `--load-table` and `--max-topn-rows` now fail explicitly.
  `--cluster-session` is a compatibility spelling with no configuration state
  or alternate execution path.

T01 remains open: generic BufferMutation::insert still chooses assertion policy,
including system-row callers. Its configured-server consumer is gone; deleting
that consumer does not prove correctness of the remaining table/index/system-row
contracts. Lower configured-plan APIs retained by standalone fixtures and wire
test templates are seed/support artifacts, not another selectable SQL server.

## Tests and script migration

The two startup regressions failed before production changes: ordinary explicit
authentication required `--read-table`, and static descriptors were accepted.
Both pass after removal, for TiKV and unistore configuration. A compatibility
test verifies the legacy mode spelling leaves the effective configuration equal.

The old lightweight activation test now creates its table with SQL on the shared
embedded store. It retains inherited globals, session SET, native commit protocol,
assertion validation and transaction-option lifetime checks. Shared SQL coverage
adds three-table joins, bounded/empty ranges, ordered LIMIT, autocommit locking
reads, staged insert/update/delete visibility and rollback. Prepared schema tests
now create three additional tables before a peer session adds/drops columns;
both point and range statements replan correctly.

Retired tests asserted obsolete descriptor grammars, static table-count refusal,
private result shaping, or the removed decoded overlay. Their SQL behavior is
covered through ordinary execution and existing wire tests; no original Go test
or fixture was deleted. Two stale wire fixtures failed on the existing handshake
SET NAMES initialization. They now explicitly accept/assert that initialization;
their client command, protocol and cleanup expectations remain.

Active catalog-load, DDL, DDL-notify, multi-statement transaction and prepared-write
runners start the same server lifecycle. Catalog identity/type checks query
information_schema rather than private readiness descriptors. JSON DDL/read
checks now compare ordinary shared-session results with Go. The convergence
runner includes migrated range, join, ordering and locking SQL comparisons.

The retired lib/live-sql-node-harness.sh and its six bigint-selection,
clustered-pk-range, prepared-point-read, topology-churn, ordered-join and
multi-relation-join runners required the deleted private engine telemetry and
planner restrictions. Three older read-only/multicolumn/concurrent-auth runners
also used that telemetry and even older, already-invalid descriptor flags;
they are retired in the same sweep. The script README maps active SQL/storage checks. Their
combined persistent-connection/leader-transfer/blocked-shutdown campaign has
**not** been re-established on the shared server. Historical receipts cannot be
treated as current validation. Scripts remain live-cluster gates, not claimed
passes from syntax checks.

## Validation

Exact commands, outcomes and retained diagnostics are in
[shared-server-session-validation.json](shared-server-session-validation.json).
From `rust/` (the workspace config supplies the 32 MiB debug test stack):

```sh
cargo test --locked -p tidb-server --test all -- node_config_source:: mysql_client_lifecycle_source:: concurrent_mysql_sessions_source:: --test-threads=1 --nocapture
cargo test --locked -p tidb-server --lib node_config::tests -- --test-threads=1
cargo test --locked -p tidb-server --lib real_tikv_node:: -- --test-threads=1 --nocapture
cargo test --locked -p tidb-server --lib cluster_session_node::schema_sync::tests -- --test-threads=1 --nocapture
cargo test --locked -p tidb-server --lib session_activation -- --test-threads=1 --nocapture
cargo test --locked -p tidb-server --lib shared_session_ -- --test-threads=1 --nocapture
cargo test --locked -p tidb-server --lib replans_after_another_session -- --test-threads=1 --nocapture
cargo test --locked -p tidb-server --test all panic_recovery_source:: -- --test-threads=1 --nocapture
cargo check --locked -p tidb-server --all-targets
```

These pass 97 distinct Rust tests; overlapping focused runs are not added twice.
A real unistore executable also passed SQL through stock mysql, including normal
startup without table/mode flags, three-table join, prepare/execute, transaction
read-your-writes/rollback and autocommit FOR UPDATE. SIGTERM joined shutdown with
exit zero. The validation JSON retains its exact startup arguments, SQL and rows.

From the repository root:

```sh
bash rust/scripts/test-ddl-json-fixture.sh
bash rust/scripts/test-ddl-change-count.sh
GOTOOLCHAIN=go1.25.14 make lint
git diff --check
```

All changed shell scripts passed `bash -n`. No Go, Bazel, generated protocol or
dependency inputs changed; bazel_prepare and Go failpoint mutation are unnecessary.
The architecture index update only describes ownership and introduces no policy;
its retained paths and reference consistency were checked against the agent docs
review guide.

Initial diagnostics are retained: concurrently running startup tests raced over
process-global resources; serial startup checks pass. An initial test invocation
from the repository root omitted rust/.cargo's required debug stack configuration.
An initial new test bypassed the wire transaction-control entrypoint for BEGIN;
it was corrected to use the same control_transaction path as the connection.
The unfiltered integration run was stopped at the old authentication fixture;
the affected complete wire/concurrency groups pass after its explicit correction.
No full-workspace or full unfiltered integration-suite pass is claimed.

## Publication and limits

Code commit `b4876275dc892a8897b95fbf6e6acbc7e3e499fe` is published to
hparser-integration and was verified against the exact remote SHA with a clean
worktree. The actual hooks/pre-commit locked server build passed in 7.95s; a fresh
locked server build immediately before push passed in 13.04s. This final receipt
update repeats both gates; its branch-head verification is recorded in the task
thread. No hook is bypassed.

Live TiKV peer-DDL, leader movement, combined blocked shutdown, Go race suites and
sysbench/TPC-C/TPC-H/YCSB performance comparisons were not run in this batch.
Removing the extra SQL engines improves ownership and removes their restrictions;
it is not a measured benchmark improvement. Existing shared-runtime findings,
including account/configuration gaps and T01, remain explicit in the register.
