# Remove captured cluster state from runtime information tables

This living ExecPlan follows root `PLANS.md`. Keep progress, decisions, evidence
and remaining owner requirements current. This withdraws fabricated runtime
answers; it does not implement or accept a partial Go package.

## Purpose and context


Cluster information must describe discovered servers, not a previously captured
test environment. Remove the unconditional tikv/store1 row, the compiled
CLUSTER_CONFIG capture and its fabricated missing-status-address warning. Keep
the existing TiDB server source and propagate its failures to SQL in both
CLUSTER_INFO and TIDB_SERVERS_INFO. Configuration queries will explicitly report
unsupported live retrieval until the real owner exists.

Starting integration is ba62b48ee1350bb7dd67942cd5acce5c1a6eba5e. Fetching master
and hparser-integration and fast-forwarding the latter reports already current.
Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. Native client-rust stays at
6163ecfc587b248dcbf0e30c1c9d905b4bc5a665 and is outside this removal.

Go master pkg/infoschema/tables.go::GetClusterServerInfo composes seven real
retrievers and returns discovery failures. pkg/executor/infoschema_reader.go
renders these records and independently propagates GetAllServerInfo errors for
TIDB_SERVERS_INFO. pkg/executor/memtable_reader.go::fetchClusterConfig discovers
and filters nodes, checks CONFIG privilege, requests live HTTP configuration,
flattens it and reports actual per-node failures. Mock facts are explicitly
injected through test providers/failpoints. These complete upstream packages,
including all production/build/generated/platform variants, original tests and
fixtures, remain the acceptance units; this removal accepts none of them.

## Progress


- [x] Refresh refs, compare Go master owners and trace runtime capture consumers.
- [x] Add SQL regressions using the existing recording EtcdOps fake.
- [x] Demonstrate all four regressions fail before removing production shortcuts.
- [x] Remove the complete captured configuration provider and mock topology row;
  preserve live records and propagate discovery failures through both callers.
- [x] Run scoped SQL and server integration tests, formatting and make lint;
  reproduce the two adjacent schema-suite failures on unchanged production.
- [x] Update I01/I02 evidence without closing missing live owner findings.
- [x] Pass the actual pre-commit locked server build and a fresh post-commit
  build. Repeat both for the final receipt amendment before publication; the
  final task result records the published commit and remote verification.

## Milestones and implementation


First add tests to tests_domain_serverinfo_syncer_source.rs: no identity, local
identity, empty remote registry, multiple peers, peer deletion, read errors and
recovery. Add configuration refusal coverage for plain, filtered, joined and
prepared reads. Run the cluster_metadata filter before modifying production.
The old implementation must fail by adding a fake node, swallowing both tables'
errors and returning captured configuration.

In tidb-session/src/lib.rs delete only the fake topology row and its bookkeeping.
Change both discovery-backed row providers to Result and propagate errors in
dispatch.rs. Do not change cluster fanout or invent missing non-TiDB sources.
Remove the configuration warning, infoschema.rs capture helper and include,
and the entire generated cluster_config_rows.rs capture. No tracked generator
or other consumer exists; do not rewrite generated values. Keep table schemas,
static metadata, original Go fixtures and audit reproductions. Replace runtime
admission with an explicit unsupported error. This is containment, not Go's
supported CLUSTER_CONFIG behavior.

Then run SQL tests and the existing server cluster_info_reports_this_node test.
Update the two finding rows in both Markdown and JSON; retain open status.
Review the diff, run lint and publish with both required locked server builds.

## Validation and publication


From rust/:

    cargo test --locked -p tidb-session --lib cluster_metadata
    cargo test --locked -p tidb-session --lib tests_domain_serverinfo_syncer_source
    cargo test --locked -p tidb-session --lib tests_system_schemas
    cargo test --locked -p tidb-server --lib cluster_info_reports_this_node

From root:

    rustfmt --check --edition 2021 rust/crates/tidb-session/src/tests_domain_serverinfo_syncer_source.rs
    git diff --check
    make lint

Commit using TERM=xterm git -c core.hooksPath=hooks commit; the actual hook must
pass cd rust && cargo build --locked -p tidb-server. Run that exact build again
after the final commit before git push origin HEAD:hparser-integration. No Go,
module or Bazel input changes require bazel_prepare or Go failpoint enablement.
Use /private/tmp/tidb-cluster-fixture-*.log for outputs. On failure retain evidence
and repair or report it; do not bypass hooks or weaken tests.

## Surprises & Discoveries


The existing server test already expects a single discovered TiDB node, while
runtime appends store1. Both nearby comments incorrectly describe five Go
retrievers and claim no invented peers. Master actually has seven retrievers.
The compiled capture has 263 lines of one test server's settings.

## Decision Log


Remove capture ownership instead of adjusting captured values or adding another
local-only configuration generator. No local generator can implement Go's
distributed HTTP retrieval contract. Keep legitimate discovered records and
their error channel; a failure must not appear as an empty healthy cluster.
User authorization explicitly includes removing Go-absent implementations.

## Outcomes & Retrospective


Removed the 263-line compiled configuration capture, its helper/include and
fabricated warning, plus the unconditional tikv/store1 row and its timestamp
bookkeeping. Both existing server-info table providers now carry discovery
failures to SQL. Table schemas, real TiDB registry records, original Go fixtures,
static definitions and native client/protocol dependencies remain unchanged.

All four new regressions fail before the production removal and pass afterward.
The existing syncer suite reports 7 passed and 2 previously ignored cases
(cross-keyspace wiring and stale DDL owner cleanup). The existing server-stack
cluster_info_reports_this_node regression passes. System schemas report
18 passed and 2 failed: the database-list assertion omits existing system schemas,
and the index-merge read assertion expects sorted rows without ORDER BY. Each
failure reproduces independently with all changed production files restored to
HEAD; the removal is then restored in a finally block. No tests were weakened,
deleted or newly ignored. This is not a passing complete system-schema suite.

Exact baseline diagnostics from rust/:

    cargo test --locked -p tidb-session --lib the_system_schema_is_listed_among_the_databases
    cargo test --locked -p tidb-session --lib tidb_index_usage_records_real_data_reads

Formatting of the changed provider block and test file, git diff --check and
root make lint pass. All 85 Markdown/JSON register rows remain synchronized;
77 unresolved statuses are retained. Logs are /private/tmp/tidb-cluster-fixture-
before.log, -after.log, -syncer.log, -schemas.log, -server.log, -lint.log and the
two -baseline-<test-name>.log files. The before log contains actual SQL failures;
configuration returned the fixed 127.0.0.1:15100 capture, both discovery failures
returned Rows([]), and cluster info appended store1 to the genuine node.

I01/I02 remain open. Rust now refuses CLUSTER_CONFIG reads that the old runtime
answered with another server's settings; Go supports live retrieval, so refusal
is containment, not parity. Six missing discovery sources, complete configuration
retrieval/filtering/privileges, version/identity rendering, redaction and lifecycle
still require complete upstream package acceptance. Live distributed HTTP,
mixed-node discovery, full Go suites and workload benchmarks were not run.
No workload performance improvement is claimed.


The actual commit hook passed the locked server build in 8.39 seconds; the fresh
post-commit build passed in 12.14 seconds. Outputs are -commit.log and
-prepush.log under the same /private/tmp/tidb-cluster-fixture prefix. The final
receipt amendment repeats the hook and fresh build, with -commit-final.log and
-prepush-final.log outputs, before pushing origin HEAD:hparser-integration.
Publication does not claim either full upstream package is transcreated.

Changed files: tidb-session/src/lib.rs, dispatch.rs, infoschema.rs and
tests_domain_serverinfo_syncer_source.rs; deleted tidb-session/src/cluster_config_rows.rs;
tidb-server/src/cluster_session_node/tests/unistore_cop.rs (corrected source-owner
comment only); this receipt and current-audit/structural-findings.json/.md.
Crate paths above are relative to rust/crates; receipt/audit paths are under
rust/docs/parity/current-audit and rust/docs as named earlier.
