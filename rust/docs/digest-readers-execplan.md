# Share diagnostic SQL-digest retrieval

## Purpose / Big Picture


Transactions, deadlock history and lock waits must resolve normalized SQL through
the selected current/history statement-summary owner. Go uses local retrieval for
transactions and local-first cluster fallback for deadlocks and lock waits. Replace
the three direct v1-map lookups together; preserve visibility, errors and shared
peer transport. This repairs consumers of I01/I03/O18, not complete Go packages.

## Progress


- [x] Read current batch map and fetch Go master1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d from baselinebb8b09a2af.
- [x] Capture three baseline failures: all readers return NULL for v2-owned text.
- [x] Share local/history retrieval and missing-digest cluster fallback; migrate all three readers and remove the stale map helper.
- [x] Validate21 grouped Rust cases, production v1/v2 SQL/peer behavior, affected checks and make lint.
- [ ] Run actual commit hook, fresh locked pre-push build, normal push and checkpoint readback.

## Context and Orientation


Go expression/util.go SQLDigestTextRetriever reads both current and historical
summary tables, querying peers only for unknown digests. Executor
infoschema_reader.go selects local versus global policy and propagates errors.
Rust session dispatch.rs and process_arm.rs currently read only
STMT_SUMMARY_BY_DIGEST_MAP.normalized_sql_for_digest, ignoring persistent v2 and
remote summaries. Reuse statement_summary_table_rows and cluster_table_rows
owners, with internal privilege semantics and projected digest columns. Receiving
deadlock requests need the existing outgoing peer client as well as fresh metadata.

## Milestones and Concrete Steps


First reproduce all three persistent-owner failures in one server-lib filter.
Next share selected-summary retrieval, then missing-only peer fallback through
the existing cancellation/discovery/error boundary. Preserve the caller's identity
and statement state; internal lookup may inspect all summaries but the outer
transaction/deadlock/lock-wait visibility gates remain authoritative. Migrate every
direct caller before deleting the map-only helper. Test local hits, persistent and
history data, remote fallback, unknown digests and error propagation together.

Source /workspace/.cloud-setup/env.sh, export CARGO_BUILD_JOBS=1 and run Cargo from
/workspace/tidb/rust. Run cargo test --locked -p tidb-server --lib
digest_readers_batch -- --test-threads=1 before and after. Run retained cluster
diagnostic/summary filters, affected all-target checks and make lint from the root.
The executable precommit hook must pass cargo build --locked -p tidb-server;
repeat that command immediately before normal push and verify the remote SHA.

## Surprises & Discoveries


The first production probe used an incorrect unquoted function-name prefix; observing the actual canonical summary text fixed the probe without changing source. Both modes then passed.

The outgoing scan previously requested every column. Digest lookup should project
only DIGEST and DIGEST_TEXT through that same transport, rather than introduce
another RPC client or collect irrelevant summary payloads.

## Decision Log


Use shared summary readers with explicit internal lookup semantics and preserve
outer admission. No synthetic client registration, second summary owner or new
runtime. Date/author:2026-10-08,Codex.

## Outcomes & Retrospective


All three baseline failures are repaired. Selected v1/v2 current/history lookup, local-first cluster fallback, original column-ID projection and admission/error preservation pass21 grouped Rust cases and 16 production assertions. All-target checks, make lint and locked production build pass. Parent findings remain partial and54 unresolved roots retain their broader obligations.

## Idempotence and Recovery


Preserve concurrent changes, exact destinations and native checkout. Do not force
push or bypass hooks. Evidence lives in /workspace/.cloud-setup/digest-readers-batch.
Retire only completed owned binaries with hashes/process checks when space requires
it; retain dependency caches and the final server. Keep env.sh's32MiB Rust stack.

## Interfaces and Dependencies


Reuse Session, summary v1/v2 readers, ProcessRegistry and ClusterPeerClient. Project
wire columns using real schema IDs; preserve source/result encodings. No generated
protocol edits or dependency changes are required.

Final publication/checkpoint evidence is recorded externally in /workspace/.cloud-setup/digest-readers-batch/final-handoff.json after normal commit/push. See parity/current-audit/digest-readers-validation.json for commands, source/log hashes and limits.
