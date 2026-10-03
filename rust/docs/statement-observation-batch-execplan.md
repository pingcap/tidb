# Connect statement observation owners to SQL and process lifetimes

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture

Completed SQL must reach the existing summary owner and per-session TopSQL counters. Persistent summaries must initialize a usable sink before admitting SQL, fall back to memory on initialization failure, and flush during shutdown. This batch advances O18, O11 and N03 together against Go master 93a01d31f6da205ae4bf376825293903a6899fdb; no whole-package transcreation or profiling transport completion is claimed. No pushes are authorized.

## Progress

- [x] Inspect current cloud branches, common session completion and Go initialization fallback.
- [x] Four behavioral regressions fail before repair; old server wire retrieval also fails and cumulative records are absent.
- [x] Connect the shared summary gate and observation scope, current/history readers, checked sink initialization/fallback and process cleanup.
- [x] Finish behavioral validation: 306 distinct passing Rust cases, six-crate all-target checking, make lint, locked server build and three real MySQL/unistore modes.
- [ ] Capture the actual normal source-commit precommit locked build and finalize local retention.
- [ ] Update both finding registers and durable receipts.

## Context and Orientation

The editable checkout is /workspace/tidb on hparser-integration; native client /workspace/client-rust remains unchanged. Source /workspace/.cloud-setup/env.sh before Cargo. tidb-session owns statement begin and finish, including record-set close. tidb-stmtsummary owns v1 memory and v2 persistent records and readers. tidb-util owns per-session TopSQL counters and their process aggregator. tidb-server::run_configured_node owns startup and shutdown for both stores.

## Milestones

First demonstrate missing real-session summary and counters and invalid persistent startup. Then feed the existing owners through common SQL completion, select existing readers and start/close process workers. Finally validate regressions and update evidence without marking absent profiling or cluster fanout repaired.

## Plan of Work

Keep observation state in the Session, consume it once at finish, and preserve Go internal/disabled/PREPARE/COMMIT rules. Reuse normalization and typed summary/statistics interfaces. Validate the persistent writer before publishing global ownership; disable persistent configuration on failure. Use process cleanup guards after connections have closed.

## Concrete Steps

From /workspace/tidb/rust after sourcing the cloud environment, run targeted cargo test --locked -p tidb-session --lib observation_batch and cargo test --locked -p tidb-stmtsummary --lib observation_batch. Run cargo check --locked --all-targets for touched crates. From the repository run make lint and commit normally, allowing hooks/pre-commit to run cargo build --locked -p tidb-server.

## Validation and Acceptance

A real authenticated Session executing SQL produces summary rows and execution counters without tests injecting records. Disabled/internal rules suppress the correct records. An unusable log path returns an error, disables persistent mode and leaves usable memory aggregation. Successful startup and shutdown flush records. Tests must fail before the implementation and pass after it.

## Idempotence and Recovery

Preserve concurrent changes and exact remotes. Repeated tests use unique temporary files and restore global switches. Retain logs outside the checkout. Never bypass hooks or push.

## Interfaces and Dependencies

Reuse StmtExecInfo, StmtExecLazyInfo, StatementObserver, create_statement_stats and the global aggregator. Existing dependencies suffice. Unimplemented CPU profiling and reporter transport remain explicitly unresolved.

## Surprises & Discoveries

The v2 writer currently discards open errors and Setup leaves the persistent flag enabled on failure, unlike fresh Go. Server startup initializes metrics but never starts the statement aggregator. Fresh Go names IA_EXEC_COUNT differently from the old Rust receipt; selecting all 127 schema columns exposed that stale constant. Three existing schema/order assertions contradicted the current Go owners and have been corrected without discarding their behavioral checks.

## Decision Log

Reuse tidb-exec::adapter::decide_summary_stmt as the canonical gate. Use one SQL completion producer and existing summary/counter owners. Keep parent findings partial when broader contracts remain absent; do not replace missing measurements with claims of verified telemetry.

## Outcomes & Retrospective

The initial session/runtime regressions pass. Source self-review found nested account/local-DDL execution could publish before durability; the routed scope now preserves one outer completion and transaction-control attribution. Final runtime validation passes in memory, persistent and invalid-sink fallback modes, including text/named/binary prepared DML, prepared DDL attribution and all five live instance getters. No complete package acceptance is claimed. O18/O11/N03 stay partial; 58 unresolved remain. The synthetic cumulative fixture is replaced with real SQL and three stale schema/order assertions are corrected. Actual source-hook evidence and local-only retention are the final steps.
