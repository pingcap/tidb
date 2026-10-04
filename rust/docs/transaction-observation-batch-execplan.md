# Connect transaction history, live SQL rows and runtime controls


This living ExecPlan follows root PLANS.md. Cloud checkout: /workspace/tidb, hparser-integration, starting 0faf5c205ea7caf399b48bac482bb9b6e98f7696. Fresh Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb. Native client remains unchanged. Remote integration 86e41faa92c1a7bb244e6793d90d88c8ce49f44b is preserved unmerged. No push or dry run.

## Purpose / Big Picture


Complete the connected transaction-observation contracts under I01/N03/N05. A real transaction should leave a bounded digest-sequence summary when it ends, subject to the configured minimum duration. TRX_SUMMARY should expose those records only to PROCESS-authorized observers. HTTP settings and startup configuration should govern that same process recorder. Live TIDB_TRX must use storage's actual timestamp, and logical catalog operations must not end its lifetime before storage.

## Progress


- [x] Read instructions and fresh source; preserve remote changes.
- [x] Select shared transaction completion, recorder, SQL and configuration boundaries as one batch.
- [x] Capture grouped baseline failures using the unchanged server: five failures and three passing controls.
- [x] Implement the selected recorder/runtime ownership boundary, including cleanup and existing retry paths.
- [x] Extend existing suites; 80 distinct selected Rust cases, ten live assertions, affected all-target checking, lint and locked build pass.
- [x] Update both finding registers, batch map and committed validation receipt.
- [ ] Operational publication preparation: normal local commit through locked-build hook, verified recovery bundle and Cloud draft. The external final-handoff.json records completion without a self-referential commit amendment. No push.

## Context and Orientation


Go pkg/session/txninfo/summary.go owns one mutex-protected recorder with duration filtering and an FNV-1a digest/LRU (least recently used) cache. pkg/session/txn.go calls it when a valid transaction becomes invalid or is replaced. cmd/tidb-server/main.go initializes capacity and milliseconds from canonical config; pkg/server/handler/tikvhandler updates both config and recorder in request order. pkg/executor/infoschema_reader.go renders summaries and returns no rows without PROCESS. Rust tidb-exec/txn_summary.rs already owns the digest/cache; process.rs retains live transaction digests but discards them on finish. HTTP settings refuses the two controls, and the summary tables are absent from the serving schema. ClusterSession owns physical transactions separately from Session's catalog copy.

## Plan of Work


Extend the existing cache owner with synchronized duration filtering and rendering. Compose it at process startup and HTTP settings; preserve ordered partial errors and zero-capacity clearing. Register exact Go summary columns and route SQL retrieval through the existing materialization/authorization path. Add a retained transaction-observation handle at the physical timestamp/completion boundary, so eager/lazy reads, explicit transactions, retries and teardown share it. Preserve local catalog sessions' existing lifecycle. Remove refusal branches and stale helper-only descriptions after migrating callers.

## Milestones


First, connect the existing digest/LRU cache to synchronized duration admission and canonical configuration. Existing txn_summary_source cases and the settings tests must pass, including future timestamps, zero capacity and ordered errors. Second, connect physical transaction activation/completion and routed statement history to PROCESS-authorized SQL rows. Session/process and grouped server transaction tests must prove text/prepared, rollback, failed commit and idempotent cleanup. Third, start the real unistore server and verify the same SQL/HTTP contracts, including disconnect. Run the final checking/lint/build gates together; preserve complete-package limits in both registers.

## Concrete Steps


Activate source /workspace/.cloud-setup/env.sh. Run Cargo from /workspace/tidb/rust with CARGO_BUILD_JOBS=1. Store before/after evidence under /workspace/.cloud-setup/transaction-observation-batch. Extend tidb-exec/tests/txn_summary_source.rs, existing session/process tests and server settings/transaction suites. Combine filters per target. Run affected all-target checks, make lint, changed-hunk formatting and git diff --check. The normal precommit must run cd rust && cargo build --locked -p tidb-server.

## Validation and Acceptance


Capture the previously refused settings and missing summary table before edits. Afterward verify ordered digest sequences, FNV identity, duration threshold, LRU eviction/resize, zero capacity, PROCESS filtering, transaction replacement/rollback/drop, real storage timestamps and streaming completion. Verify real MySQL/unistore and HTTP behavior in the current Cloud environment. Keep failed, skipped and unrun outcomes distinct. No full multi-node, performance or complete upstream package acceptance follows from this maintenance batch.

## Idempotence and Recovery


Keep all changes local. Preserve concurrent integration commits, native checkout, installed tools and valid build outputs. Do not reset, force-push, bypass hooks or disable meaningful tests. Regenerate and verify the recovery bundle before replacement. Preserve unrelated Cloud configuration bindings.

## Interfaces and Dependencies


Reuse tidb-exec TransactionSummaryCache, process registry, existing statement and transaction owners, canonical config and HTTP Settings. No new dependency, worker or private SQL interpreter is needed. Physical observation must not retain a connection beyond its existing result/transaction lifetime.

## Surprises & Discoveries


The missing SQL table is a schema registration gap as well as a missing data provider. The current HTTP refusal masks absent startup configuration consumers. Storage timestamps and catalog timestamps have separate publication paths; wiring only transaction_finished would leave physical completion and catalog completion with different owners. The sampled baseline physical timestamp was already correct; no reproduced timestamp regression is claimed. Intermediate wire checks caught missing BEGIN/COMMIT/ROLLBACK digests, fixed by making the outer routed observation record once. The disconnect lock control passed before the explicit Drop correction, which follows Go session.Close rather than claiming a reproduced lock leak.

## Decision Log


Decision: group the complete recorder-to-runtime boundary rather than implement the two settings alone. Rationale: settings without producers and retrieval would still be inert. Date: 2026-10-04.

## Outcomes & Retrospective


The selected end-to-end behavior is implemented: all 80 selected Rust cases and all ten final wire assertions pass. Both configuration controls, completed history, SQL visibility and transaction completion now share owners. I01/N03/N05 remain partial; complete transaction diagnostics, remote fanout, other providers/configuration consumers and complete packages remain unaccepted. Other53 unresolved roots retain prior evidence. No push.

The full commands, hashes and before/intermediate/after outcomes are in parity/current-audit/transaction-observation-batch-validation.json. Run Cargo from rust with CARGO_BUILD_JOBS=1; make lint runs at repository root. Current-instance logs are in /workspace/.cloud-setup/transaction-observation-batch. The following selected commands were executed successfully:

    cargo test --locked -p tidb-exec --test all -- txn_summary_source --test-threads=1
    cargo test --locked -p tidb-session --lib -- transaction_observation_batch tidb_trx_reads_live_transactions process::tests:: --test-threads=1
    cargo test --locked -p tidb-server --lib -- transaction_observation_batch http_settings::tests:: cluster_session_node::tests::transactions:: cluster_session_node::tests::prepared_transactions:: cluster_session_node::tests::autocommit_transactions:: --test-threads=1
    ./target/debug/build/tidb-server/77c9a1484da84886/out/tidb_server-77c9a1484da84886 --exact cluster_session_node::tests::observation_batch::observation_batch_text_and_binary_writes_publish_after_commit_verdict --test-threads=1
    cargo check --locked -p tidb-exec -p tidb-session -p tidb-server -p tidb-config --all-targets
    cargo build --locked -p tidb-server
    make lint


Final operational status is recorded at /workspace/.cloud-setup/transaction-observation-batch/final-handoff.json; it is not proof of fresh-task restoration.


Revision note, 2026-10-04: recorded the implemented shared lifecycle, intermediate failures and final selected validation; broader package acceptance remains explicit.
