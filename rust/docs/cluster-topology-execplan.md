# Shared cluster topology and replica-read policy

This living ExecPlan follows root PLANS.md.

## Purpose and context

Advance connected I02 discovery, O13 adaptive replica-read and N03 configuration-consumer findings together. Go master: 93a01d31f6da205ae4bf376825293903a6899fdb. Cloud source starts at ac241f4b3a49f13970893256fa56a188fa304e74; native stays 19a56ccda1e128218cd33c69709038219aced9bc. Remote integration 0a61ca9b586add9697a10f9bf8264931e3281a95 is preserved unmerged; avoid its schema-sync files. No pushes or push dry runs.

## Progress

- [x] Compare live Go discovery, domain adaptive policy and configuration owners.
- [x] Existing SQL metadata regression fails before edits: expected discovered proxy/CDC rows, received none.
- [x] Compose seven-source discovery, shared zone policy, session/index-lookup fallback, canonical startup labels and request adjustment after task creation.
- [x] 80 distinct targeted Rust tests, affected all-target checks, lint and self-review pass; two pre-existing ignored source obligations remain.
- [x] Update both registers, batch map and validation receipt; prepare gated local commit and recovery instructions. Exact hook, after-wire, commit, bundle and draft outcomes live in the external final handoff.

## Work and milestones

Reuse server-info syncer and process-owned PD. Remove duplicate startup record construction so labels and lease follow canonical configuration. Compose TiDB, PD, stores, TiProxy, TiCDC, TSO and scheduling metadata with Go error/warning behavior. Share topology with domain adaptive policy, preserve previous decisions on errors, refresh immediately and each minute, join before PD closes. Route effective policy to sessions and consume the request adjustment seam after region tasks exist. Do not claim missing planner-estimate or point-get consumers complete.

## Validation and acceptance

Use existing suites with grouped validation. Cargo runs from rust, sourcing /workspace/.cloud-setup/env.sh, CARGO_BUILD_JOBS=1. Extend metadata/transport regressions and Go zone balancing cases. Run affected all-target checks, make lint, diff checks and actual pre-commit cargo build --locked -p tidb-server. Existing-owner maintenance does not establish complete Go package acceptance. Full distributed, TLS and performance validation remain unverified.

## Surprises & Discoveries

NodeConfig already owns canonical global_config but node_server_info discards its labels. DirectUnaryQueryTransport never invokes the existing adjuster. PD store projection discards build/start metadata needed by CLUSTER_INFO.

## Decision Log

- 2026-10-04: batch the shared topology/configuration lifecycle; preserve concurrent schema-ack commits.

## Outcomes & Retrospective

Implementation and grouped validation pass for this maintenance boundary. SQL proxy/CDC discovery failed before edits; retained pre-change server also loses labels/lease text and rewrites configured version. After-wire confirmation follows the mandatory commit build. I02 retains SEM redaction and full mixed-cluster/HTTP acceptance; O13 retains planner-size/threshold and point-read producers. N03 retains wider defaults/consumers. No complete package claim.

## Recovery and interfaces

Preserve exact destinations and no-push constraint. Do not bypass hooks. After validation update unpublished bundle and reusable Cloud draft. Fresh-task restoration remains unverified.

Final post-gate evidence: /workspace/.cloud-setup/cluster-topology-batch/final-handoff.json. The committed receipt records80 distinct Rust passes, nonzero selections, exact logs/source hashes, ignored obligations and recovered test-fixture mistakes. The actual hook gate is mandatory; no push is authorized.

## Continuation: cluster configuration and bounded discovery


Starting at 3923a01d3edba6a4220bfef97297c032f6ca8224, repair I01 live
CLUSTER_CONFIG, I02 bounded address resolution and N03's internal HTTP
consumer together. Refreshed Go master remains93a01d31f6. Retain the seven
retrievers and process PD owner; do not touch incoming schema-sync changes.

Milestone one captures grouped failures for CONFIG authorization,
contradictory-filter request skipping and concurrent/joined address resolution.
Milestone two composes HTTP routing, shared TLS, status/JSON error warnings,
canonical flattening/hidden settings, stable rows and typed planner filters.
Wire the retriever into the production session factory used by both stores.
Keep remaining log/fanout/provider/package obligations explicit.

- [x] Capture three grouped Rust failures and seven real MySQL failures.
- [x] Implement connected retriever, filtering, workers and both factory paths; remove captured SHOW CONFIG.
- [x] 34 distinct Rust cases pass; affected all-target checks and lint pass.
- [x] Update both registers and validation receipt. Final hook, after-wire and recovery outcomes are owned by the external final handoff; no push.

Commands run in rust/ after sourcing /workspace/.cloud-setup/env.sh.
Build selected session/domain tests together, run filters in the produced
harnesses, then group affected exec/session/domain/server checks. Reuse the
existing aggregate integration harness for HTTP cases; no standalone target.
Record commands and exact results under /workspace/.cloud-setup/cluster-config-batch/.
Restore individual files from the parent above if needed, preserving concurrent
changes. Original upstream inventories and fixtures remain unaccepted; no
whole-package or performance claim follows from this maintenance batch.

Discovery: SHOW CONFIG had a second captured configuration implementation; it now uses the same live retriever and CONFIG gate. The existing internal HTTP builder lacked Go’s five-minute timeout; all its consumers now receive it. HTTP node pools remain distinct from discovery pools, sharing policy rather than claiming one universal pool.

Remaining: lower/upper and parameter-marker request extraction, the complete CTE/join/extractor matrix, CLUSTER_LOG bounds and other providers, full upstream source/generated/platform/test/fixture acceptance, live multi-node TiKV, TLS HTTP end-to-end and performance. Ordinary SQL predicates remain active when extraction falls back. Two pre-existing ignored parity obligations remain; the third ignored harness entry is a subprocess helper executed by its parent. No finding or package is closed. Final post-gate evidence: /workspace/.cloud-setup/cluster-config-batch/final-handoff.json.
