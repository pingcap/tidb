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
