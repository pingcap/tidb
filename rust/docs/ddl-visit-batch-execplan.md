# Share DDL admission visits across execution and observation

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Repair connected A01 privilege admission, D01 persistent DDL routing, D11 multi-action DDL admission and O18 table attribution against Go master 7a3dacb52efe58d28db360ae8639d8838c376544. Supported destructive DDL must check every source/destination privilege before mutation; foreign keys must require REFERENCES and CREATE LIKE must require source SELECT. The same ordered visits must feed statement summaries. This maintains existing owners, without accepting complete planner/executor packages.

## Progress


- [x] Confirmed clean TiDB hparser-integration at 0a44d34fd0 and native master at 8b752f9638; refreshed Go master and verified managed reads.
- [x] Traced Go buildDDL, Rust shared admission and local/cluster callers; selected one connected batch.
- [x] Six baseline regressions failed; migrated shared DDL visits and Go temporary-table policy. Denials preserve data, grouped schema and open transactions.
- [x] 204 grouped privilege/observation tests, affected all-target checks, lint and 36 real SQL checks pass.
- [x] Updated both registers and self-reviewed. Publication outcomes (actual hook, fresh locked build, normal push, remote SHA and Cloud checkpoint) are recorded in external final-handoff.json.

## Context and Orientation


rust/crates/tidb-session/src/table_privilege.rs supplies ordered requests to identity.rs. Both local dispatch.rs (before DDL implicit commit) and cluster_session_node/mod.rs call this owner. check_table_privilege_requests also publishes the same tables to observation.rs. Existing tests_grants/table_scope.rs owns grant/denial and positive controls; tests_observation_batch.rs exercises completed SQL summaries. Go pkg/planner/core/planbuilder.go::buildDDL defines privilege order, source and destination scope, and error verbs; there is no package-level doc.go in that directory.

## Plan of Work and Milestones


First extend existing behavior suites for standalone destructive DDL, sequence/database changes, CREATE LIKE, foreign keys and partition/rename multi-action visits. Capture failures together before editing production. Then replace broad single-ALTER/single-CREATE shortcuts with action-aware visits, preserving one shared admission/observation owner. Finally run one grouped validation boundary and publish accurate receipts. Unsupported view/durable DDL execution remains unsupported.

## Concrete Steps and Validation


In /workspace/tidb/rust source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Baseline: cargo test --locked -p tidb-session --lib ddl_visit_batch -- --test-threads=1. After edits run the neighboring tests_grants and tests_observation_batch filters in one test invocation. Run cargo check --locked --all-targets -p tidb-session -p tidb-server, root make lint and cargo build --locked -p tidb-server. Start an owned unistore server to test real authentication, denied mutations and grant-enabled controls; stop and join it. Actual hooks and a fresh identical locked build immediately before push are mandatory.

## Surprises & Discoveries


The existing collector omitted whole supported DDL kinds, and ALTER required only ALTER even when Go demands both tables' CREATE/INSERT/ALTER/DROP for EXCHANGE. Go CREATE LIKE reports CREATE in a source SELECT denial; preserve the error verb separately from the checked privilege. Resource headroom is only190MiB; retire only completed inactive executable aliases with hashes and accessible process checks, retaining compilation caches.

The first wire run exposed cluster ALTER-FK capability refusal before shared privilege admission. The cluster unsupported-route boundary now checks that same collector; authorized callers retain the capability refusal. Cluster DDL execution reuses the original parsed statement, and both DDL wrappers reuse the original parsed node. Local TRUNCATE admission and historical-read validation precede its external cluster transaction commit. Existing cluster sequence creation does not supply a durable sequence for later ALTER/DROP; in-process sequence behavior is verified, cluster sequence completion is not claimed.

## Decision Log


- Decision: repair DDL admission and its shared observation consumers together.
  Rationale: these missing visits can permit unauthorized mutation and lose source/destination attribution; one collector fixes all callers before mutation.
  Date/Author: 2026-10-07 / Codex.

## Idempotence and Recovery


Preserve concurrent changes, normal branch destinations and hooks. No native edits, Go files or manifests are needed. Keep behavioral tests; do not add a standalone test executable. Re-run only checks affected by subsequent edits. Saving Cloud configuration is distinct from publication.

## Interfaces and Dependencies


Reuse TablePrivilegeRequest, GlobalPriv and typed DdlStmt/AlterTableAction. Extend error labeling only where Go's checked privilege differs from its denial text. No new dependencies or service owners.

## Outcomes & Retrospective


204 selected Rust cases pass with no ignored tests; 36 live SQL checks pass. The original six Rust failures and five confirmed live admission/routing/shadow failures are repaired. One intermediate test incorrectly expected CREATE LIKE from a local temporary source to succeed; Go serial_test.go explicitly rejects it, so only that expectation was corrected. ALTER DATABASE execution remains unsupported after its corrected admission. Source checks, lint and locked build pass. Parent findings remain partial; this is not complete package transcreation.


Routing follow-up: real MySQL reproduced partition ADD disappearing after unrelated schema refresh, sequence CREATE falsely succeeding without durable metadata, and local-temporary TRUNCATE incorrectly reaching cluster metadata. Remove the stale sequence/partition local-routing exceptions. Supported partition actions now use existing cluster DDL; missing sequence actions refuse before mutation. Resolve local-temporary TRUNCATE through the session owner while retaining Go's implicit cluster transaction commit. Retire repeated parsing by passing the original parsed node to both cluster and local DDL execution. Re-run the affected session group, checks/lint/build and the enlarged wire batch.


Self-review correction: privilege exemptions are restricted to owners that actually resolve the session overlay (queries/DML, local CREATE/DROP and TRUNCATE). Other cluster DDL retains its existing checks until complete local-target splitting is implemented; a blanket exemption could mutate a hidden permanent table. A real wire regression also showed CREATE LIKE copying permanent metadata hidden by a temporary source. Go preprocess.go::checkCreateTableGrammar rejects that source before checking privileges; the shared collector now preserves that8006 refusal. All three original routing failures and this shadow-source failure are retained externally. The package build also links cluster-session-smoke (~383MB) beside tidb-server (~452MB); reserve room for both, retiring only completed inactive executable outputs, never compiler caches.
