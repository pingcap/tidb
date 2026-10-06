# Remove disconnected reader and result-field resolver

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the unused table/index reader and tableless type resolver together with
their private harnesses. Live readers, shared DistSQL lifecycle and result
metadata conversion remain unchanged. Work in /workspace/tidb on
hparser-integration from 5bdd923e92b8a42f19f1d9d9fa62d061800489ca.
Fresh Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
Go TableReader/IndexReader consume planner requests; recordSet.Fields consumes
planner schema/names. The removed Rust paths have only private test callers.

## Progress


- [x] Trace all exported types/functions, including aliases and root reexports.
- [x] Remove six unused source/harness files and their module registrations.
- [x] Correct obsolete crate description and annotate two historical documents.
- [x] Validate shared DistSQL and protocol metadata tests, continuity and lint.
- [ ] Pass actual hook and fresh pre-push build, verify remote and Cloud draft.

## Milestones and Plan of Work


Delete tidb-exec/src/storage_reader.rs and its table_index_reader.rs child,
both reader test carriers, result_field_resolver.rs and its test carrier.
Remove declarations and root exports from lib.rs and registrations from tests/all.rs.
Keep cop_scan, real_tikv_read, distsql_recordset, shared InjectedQueryRuntime,
SerialSelectResults and result_metadata. Annotate the original coprocessor
audit and session cleanup plan so retired APIs are not recommended as owners.

## Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-exec -p tidb-distsql --test all -- result_metadata_source:: select_iter_source:: query_runtime_source:: active_cancellation_source:: --test-threads=1

Expect protocol names/types/flags, serial row ordering, cancellation, response
ownership and close behavior to pass at retained owners. Compare all retained
production and test files against their before-images; no caller or dependency
may change. Search non-cache sources for retired symbols, imports and aliases.
From repository root run make lint and git diff --check. The real hook must
execute cd rust && cargo build --locked -p tidb-server; rerun that command
immediately before normal push and verify remote SHA.

## Surprises & Discoveries


The storage reader was described as executor-owned but no live executor or
server constructed it. A prior cleanup consolidated the tableless resolver
harness without checking whether a production caller remained. The real
protocol converter and shared DistSQL lifecycle are separate and stay intact.

## Decision Log


Remove only disconnected wrappers and private checks. Preserve actual Go
behavior tests at retained owners. Keep historical evidence with clear current
status rather than treating old source-line assertions as live findings.
This cleanup does not complete reader, inference or upstream package parity.

## Outcomes & Retrospective


Implementation and validation complete: 24 retained-owner tests passed,
along with lint and continuity/diff checks. Removed 2268 net Rust lines and
19 private tests. Publication gates remain pending here; external
final-handoff.json records their later results. No full Go suite,
live cluster run or measured speedup is claimed.

## Recovery, Artifacts and Dependencies


Before-images: git show 5bdd923e92:<path>. Restore individual files only and
preserve concurrent changes. External inventory and logs are under
/workspace/.cloud-setup/reader-resolver-cleanup. Durable receipt:
rust/docs/parity/current-audit/reader-resolver-cleanup-validation.json.
Native client and dependencies remain unchanged. Draft save, Publish and
future restore are separate. Revision: replace the completed slow-log
retirement plan with this connected reader/result cleanup.
