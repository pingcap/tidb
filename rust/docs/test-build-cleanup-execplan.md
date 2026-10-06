# Remove the unused Domain sysvar facade and stale audit guidance

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Reduce maintained mock-only code and obsolete documentation without changing
live configuration behavior. Work in /workspace/tidb on hparser-integration
from a1d48bd7fc290918b99b5381f48832cc78e73864. Fresh Go master is
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Go initDomainSysVars installs
callbacks on the real Domain/store owners. Rust's domain_sysvars.rs instead
requires DomainSysVarEnv, implemented only by MockEnv in its private tests.
Live session/server configuration uses separate maintained owners.

## Progress


- [x] Trace facade callers and compare Go's real initialization callbacks.
- [x] Distinguish original Go TopN/CDC case tables from the mock-only facade.
- [x] Remove facade, private tests, stale crate narrative and two audit docs.
- [x] Run grouped live sysvar SQL tests, continuity and lint.
- [ ] Complete actual hook, fresh pre-push build, remote and Cloud checks.

## Milestones and Plan of Work


Delete rust/crates/tidb-domain/src/domain_sysvars.rs and unregister it in
lib.rs. Replace lib.rs's obsolete planning narrative with a concise current
ownership description. Delete completed, unreferenced
rust/docs/domain-sysvar-cache-parity-audit.md and
rust/docs/plan-replayer-domain-parity-audit.md. Their historical package
inventory/validation receipt remains in
rust/testport/receipts/domain_plan_replayer_retention.md with current status.

Keep topn_slow_query.rs and cdcutil.rs with their original Go case tables;
being unintegrated does not make those semantic tests useless. Preserve
session/show_admin.rs and every real sysvar, statistics and native consumer.
Record the remaining TopN live-owner integration gap without pretending it
was fixed. Update both structural registers and the current-audit index,
leaving all finding dispositions unchanged.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust, run with
CARGO_BUILD_JOBS=1:

    cargo test --locked -p tidb-session --lib -- tests_global_vars::low_resolution_tso_update_interval_clamps_and_warns tests_global_vars::schema_cache_size_global_hook_publishes_bytes tests_global_vars::circuit_breaker_pd_metadata_ratio_global_hook_publishes_float tests_global_vars::resource_control_global_hooks_publish_process_switches tests_global_vars::global_config_explicit_sql_notifies_the_domain_owner tests_global_vars::global_config_notifies_default_and_scratch_writes_but_not_cache_reloads --exact --test-threads=1

Expect six existing SQL tests to preserve clamping/warnings, typed process
publication and global-config notification ownership. Verify every retained
Rust source/test and dependency file is unchanged. No retired imports/types
may survive. Run make lint and git diff --check from root. Normal commit must
run the actual locked tidb-server hook; repeat that build immediately before
push and verify the remote SHA. No Go/Bazel or dependency changes are needed.

## Surprises & Discoveries


The old crate header says TopN lives in tidb-exec, although its module and
Go heap/FIFO tests live in tidb-domain. The live ADMIN SHOW SLOW recorder uses
a separate session collection. Keep the genuine Go algorithms/tests and
record that integration gap. The sysvar facade's 12 tests only instantiate
its mock environment; no real Domain/store implements the trait.

## Decision Log


Remove the mock-only facade after exhaustive caller tracing. Preserve useful
original Go cases, including unintegrated TopN and CDC algorithms. Remove
completed duplicated audits rather than editing their old VERIFIED claims.
Keep dated source/test inventory receipts with a current-status correction.
This is cleanup, not a bug fix or complete Go package acceptance.

## Outcomes & Retrospective


Implementation and validation complete: six live sysvar SQL tests passed,
along with lint and continuity checks. Removed 745 net Rust lines, twelve
private tests and two completed audits. Publication gates remain pending
here; external final-handoff.json records their later completion. No measured speedup, full Go suite,
live cluster or completed Domain lifecycle parity is claimed.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show a1d48bd7fc:<path>, preserving
concurrent changes. External inventory/logs: /workspace/.cloud-setup/domain-facade-cleanup.
Durable receipt: rust/docs/parity/current-audit/domain-facade-cleanup-validation.json.
Publication gates are recorded in external final-handoff.json after commit
so evidence does not cause another source mutation/build cycle. Cloud draft
save, Publish and fresh-task restore remain separate. Revision: replace the
completed planning-leaf cleanup with Domain facade/documentation retirement.
