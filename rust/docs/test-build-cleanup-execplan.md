# Retire disconnected execution models and private harnesses

This living ExecPlan follows root PLANS.md. Earlier batches are preserved in the
receipts indexed by [the current audit](parity/current-audit/README.md), with their
original validation and recovery limits. Historical source is available in Git.

## Purpose / Big Picture


Reduce compiled, disconnected code and misleading coverage claims. Retire eleven
unused tidb-exec modules and their private tests together. The active chunk
executor, cluster statistics consumers, original Go tests and Rust correctness
regressions remain unchanged. This cleanup does not complete any Go package or
repair a structural finding, and no timing improvement is claimed.

## Context and Orientation


Base: 3f0ac881fe9f8474eba30e365a99760adc16359d on /workspace/tidb
hparser-integration. Refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
Native client-rust remains a0a6ec32deb8dfb565494b9b803598cd0e7bcef2.
The removed modules are backfill_metrics, broadcast_query_error, config_int_json,
configured_topn, infoschema_context, metrics_reader, readable_size,
real_tikv_stats_dump, slow_log_split, stats_load_result and traffic_form.
They have no production callers. Their tests exercise isolated copies rather
than the SQL/runtime owners. Go remains the authority for the original contracts;
absence of a literal Go test name alone is not grounds for deleting a regression.

## Progress


- [x] Refresh refs, trace exported symbols and module paths across tracked Rust code, scripts and manifests; no production references outside the deletion set.
- [x] Remove eleven modules, seven private test files, eleven exports and 48 tests; retain before-images, hashes and original Go anchors.
- [x] Replace stale coverage claims and condense accumulated cleanup instructions into this current plan plus durable receipts.
- [x] Verify 3,723 retained files and fresh generated registration; grouped affected compilation, lint and self-review pass.
- [ ] Commit with the actual locked-build hook, rebuild immediately before push, verify remote SHA and refresh Cloud startup/recovery state.

## Milestones and Plan of Work


The removal milestone deletes the disconnected owners and their test entrypoints
as one unit. Generated aggregate registration discovers the remaining test files;
do not hand-edit generated output or add another permanent cleanup script.
The documentation milestone retires the metrics-reader verification claim and
unused statistics-wrapper ownership claim, while preserving dated PD failure
receipts. This replaces the prior 365-line rolling plan; each earlier batch's
receipt retains its evidence instead of carrying stale pending checkboxes forward.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in each shell. From rust/ run
CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets.
From the repository root run make lint and git diff --check. Compare every
retained Rust source, manifest and script with its before hash; only lib.rs may
lose the eleven module declarations. Verify generated all_tests.rs no longer
registers the seven retired harnesses. These are pure deletions: no new behavior
or regression requires a behavioral suite rerun. Normal commits must execute
hooks/pre-commit through core.hooksPath=hooks, including cd rust && cargo build
--locked -p tidb-server. Repeat that exact build immediately before each push.

## Surprises & Discoveries


The configured TopN/Limit model survived its planner and query-adapter retirement.
It is now entirely test-owned; the real tidb-executor::topn remains intact.
The old metrics-reader audit called an unconnected seed VERIFIED. The statistics
storage receipt listed an unused real_tikv_stats_dump wrapper, while actual
cluster_session_node callers use cluster_stats_dump directly. All 24 explicitly
cited Go file paths still exist; removal is about unused Rust ownership, not
claiming that these Go behaviors disappeared.

## Decision Log


Delete the whole unused owner plus its tests instead of hiding failures or
removing tests from live code. Preserve Go obligations and dated evidence. Keep
connected helpers even when their names resemble the retired models. Date:
2026-10-05 UTC. No dependencies, scripts, native code or original Go files change.

## Outcomes & Retrospective


Eleven modules and seven harness files remove 3,635 lines plus eleven exports.
Forty-eight private tests/checks are retired. Validation passes; exact
outcomes belong in parity/current-audit/leaf-owner-cleanup-validation.json.
Publication and Cloud draft outcomes belong in
/workspace/.cloud-setup/leaf-owner-cleanup/final-handoff.json.

## Recovery


Before-images and retained-file hashes are under
/workspace/.cloud-setup/leaf-owner-cleanup. Recover an individual file with
 git show 3f0ac881fe9f8474eba30e365a99760adc16359d:<path> > /tmp/<filename>
and review before restoring it. Preserve concurrent changes and never reset or
force-push. Earlier cleanup receipts remain linked from README.md, including
configured-planner-cleanup, result-path-cleanup, session-leaf-cleanup,
dead-leaf-cleanup, orphan-test-cleanup, aggregate-leaf-cleanup,
comment-test-cleanup, audit-plan-cleanup and discard-check-cleanup validation.
