# Recheck unresolved ownership findings and make local ALTER atomic

## Purpose / Big Picture

Reassess every currently unresolved structural finding against Go master and
current Rust callers, then repair the confirmed local ALTER publication gap.
Failed ALTER statements must preserve the prior schema and stored rows, including
earlier actions and grouped column specifications. Go owns this outcome in the
DDL job lifecycle. This change repairs an existing synchronous Rust owner; it
does not accept the complete Go DDL package or replace its missing durable owner.

## Progress

- [x] Pull TiDB integration, Go master and client-rust; read repository and DDL instructions.
- [x] Compare prior source anchors: 54 unchanged findings and 17 with changed anchors.
- [x] Complete caller review and fresh retained diagnostics; classify all 71 findings.
- [x] Demonstrate failed-ALTER schema/data regressions before production changes.
- [x] Replace action-specific catalog publication with one statement boundary; both callers retain the common API.
- [x] Run affected tests, lint, all-target checking and the actual commit hook; record the guarded publication command.

## Context and Orientation

Starting TiDB integration is b52dbfef7f9a0076500132bc63b276617440aa89;
Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. Native client-rust and
the dependency are 19a56ccda1e128218cd33c69709038219aced9bc. The current audit
tracks 86 IDs: 71 unresolved (64 open, seven partial), 15 repaired. D09/D10
are disabled seeds, not admitted production failures. Prior source continuity
starts at cfc6a174bb3e46312dae48a7b85a53053b2f5ea0; unchanged references retain
prior evidence but do not replace a review of changed callers.

The common local ALTER dispatcher is tidb-executor/src/ddl/alter_table.rs.
Its callers are session dispatch and cluster DDL column derivation. It currently
clones Catalog only for selected multi-action foreign-key statements. Catalog
metadata and in-process table storage use copy-on-write; shared service fields
and external storage need examination before relying on that isolation.
Go pkg/ddl/multi_schema_change.go reverses failing subjobs and crosses the
non-revertible boundary together. Its tests preserve original rows when later
unique-index construction fails. Durable scheduling remains tracked separately.

## Milestones and Plan of Work

Retain per-ID source references, current hashes, remaining contracts and diagnostic
limits in a new audit receipt. Review all changed crate callers and native-client
ownership since the prior full review, excluding repaired symptoms from current
allegations. Re-run retained SQL/wire diagnostics with temporary examples and
remove them afterward. Exit zero means a probe completed, not parity.

Extend the existing executor multi-schema test suite with a failed later column,
grouped columns, index/storage mutation and successful controls. Check published
metadata versions and rows, and examine shared catalog services. Establish red
tests first. Then use one statement publication boundary for all local ALTER
actions, without a foreign-key category exception or repeated parsing. Preserve
existing admission, warnings and error identity. Check the real cluster planner
caller separately; do not treat catalog cloning as external transaction rollback.

Run focused tests and affected ALTER suites, checking any failures against the
unchanged baseline. Run all-target checking for affected callers and root lint.
Update the audit only for a fully repaired recorded allegation; retain broader
DDL and generated-column limitations. Review the final diff and publish using
the required locked Rust server build in the hook and again immediately before
push. No Go/Bazel/generated inputs change, so bazel_prepare is unnecessary.

## Validation and Publication

Exact commands and results will be recorded here and in the audit receipt.
Use GOTOOLCHAIN=go1.25.14 make lint from the root. Commit with
TERM=xterm git -c core.hooksPath=hooks commit, then run from the root:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify the remote commit and a clean checkout. Preserve concurrent work; never
force push. No live distributed recovery or benchmark result is implied by
source review and local fixtures.

## Surprises & Discoveries

A single AddColumns AST action may contain several column specifications, so
action count alone cannot define statement atomicity.

## Decision Log

2026-10-02: Keep the complete known-finding review distinct from existing-owner
repair and whole-package acceptance. The registry is a list of unmet contracts,
not a count of freshly reproduced runtime failures.

## Outcomes & Retrospective

All 71 known unresolved IDs still describe an unmet contract; D11 advances to
partial, so the register is 63 open, eight partial, 15 repaired (86 tracked).
Six retained diagnostics and one allocator diagnostic were rerun; the LFU
admission regression still fails. See [the full review](parity/current-audit/alter-review.md)
and [validation](parity/current-audit/alter-review-recheck/validation.json).

The code change removes the FK-only staging branch and duplicate parsing.
Failed ALTER now retains the catalog image, row/index keys, cross-table FK
metadata and versions. Four executor regressions and one session regression
failed before repair and pass afterward; 37 executor DDL and 14 session tests
pass. The cluster DDL suite has 105 passes and five identical baseline failures.
All-target checking and make lint pass. The actual pre-commit hook passed the
locked server build in 16.03 seconds. The receipt amendment repeats the hook;
publication then requires a fresh post-commit locked build immediately before
push, followed by remote-SHA and checkout verification.

This does not complete D11: shared allocator rebasing still happens before all
subjobs have been admitted, and an existing drop-column/index-rename combination
still refuses where Go succeeds. A retained diagnostic demonstrates the allocator
side effect. The next owner change must prepare the whole ALTER before executing
external effects, following Go's job collection boundary. Do not roll back the
shared allocator or invent a second transaction engine.


2026-10-02 update: source and runtime review exposed the allocator boundary beyond
catalog copy-on-write. Keep D11 partial, correct two tests that encoded partial
publication, and retain the admission difference explicitly. The repair is an
existing-owner maintenance change, not package acceptance or durable DDL closure.


2026-10-02 publication update: origin advanced to c510669484 (SHOW statements),
so the first push was rejected. Inspected both files, rebased without conflicts,
and retained all findings. Repeat session tests, all-target checking, lint, the
actual hook and fresh pre-push build on the integrated branch.
