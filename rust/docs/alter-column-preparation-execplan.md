# Prepare all existing column actions before ALTER execution

## Purpose / Big Picture


An ALTER must resolve each column action against its original schema, preserve
Go's ordered diagnostics, exclude conditional no-ops from conflicts, and finish
admission before any earlier column rewrite or index backfill. This continues
existing-owner maintenance after index preparation; it does not accept or
partially transcreate the whole Go DDL package. The user authorizes broader
owner migration, deletion of displaced code, targeted validation, commit and push.

## Progress


- [x] Refresh integration/client and Go master; read repository, DDL and testing guidance.
- [x] Trace all six existing column spellings and Go's job/conflict lifecycle.
- [x] Reproduce original-schema, conditional-note and admission-before-rewrite failures.
- [x] Split builders from execution, migrate every column caller and consume admitted operations in conflicts.
- [x] Verify metadata positions, generated dependencies, allocator ownership and ordered diagnostics.
- [x] Run affected tests, all-target checks and lint; update D11 evidence.
- [x] Commit with the actual locked-server-build hook (18.75 seconds); the receipt amendment repeats that hook.
- [ ] Rerun the locked build after the amendment immediately before push and verify the remote; record final publication in the task thread.

## Context and Orientation


Starting integration a49cc7550206f62eb7c4b09c0cba185deace8152, Go master
93a01d31f6da205ae4bf376825293903a6899fdb and native client/dependency
19a56ccda1e128218cd33c69709038219aced9bc are current. The prior commit is pushed
and its final build and remote verification succeeded. D11 remains partial;
the register has 71 unresolved IDs (63 open, eight partial), 15 repaired.

The local dispatcher is rust/crates/tidb-executor/src/ddl/alter_table.rs.
It stages a catalog, prepares indexes through ddl/index_changes.rs, but derives
column conflicts directly from syntax and executes column builders immediately.
Its ADD/MODIFY/DROP helpers and alter_metadata.rs's RENAME/DEFAULT helpers mix
admission with mutation. Session dispatch and partition metadata derivation call
this common entry. This is not the durable cluster worker.

Go pkg/ddl/executor.go resolves specifications and invokes AddColumn,
DropColumn, ModifyColumn, ChangeColumn, RenameColumn and AlterColumn against the
original table. Only admitted jobs reach multi_schema_change.go::fillMultiSchemaInfo.
It checks combined visible-column counts after conflicts. Modify admission
includes the read-only NULL precheck, while row conversion runs in execution.
Use git show origin/master:<path>; this branch's Go working tree is older.

## Plan of Work


First extend the existing executor multi-schema suite and session ALTER suite,
then run the new tests against unchanged production. Cover missing original
sources, duplicate original targets, conditional no-op names, error/note order,
and a failing row rewrite that must not precede later admission errors.

Next retain column definitions and original identities in typed prepared changes.
Split the existing builders without creating a second table or transaction
engine. Keep generated expressions bound by their existing dependency names.
Resolve physical positions at application against the current staged table, as
Go's worker does. Preserve shared allocator operations behind the existing
PreparedAllocatorChanges execution boundary. Carry diagnostics per action so
preparing a later action cannot publish its warnings before an earlier failure.
Replace AST-only column conflict collection with admitted operation metadata and
remove the old immediate action dispatch. Index and column changes must share
one preparation/error boundary.

Finally exercise nearest DDL/column/default/generated/FK suites and all affected
crate targets. Compare unexpected failures against the unchanged baseline. Update
the D11 receipt and register without closing remaining durable ownership gaps.
No Go/Bazel/generated inputs are planned, so bazel_prepare is not required.

## Concrete Steps and Validation


From rust/:

    cargo test --locked -p tidb-executor --lib tests_ddl_multi_schema_change_sql -- --nocapture
    cargo test --locked -p tidb-session --lib tests_alter_column -- --nocapture
    cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets

Select the existing default/generated/metadata suites after inspecting their
names. New controls must fail before repair and pass afterward. Successful
statements must retain rows, defaults, offsets and identities; rejected ones must
leave catalog/storage/shared allocator state unchanged. Root validation:

    GOTOOLCHAIN=go1.25.14 make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "ddl: prepare column actions before ALTER execution"
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify remote SHA and clean checkout. No whole-package acceptance, distributed
recovery or benchmark speedup follows from in-process tests.

## Idempotence and Recovery


Use fresh in-process schemas. Baseline controls restore only this task's files
and restore saved bytes in a finally block. Never discard unrelated changes or
force push. On remote advancement inspect it, rebase safely and repeat required
gates. Unsupported actions remain refused until their owners are implemented.

## Surprises & Discoveries


Column conditional no-ops currently still enter conflict categories. Column
builders can also observe metadata altered by prior actions, while the index
builder already uses the original table. This split is the root mismatch.

## Decision Log


2026-10-02: Expand the existing preparation lifecycle across all current column
spellings. A second validation-only pass over mutating helpers would duplicate
work, diagnostics and side effects; split the builders and migrate callers.

## Outcomes & Retrospective


Implementation and scoped validation pass: 293 selected tests pass, two
failures reproduce on unchanged production, and eight existing tests remain
ignored. Non-column/non-index admission and durable jobs remain outside this
maintenance scope; D11 stays partial. The actual commit hook passed; the receipt
amendment repeats it before the final build/push gate.

2026-10-02 discoveries and results: the shared executor now retains original
column definitions/IDs and resolves positions on the execution image. The
AUTO_RANDOM offset was absent from all three existing column permutations;
ADD, DROP and MOVE now carry it with the other metadata owners. The same
layout-compatibility check serves preparation and staged execution. Local
warning isolation is shared with the existing statistics replay context; local
draining does not consume coprocessor sinks. The separate index-note variant
is removed. Go's original and combined column-count checks now run at their
respective boundaries.

Nine new regression tests failed before their respective repairs. A tenth
new test is a successful sibling-position/generated-dependency control. The
existing TestIssue5092 translation contained a wrong 8200 expectation and
changed the upstream drop precondition; it now follows the master source,
including original duplicate 1060 and combined-drop 1090 diagnostics. Its
corrected expectation fails against unchanged production. A CREATE DEFAULT
parser rejection and the existing FK-name expectation fail identically on
unchanged HEAD. Exact evidence is in the column preparation receipt.
