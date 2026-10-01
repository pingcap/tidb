# Resolved UPDATE privilege repair

Reference: TiDB master `93a01d31f6da205ae4bf376825293903a6899fdb`.
Integration baseline: `9d6fab5fdcc062033c95b974ffc9d057f89fbb58`.

This repairs the demonstrated A01 authorization bypasses. It does not accept
the complete Go planner/core, session, executor or privilege packages as
transcreated. The existing package inventory and other open findings still
apply; A01 remains partially repaired.

## Cause and change

The session's AST collector could identify qualified UPDATE assignments but
silently omitted unqualified assignments in joined updates. The write executor
resolved those columns later, so a SELECT-only account could write. Stored
PREPARE requests inherited the omission. EXPLAIN dispatched before the shared
table-privilege check, including the write-capable ANALYZE form.

`tidb-executor/src/driver/planner_bridge.rs` now shares its logical FROM-plan
builder between correlation scopes and UPDATE privilege resolution. The latter
uses the existing Go `FindFieldName` port over logical output names, then the
outer updatable-table list to recover the base database/table. It excludes
derived sources from that writable list. There is no SQL-text column guesser,
physical optimization or row execution in this metadata pass. Single-table
UPDATE retains its fast, unambiguous target path.

`tidb-session/src/table_privilege.rs` removes its UPDATE qualifier branch.
`identity.rs`, `prepared_ast.rs`, `prepared_statements.rs` and `dispatch.rs`
share the resolved requests and the existing live grant evaluator. PREPARE
initializes markers to NULL, as Go `plan_cache_utils.go` does. A joined UPDATE
is not admitted to the current physical plan cache and rebuilds on EXECUTE;
its privilege targets rebuild too. DDL can move an unqualified column from one
table to another between PREPARE and EXECUTE. Resolution uses the transaction's
catalog and session temporary-table overlay, matching execution's visible
metadata. Read-only privilege collection does not acquire a catalog or plan.

`explain_arm.rs` now checks the bound inner statement before EXPLAIN or ANALYZE.
This retains the ordinary error codes: SELECT denial 1142, UPDATE denial 8121,
ambiguous assignment 1052, missing assignment 1054, non-updatable source 1288.

## Corrected audit allegation

The earlier column-only SELECT claim was wrong. Go `buildDataSource` appends
table SELECT with an empty column name; its privilege verifier does not match
a specific column grant to that empty name. On master, GRANT SELECT(x) followed
by ordinary SELECT x returns 1142, exactly as Rust's earlier probe did. The
audit now marks this candidate disproved; no new column-grant semantics were
added to Rust.

## Evidence

`tests_grants/table_scope.rs` has 20 passing tests. Seven additions exercise
unqualified/aliased joined targets, empty results, several write targets,
read-only participants, SQL/binary PREPARE, REVOKE, DDL retargeting, name errors,
derived parameter markers, EXPLAIN and temporary metadata. Five of these fail
with unchanged HEAD production code and pass with the repair; the other two
are compatibility controls. The first pre-fix probe wrote a row despite lacking
UPDATE, and the prepared probe wrote after UPDATE had been revoked.

Both original tests in Go `pkg/session/test/privileges` pass, together with the
added oracle cases in [the reference patch](update-privilege-oracle.patch).
That patch extends an existing test without new Go files, imports or top-level
test functions. The reference worktree's fresh Bazel preparation passed. No
failpoint or testfailpoint references or Bazel failpoint dependency were found
in the owning test package, so the Go tests ran without failpoint toggling.

Broader Rust suites are not green. Their failures were reproduced with unchanged
HEAD production code and identical tests, then compared with the repair:

| Suite | After repair | Change in failures |
| --- | --- | --- |
| `tests_grants::` | 80 passed, 31 failed | Five new regressions repaired; all 31 remaining failures also occur on HEAD |
| `tests_prepared` | 82 passed, one failed | Same FTS cache failure on HEAD |
| `tests_explain` | 99 passed, 12 failed | Same optimizer/cardinality/unsupported-plan failures on HEAD |

The final grant run includes the additional passing temporary-table control.
The earlier baseline has one fewer test. Exact names and comparison results are
in [the baseline receipt](update-privilege-baseline.json). Grant failures include
SHOW GRANTS quoting, process-list behavior and schema visibility; they were
neither hidden nor changed by this repair.

## Validation commands

From `rust/`:

```sh
cargo test --locked -p tidb-session --lib joined_update_
cargo test --locked -p tidb-session --lib tests_grants::table_scope::
cargo test --locked -p tidb-session --lib tests_grants::
cargo test --locked -p tidb-session --lib tests_prepared
cargo test --locked -p tidb-session --lib tests_explain
cargo check --locked -p tidb-executor -p tidb-session --all-targets
```

At the clean Go master reference, apply the saved oracle patch after fresh
workspace preparation:

```sh
PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -tags=intest,deadlock ./pkg/session/test/privileges -run '^TestSessionAuth$' -count=1
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -tags=intest,deadlock ./pkg/session/test/privileges -run '^Test(SessionAuth|SkipWithGrant)$' -count=1
```

From the integration root: `make lint` and `git diff --check`. Rustfmt was applied
to changed code while preserving unrelated pre-existing formatting. All-target
compilation and lint pass. The initial sandboxed lint attempt could not resolve
the pinned lint tool through proxy.golang.org; the network-enabled retry passed.
There are no Go or Bazel input changes in this integration repair, so its
`make bazel_prepare` gate is not triggered.

Publication is gated by the real pre-commit hook's
`cd rust && cargo build --locked -p tidb-server`, and another fresh invocation
of that exact build immediately before push. Neither gate may be bypassed.

## Remaining scope and risks

Other statement kinds still collect AST visits, and execution still has its
separate DML assignment representation. Completing Go's single visit-info and
resolved-write lifecycle requires the full owning planner/session packages,
including temporary-table privilege exemptions and compilation/schema lifetime.
This repair must not be presented as closing that larger structural finding.

Joined authorization now performs a logical metadata build before execution;
folding this into the retained compiled plan remains part of that migration.
No sysbench, TPC-C, TPC-H, YCSB, live TiKV, concurrent DDL or wire-server workload
was run. Performance improvement and complete package parity are not claimed.
The disposable Go reference build outputs were cleaned and its checkout archived.
