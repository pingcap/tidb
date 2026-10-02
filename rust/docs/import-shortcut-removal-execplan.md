# Remove the private IMPORT INTO execution pipeline

This living ExecPlan follows `PLANS.md`. Keep progress, decisions, evidence and
remaining owner requirements current. This withdraws an unsafe partial runtime;
it does not transcreate or accept part of a Go package.

## Purpose and context


Remove successful IMPORT INTO execution that silently disregards the statement's
meaning. The session dispatcher owns a private CSV reader, a nested COUNT
precheck and a row-by-row generated INSERT loop. It ignores import options and
assignments, turns user variables into column names and returns affected rows
instead of file-import job information. Its SELECT arm rewrites IMPORT INTO to
INSERT SELECT. These are competing execution policies without Go's import owner.

Starting integration is bca8a743b76e7e0f1e699d936042c240316086d7. Both integration
and Go master were fetched before work; Go master remains
93a01d31f6da205ae4bf376825293903a6899fdb. Native client-rust remains
6163ecfc587b248dcbf0e30c1c9d905b4bc5a665 and is outside this removal.

Go pkg/executor/import_into.go constructs an import plan and controller for
both source types. File import initializes data/configuration, uses a separate
precheck session, submits a durable task (including local standalone tasks),
waits unless detached and returns job information. SELECT has its distinct
importer/encoding path; it is not evidence for file-import result shape. The
existing complete inventories for pkg/executor, pkg/executor/importer and
pkg/dxf/importinto remain the future acceptance units. No original Go artifact,
test, generated/platform/build variant or fixture is accepted by this withdrawal.

## Progress


- [x] Refresh refs, compare source owners and trace every runtime ImportInto arm.
- [x] Add containment regressions and demonstrate failure on the old pipeline.
- [x] Remove both rewrites, private precheck/CSV parsing and false-success test.
- [x] Verify refused statements preserve rows/schema and transaction rollback;
  check ordinary DML, LOAD DATA statement context and prepared-command behavior.
- [x] Update E05 without closing the complete import owner; pass lint, formatting
  and register/source checks. Reproduce the unrelated DML failure on baseline.
- [x] Pass the actual locked-build commit hook and fresh post-commit locked
  build. Repeat both for the final receipt amendment before publishing; record
  the final commit/remote state in the task result.

## Milestones and implementation


Extend the existing session DML test module with file and SELECT containment
cases. Require each SQL form to parse before execution; exercise columns,
options, assignments, populated targets and missing files. Check unchanged rows
and schema and continued ordinary INSERT use. These tests assert safe refusal
by an incomplete Rust runtime; they do not claim Go refuses these statements.
Run them before editing production and retain actual assertion failures.

Remove the entire ImportInto dispatcher body and parse_csv_records from
tidb-session/src/dispatch.rs. Replace execution with an explicit unsupported
error before nested SQL, file reads or data mutation. Remove the existing test
that incorrectly describes generated INSERT as the Go import data path, replacing
it with the new containment coverage. Retain parser/AST, statement classification,
prepared-statement restrictions and materialized-view source SQL builders.

Confirm no other production runtime implements a hidden import fallback. Existing
INSERT and LOAD DATA are separate owners and remain in place. NodeConfig's
second whitelist and native/TiDB routing owners are also retirement candidates,
but their complete consumers/replacements are still missing; this removal does
not simply delete those live dependencies or claim their findings are closed.

## Validation and publication


From rust/, use the existing session library test module:

    cargo test --locked -p tidb-session --lib import_into_refusal
    cargo test --locked -p tidb-session --lib tests_core::dml::
    cargo test --locked -p tidb-session --lib an_assignment_cast_reports_cast_values_own_error
    cargo test --locked -p tidb-session --lib statement_priority_and_no_cache_reach_the_statement_context
    cargo test --locked -p tidb-session --lib a_prepared_statement_may_not_be_a_prepared_statement_command

The casting filter is a baseline diagnostic, not a passing gate (see outcomes).
From root also run:

    rustfmt --check --edition 2021 rust/crates/tidb-session/src/tests_core/dml.rs
    git diff --check
    make lint

Commit with TERM=xterm git
-c core.hooksPath=hooks commit and require its actual cd rust && cargo build
--locked -p tidb-server hook to pass. Rerun that exact locked build after the final
commit, then push origin HEAD:hparser-integration. No Go/import/module/Bazel
files change; Bazel preparation and failpoint enablement are not required.

## Surprises & Discoveries


The current test is authored around the private implementation and claims the
ordinary INSERT data path follows Go, despite deferring its job lifecycle.
It is not an original Go parity fixture and must not preserve the false-success
contract. Original source artifacts and audit reproductions remain intact.

The first SELECT containment draft included SET, which both Go and Rust reject
at parsing (pkg/parser/import_brie_parser.go and the original AST test). It was
replaced with a valid projected SELECT. The original failing-before statement
was valid and still demonstrates the old private rewrite; all final cases require
successful parsing. File-assignment and user-variable cases remain covered.

The wider DML module exposed an existing time-cast diagnostic difference:
ODKU du='notatime' reports "Incorrect time value" where the test expects
"Truncated incorrect time value". Running the same unchanged test against
committed HEAD dispatch.rs reproduced it. The diagnostic and assertion were
not modified or ignored; the removal source was restored in a finally block.

## Decision Log


Remove the whole unsafe shortcut rather than adding an option-specific CSV fix.
The user has explicitly authorized removal of Go-absent implementations. Retain
syntax and real source helpers; a new import runtime requires complete package
ownership and caller migration under root AGENTS.md. Safe refusal is a loss of
the old partial functionality, not complete Go compatibility.

## Outcomes & Retrospective


Removed 183 production lines from dispatch.rs, replacing both import rewrites
with one explicit unsupported admission and deleting the private CSV parser.
The obsolete test claiming this followed Go was removed. Two containment tests
cover six file forms against empty and populated targets and three SELECT forms,
including surrounding transaction rollback. Parser/AST, source-derived SQL
builders, ordinary INSERT and LOAD DATA implementations remain unchanged.

Both regressions fail before removal: skip_rows=1 inserts both CSV rows, and
IMPORT FROM SELECT populates the target. Both pass after removal. Final targeted
tests, the existing statement-context test and prepared-command test pass with
none ignored. The wider DML module reports 17 passing and one failing test; the
same casting failure is independently reproduced on unchanged HEAD production.
This is not a passing full DML suite. Formatting, diff checks, root make lint,
all 85 Markdown/JSON register rows and retired-symbol checks pass.

Logs are retained in /private/tmp/tidb-import-removal-before.log,
-dml-after.log, -unrelated-baseline.log, -final.log, -stmt-context.log,
-prepared.log and -lint.log. The initial after log includes the rejected SET
test input; the corrected final log is the acceptance evidence. E05 remains
open; all 77 unresolved statuses are retained.

The actual commit hook passed the locked server build in 9.20 seconds; output
is /private/tmp/tidb-import-removal-commit.log. The separate post-commit
cd rust && cargo build --locked -p tidb-server passed in 12.47 seconds; output
is /private/tmp/tidb-import-removal-prepush.log. The final receipt amendment
reruns the hook and is followed by another fresh locked build before push.
Final logs use the corresponding -commit-final.log and -prepush-final.log names;
the task result records their outcome and the published commit.

Compatibility limitation: Rust now refuses both IMPORT INTO forms that the old
partial implementation accepted. Go supports them, so this is containment and
removal, not package parity. Complete importer/controller, file job/task and
SELECT lifecycle integration remain required. Native client-rust is unchanged.
Live TiKV imports, cancellation/restart recovery, full original Go suites and
workload benchmarks were not run. No performance improvement is claimed.
