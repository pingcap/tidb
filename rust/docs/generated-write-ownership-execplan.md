# Give generated writes the statement's conversion owner

## Purpose / Big Picture


Generated columns must use the same statement conversion, warning and NULL
policy as ordinary assignments. A strict TINYINT generated from 1000 must fail;
a permissive or IGNORE write must retain its warning and converted value. Each
value must be evaluated once before constraints/index writes consume it. This
repairs existing mutation owners; it is not acceptance of complete Go executor,
table, DDL or expression packages.

Fresh integration is 793c67cae425ac6863ca1050071be05a8986ea73, Go master is
93a01d31f6da205ae4bf376825293903a6899fdb, native client-rust is current at
19a56ccda1e128218cd33c69709038219aced9bc. Root AGENTS.md/PLANS.md apply; no
deeper Rust instruction file or owning Go executor/table doc.go was found.

## Progress


- [x] Pull latest branches and verify K03's live conversion/caller mismatch.
- [x] Compare master fillRow, updateRecord, CastValue and rowDecoder contracts.
- [x] Add and run failing statement regressions before production changes.
- [x] Share statement-owned generated conversion and remove storage reevaluation.
- [x] Migrate INSERT/UPDATE/ODKU/cascade callers and review read/DDL boundaries.
- [x] Validate the integrated tree and reconcile the finding as partial.
- [x] Commit through the actual hook; publish only through the fresh locked-build gate below.

## Context and Orientation


Go pkg/executor/insert_common.go fillRow evaluates/casts/NULL-checks each
generated column before the next dependency. write.go updateRecord casts each
expression under its caller's error handler, then handles NULL across the row
after all expressions finish. pkg/table/column.go CastValue uses the
active type/error context. pkg/util/rowDecoder/decoder.go is a distinct read/
backfill contract: CastColumnValue(false, true) suppresses truncation there.

Before this repair, generated_column.rs converted using fixed flags and discarded
conversion events. driver/dml.rs and dml/update_record.rs called it before
constraints, then KvTable insert_row_in/update_row_in repeated it. Pure
values may be idempotent, but warnings and evaluations are observable effects.
foreign_key.rs directly rewrites cascade rows and must materialize before
nested dependents inspect them. partition_maintenance moves already-decoded
rows and must preserve the values rather than reevaluating expressions.

## Plan of Work


First extend tidb-session/tests/generated_column_write_source.rs with strict,
permissive, IGNORE, dependent NULL, INSERT/UPDATE/ODKU and index checks. Run the
new tests against unchanged production and retain actual failing results.

Extract the existing left-to-right evaluation loop behind a caller-supplied
conversion callback. The mutation adapter uses driver/write_cast.rs and
bad_null.rs with INSERT, UPDATE and ODKU's existing error naming and row index.
Migrate all mutation callers; remove autonomous generation from byte-storage
insert/update. Update low-level fixtures to supply the materialized row that
Go Table.AddRecord expects. Exercise FK cascades before nested constraints.

Keep read/backfill selection explicit: they are not INSERT and must not inherit
strict INSERT NULL policy. Review their remaining fixed/raw conversion behavior
against master before deciding K03's final status. Do not claim broader casting,
DDL reorganization or generated-expression parity merely because mutation tests
pass. Missing shared datatype/error identities remain named limitations.

## Validation and Acceptance


From rust/, start with:

    cargo test --locked -p tidb-session --test all generated_write_policy -- --test-threads=1

Then run affected generated/NULL/strict conversion/FK integration groups,
executor generation/row-decoder tests, and all-target checks for touched crates.
Use bounded scoped tests; distinguish baseline failures with unchanged controls
where necessary. Root make lint, targeted formatting review and git diff --check
are required. There are no Go/Bazel inputs, so bazel_prepare is not triggered.
No original Go package test or live cluster is implied by source inspection.

Publication uses the actual hook:

    TERM=xterm git -c core.hooksPath=hooks commit

After the final commit/amend, immediately before push:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify remote HEAD and a clean tree. Never force-push; integrate concurrent
remote changes and repeat affected gates. No native dependency update is needed
unless native client-rust actually changes.

## Surprises & Discoveries


Storage-level reevaluation currently hides the absence of a statement policy.
Forwarding warnings without removing that reevaluation would report them twice.
Go's read/backfill CastColumnValue suppression is deliberate, so replacing all
materialization flags with strict write flags would introduce a second defect.

## Decision Log


Decision: carry statement policy at mutation materialization, with one shared
expression/dependency loop; remove storage's independent evaluation.
Rationale: warning/error/NULL effects belong before constraints, and dependencies
must see the converted value, with INSERT's NULL substitution occurring before
the next expression and UPDATE's occurring after all expressions. A flags-only
patch cannot establish that lifecycle. Date/author: 2026-10-02, Codex.

## Outcomes & Retrospective


K03 is **partial**: the mutation owner now supplies the existing ordinary-column
cast policy, warning handling and source-specific NULL ordering to generated
expressions. INSERT, INSERT SELECT, REPLACE, prepared writes, single/joined
UPDATE and ODKU reach that owner. Cascades materialize before building nested
parent changes. Storage's second expression pass and row clone are removed.
The low-level decoder fixture now supplies a completed row, like Go AddRecord.

Remaining K03 scope is explicit: rowDecoder/DDL/ANALYZE still use raw datatype
conversion instead of complete Go CastColumnValue semantics. Their deliberate
force-ignore-truncation contract must not be replaced with strict INSERT rules.
The shared write cast also has previously documented datatype/error-identity
gaps. No complete pkg/executor, pkg/table, DDL or expression acceptance follows
from this repair. No live TiKV/TiFlash or measured sysbench/TPC-C/TPC-H/YCSB
performance claim; removal of redundant evaluation is not a benchmark result.

## Files and Validation Evidence


The implementation changes generated_column.rs, driver/write_cast.rs, driver/dml.rs,
driver/dml/update_record.rs, driver/multi_dml.rs, foreign_key.rs and kv_table.rs
under rust/crates/tidb-executor/src. Regression coverage is in the existing
tidb-session/tests/generated_column_write_source.rs and tidb-server's
cluster_session_node/tests/unistore_cop.rs. The existing row_decoder_source.rs
fixture is adjusted to the low-level table contract. This receipt and the
structural registers record the bounded result.

Initial unchanged production failed all three first regressions. After the
full nine regressions were added, an unchanged-production control at 793c67cae4
failed all nine. Both control and repair retained identical tests; only the
changed production files were temporarily restored to baseline, then restored
byte-for-byte to the repair in a finally block. The
[machine-readable comparison](parity/current-audit/generated-write-validation.json)
retains every failing test ID and exact suite commands. The full executor suite
remains 1468 passed/36 failed on both sides. The session suite changes from
319 passed/28 failed to 328 passed/19 failed: only the nine new regressions change
status, with no new failing test IDs.

Exact Rust commands run from rust/:

    cargo test --locked -p tidb-session --test all generated_write_policy -- --test-threads=1
    cargo test --locked -p tidb-session --test all generated -- --test-threads=1
    cargo test --locked -p tidb-server --lib generated_write_policy -- --test-threads=1
    cargo test --locked -p tidb-executor --test all row_decoder_source -- --test-threads=1
    cargo test --locked -p tidb-executor --lib generated -- --test-threads=1
    cargo test --locked -p tidb-executor --lib -- --test-threads=1
    cargo test --locked -p tidb-session --test all -- --test-threads=1
    cargo check --locked -p tidb-executor -p tidb-session -p tidb-server --all-targets

Focused results before the concurrent upstream integration: 30 session generated
tests, one cluster-session prepared-write test and 12 decoder tests pass. The
executor generated filter passes 11 with one pre-existing ANALYZE failure,
also confirmed in both full-suite controls. Full-suite runs are justified by
removing implicit generation from both low-level table mutation methods.

Root `GOTOOLCHAIN=go1.25.14 make lint` and `git diff --check` pass before integration.
Formatting each changed Rust source on stdin with
`rustfmt --edition 2021 --config skip_children=true --emit stdout` introduces
no new formatting differences compared with formatting HEAD's same source.
Four files retain 18 pre-existing formatting hunks; unrelated churn is excluded.
Six changed Rust files are fully rustfmt-clean. No Go or generated inputs change,
so bazel_prepare and Go failpoint gates do not apply. Original Go tests,
distributed-cluster tests and workload benchmarks were not run.

Concurrent remote commit 638b8ea013 (direct-path DDL history) arrived after the
controls. `git merge --ff-only origin/hparser-integration` integrated it without
overlap. All-target compilation, the 43 focused tests, root lint and publication
gates apply to the integrated tree. All-target compilation, all 43 focused tests
and root lint have passed again after integration; the full-suite control results
above remain explicitly anchored to 793c67cae4. Existing compiler warnings remain.

The first push was rejected after another non-overlapping DDL update, e8c7c9211d
(MODIFY/CHANGE column defaults), reached the branch. `git rebase
origin/hparser-integration` preserved both changes. The all-target check, 43 focused tests and root lint passed again on the rebased
tree. Publication also repeats the actual commit-hook build and fresh pre-push
build. The full-suite baseline comparison retains
its original anchor; no force push is used.

## Publication Gates


The actual `TERM=xterm git -c core.hooksPath=hooks commit` passed its mandatory
`cd rust && cargo build --locked -p tidb-server`. This receipt-only completion
uses the same hook for amendment. After the final amendment the publication
command reruns the build, then pushes only on success:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify HEAD against `git ls-remote origin refs/heads/hparser-integration` and
verify `git status --short`. Native client-rust and the pinned dependency are
current at 19a56cc and unchanged by this executor maintenance.

## Idempotence and Recovery


Tests use isolated in-memory sessions and existing fixtures. Preserve unrelated
work. Keep baseline controls outside the checkout or restore them byte-for-byte
before validation. No generated source or Go reference is edited. The new
callback is internal to the existing executor; no protocol/dependency API changes
are required.
