# Restore generated read and backfill conversion ownership

## Purpose / Big Picture


A generated value read from stored bytes must retain the same column conversion
and diagnostics as Go master. A CHAR generated from trailing spaces must be
trimmed; a read truncation must publish its warning; forcing truncation suppression
must not hide a zero-date error. Query virtual filling also clips negative unsigned
results and substitutes bad NULL, whereas Go's general row decoder does neither.
This maintains existing executor/table reader owners, not complete acceptance of
pkg/table, pkg/util/rowDecoder, pkg/executor or DDL packages.

The integration baseline is 3397692dddc212fd6bfa9104d0268466ef2c41fc. Go master
is 93a01d31f6da205ae4bf376825293903a6899fdb; client-rust and its dependency are
current at 19a56ccda1e128218cd33c69709038219aced9bc. Root AGENTS.md and PLANS.md
apply. No deeper Rust instructions or Go table/rowDecoder doc.go exist. The DDL
read-first index was read; this change does not supply durable DDL orchestration.

## Progress


- [x] Pull both implementation branches and review current master contracts.
- [x] Trace row decoder, table readers, ANALYZE and union scan cast ownership.
- [x] Demonstrate five regressions against unchanged production.
- [x] Repair the shared raw column cast and migrate generated read consumers.
- [x] Verify virtual filling and decoder/DDL/executor typed error propagation.
- [x] Run focused checks and reconcile residual findings after concurrent integration.
- [x] Pass the actual commit hook and prepare the required fresh locked-build publication gate.

## Context and Source Contract


Go pkg/table/column.go::castColumnValue converts with the supplied type flags,
handles special temporal/charset cases, then calls types.Context.HandleTruncate
before forceIgnoreTruncate suppresses a remaining conversion error. That force
switch is not a change to type flags and does not skip early temporal failures.
Non-binary CHAR trailing spaces are trimmed after conversion. Raw column errors
have no INSERT row decoration.

pkg/util/rowDecoder/decoder.go::EvalRemainedExprColumnMap evaluates dependencies
in column order and calls CastColumnValue(false,true). FillVirtualColumnValue in
pkg/table/column.go additionally clips negative numeric values converted to
unsigned when AllowNegativeToUnsigned is set, then replaces NULL for NOT NULL or
PreventNullInsert. Table/point readers and ANALYZE use that virtual-fill owner.
UnionScanExec has its own source loop with the shared cast and NULL substitution.

Rust generated_column.rs currently bypasses the existing driver/write_cast.rs
column owner and drops conversion events. RowDecoder feeds it origin-default
flags forcibly changed to ignore truncation. ANALYZE uses the public fixed-flags
materialize wrapper. Table reads and general row decoding also share one
undifferentiated post-conversion policy. The existing raw cast entry point itself
uses write flags and does not honor force-ignore consistently, so routing callers
alone would not fix this boundary.

## Milestones and Implementation


First add regressions to existing row_decoder_source.rs and generated-column
session suites, plus direct shared-cast/ANALYZE cases where the source contracts
differ. Run them before production edits. Assertions cover values, warning
messages, failure identity and dependency ordering rather than source text.

Then give the existing column cast its caller's explicit type flags for raw
conversion; retain statement-completed mutation naming. Reuse the datatype
truncation owner rather than invent a second error allowlist. Preserve early
zero-date errors. Move rowDecoder and ANALYZE from the raw conversion wrappers
to that owner and remove the displaced wrappers. Keep query virtual-fill rules
explicit at table/point read and ANALYZE callers, separate from reorg decoding.
Carry MySQL diagnostics through decoder/DDL/executor errors instead of Debug text.

Finally validate the affected read/write, decoder, ANALYZE and union-scan surfaces.
Check shared write regressions because the conversion owner is common. Inspect
any failing test against unchanged production before attribution. Update K03
without accepting broader datatype, DDL, planner or execution packages.

## Validation and Publication


Commands run from rust/ will include scoped tests for generated-read regressions,
row_decoder_source, generated writes, ANALYZE virtual samples and union scan,
plus cargo check --locked for executor, exec, session and server all targets.
Exact commands and observed results will be recorded below. Root make lint and
git diff --check are required. Formatting excludes pre-existing unrelated churn.
No Go/Bazel/generated input changes are planned, so bazel_prepare is not triggered.
No original Go package tests, live TiKV/TiFlash or performance claims follow from
Rust fixtures or source comparison alone.

Commit with the actual hook:

    TERM=xterm git -c core.hooksPath=hooks commit

After the final commit/amend, from repository root:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Fetch and integrate concurrent updates without force, repeat affected gates, and
verify remote SHA and clean status. Native client-rust requires no update while
unchanged/current. Preserve unrelated edits; any temporary baseline control must
restore all saved production bytes even when a test fails.

## Surprises & Discoveries


The shared raw cast still uses mutation flags and INSERT-shaped diagnostics.
Go's forceIgnoreTruncate runs after HandleTruncate, so setting IgnoreTruncate in
the decoder loses warnings that should already have been appended.

## Decision Log


Decision: repair the shared column owner and distinguish source caller contracts,
rather than forwarding every read to the mutation adapter. Rationale: read,
reorg and mutation flags/NULL policies differ in Go; one blanket strictness or
ignore switch changes observable values. Date/author: 2026-10-02, Codex.

## Outcomes & Retrospective


Generated reads and ANALYZE now use the existing shared column-cast owner.
The public fixed-flags materialize wrapper and its raw conversion helper are
removed. Table/point readers and ANALYZE apply Go's virtual-fill rules, while
general rowDecoder/reorg preserves its distinct contract. In-process ANALYZE now
reads stored generated values as stored and fills only virtual values. The
RowDecodeContext carries complete caller type flags, including permissive DML's
warning level; those flags now serve defaults and all column conversion.

Column-cast and expression-evaluation errors cross table/DDL/executor boundaries
with their MySQL identity. Union scan also retains typed cast failures. K03 stays
partial: the shared lower datatype conversion still collapses some Go error/value
identities, the legacy ENUM/SET collation override is not represented at this
boundary, and end-to-end ANALYZE error publication still has string adapters.
These are not closed by replacing the generated-column raw conversion bypass.
No measured sysbench/TPC-C/TPC-H/YCSB speedup or complete package acceptance is claimed.


## Validation Evidence


The initial unchanged-production run failed three decoder regressions (CHAR
trimming before a dependent expression, query warning retention, early zero-date
failure) and two SQL regressions (point-reader raw warnings and virtual NULL
substitution). All five pass after repair. Additional tests distinguish reorg
from virtual filling, verify force-ignore after error/warn/ignore type policies,
retain DECIMAL rounding diagnostics, and cover ANALYZE virtual samples plus
production point/scan readers.

The complete affected decoder suite passes 16 tests; shared cast passes four;
SQL generated coverage passes 32; ANALYZE passes 15; union scan passes nine;
server generated coverage passes three. All-target checking passes for executor,
exec, session and server. Root GOTOOLCHAIN=go1.25.14 make lint passes. Existing
compiler/jemalloc/linker warnings remain. Full suites currently report executor
1469 passed/36 failed and session 330 passed/19 failed, with the same failing IDs
as the preceding repair. A fresh unchanged-production control confirms eight new regressions and two
strengthened existing checks fail before repair. It reports executor 1468/37,
session 328/21, decoder 11/5, and one failing ANALYZE and server regression each.
All repair production files were restored byte-for-byte after the control.
The [machine-readable evidence](parity/current-audit/generated-read-validation.json)
retains commands, results and failing test IDs.

No original Go package tests, live TiKV/TiFlash run or workload benchmark was run.
Validation uses existing in-memory/cop-backed fixtures and current-master source
contracts. No Go, Bazel or generated inputs changed.

Exact commands from rust/:

    cargo test --locked -p tidb-executor --test all generated_read_policy -- --test-threads=1
    cargo test --locked -p tidb-session --test all generated_read_policy -- --test-threads=1
    cargo test --locked -p tidb-executor generated_read_policy -- --test-threads=1
    cargo test --locked -p tidb-exec --lib generated_read_policy -- --test-threads=1
    cargo test --locked -p tidb-server --lib generated_read_policy -- --test-threads=1
    cargo test --locked -p tidb-executor --test all row_decoder_source -- --test-threads=1
    cargo test --locked -p tidb-executor --lib driver::write_cast -- --test-threads=1
    cargo test --locked -p tidb-session --test all generated -- --test-threads=1
    cargo test --locked -p tidb-executor --lib -- --test-threads=1
    cargo test --locked -p tidb-session --test all -- --test-threads=1
    cargo test --locked -p tidb-exec --lib cluster_analyze -- --test-threads=1
    cargo test --locked -p tidb-executor union_scan -- --test-threads=1
    cargo test --locked -p tidb-server --lib generated_ -- --test-threads=1
    cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets

Root gates: GOTOOLCHAIN=go1.25.14 make lint and git diff --check. Changed Rust
files were formatted with rustfmt --edition 2021 --config skip_children=true
--emit stdout; unchanged pre-existing formatting hunks were restored rather than
included as unrelated churn. The final diff and formatting comparison must pass
before publication.


## Integration and Files Changed


Remote loader commit c4552c8757 arrived during controls and was integrated by
`git merge --ff-only origin/hparser-integration`. All 79 focused checks, all-target
compilation and lint pass on that integrated tree before publication;
full-suite comparisons above remain anchored to 3397692ddd.

Production changes are in tidb-executor's driver/write_cast.rs, generated_column.rs,
kv_table.rs, kv_table/row_decoder.rs, kv_table/table_meta.rs, executor.rs,
driver/dml.rs, ddl/indexes.rs, analyze/kv.rs and union_scan.rs, plus tidb-exec's
cluster_analyze/virtual_samples.rs. Tests extend the existing executor decoder and
cast suites, session generated-column suite, exec cluster_analyze tests and server
unistore_cop tests. This receipt, the main plan, audit registers and machine
validation evidence record scope and residual limits.


Final review removed the shared caster's unconditional string-payload clone:
it borrows the original Datum for diagnostics and owns a replacement only for
charset substitution. The 79 focused tests and all-target checks were repeated
after that allocation change. No new formatting hunks or whitespace errors are
introduced. The complete-suite controls precede this ownership-only adjustment;
they were not rerun without a new behavioral concern. No workload speedup is
inferred from eliminating a clone.

## Publication Gates


The actual `TERM=xterm git -c core.hooksPath=hooks commit` passed its mandatory
`cd rust && cargo build --locked -p tidb-server`. This receipt-only amendment
uses the same hook. After the final amendment, publication reruns that build and
pushes only on success:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify HEAD against `git ls-remote origin refs/heads/hparser-integration` and
verify `git status --short`. Native client-rust and the dependency remain current
at 19a56cc and are unchanged by this reader maintenance.
