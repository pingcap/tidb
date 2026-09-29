# Preserve Go's typed protobuf conversion boundary

This living ExecPlan follows `PLANS.md`.

## Purpose / Big Picture


Protobuf expression evaluation must preserve Go's source and result field types,
argument order, and statement diagnostic policy. The systematic audit under
`rust/docs/planner/pb-systematic-audit-20260928` supplies wire inputs and Go outputs.
The owning upstream completion unit remains all of `pkg/expression`, with explicit
`pkg/types` and coprocessor dependencies. This repair is seed evidence toward
that unit; it cannot certify whole-package parity.

## Progress


- [x] Read the audit, current conversion boundaries and Go cast implementations.
- [x] Pull integration branch and fetch Go master.
- [x] Establish failing persistent regression tests for typed conversion (441 rows over 21 signatures).
- [x] Preserve numeric source/result metadata and original conversion diagnostics; share the implementation with native AST casts.
- [x] Re-run all 2,154 differential rows and expression/coprocessor/session checks.
- [x] Run make lint and review the diff.
- [x] Numeric-boundary repair validated for publication; the commit/push result is recorded in Git history.
- [ ] Complete remaining package inventory, missing signatures, context/transport
  repairs and full package validation before any transcreation claim.

## Surprises & Discoveries


Go `ProduceDecWithSpecifiedTp` differs from `Datum.ConvertTo`: decimal scale loss
always appends a warning, while overflow returns an error for statement policy.
Unsigned negative production resets the existing decimal to zero. Replacing all
casts with generic datum conversion would therefore introduce new differences.

## Decision Log


- Decision: retain typed signature dispatch and add reusable production boundaries
  rather than route every signature through Datum.ConvertTo.
  Rationale: source conversion and result production have distinct Go contracts.
  Date/Author: 2026-09-28, Codex.

## Outcomes & Retrospective


The shared numeric boundary removes 80 of the original differential rows, with
no new differing rows. All 441 captured numeric cast rows now agree on value,
error disposition and warning count. Returned typed conversion errors also match
the captured Go error text. The audit still has 188 behavioral/warning differences
across 45 signatures, plus three ASIN rounding rows and 21 excluded Go panics.
The 331 missing decoder registrations remain open. This is a verified structural
repair and seed evidence, not an integrated/transcreated Go package claim.

All 427 datatype tests and 83 coprocessor tests pass. The expression suite has
1,240 passes and three baseline failures when its local HTTP listener is allowed;
93 tests are ignored. The session cast filter improves from 30 passes / five
failures on the baseline to 31 passes / four baseline failures: JSON numeric
conversion is repaired there too. No performance benchmark was run.

## Context and Orientation


`rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs` selects the protobuf
signature kernel. Its cast arm currently discards some FieldType metadata while
mapping to AST CastType. `rust/crates/tidb-expr/src/cast.rs` implements SQL casts,
and `rust/crates/tidb-datatype/src/datum_convert.rs` owns datum conversion and
result production. Request statement policy is supplied by `Columns` and
`tidb-unistore/src/cophandler/eval_context.rs::RequestEvalContext`.

## Plan of Work


Use full source/result fields at the protobuf cast boundary. Keep source
conversions distinct where Go does, especially integer signedness, JSON kinds,
and decimal parsing. Add a shared decimal producer with the original diagnostic
and unsigned rules. Tests must assert strict/warn/ignore behavior, including
warnings that Go deliberately appends independently of truncation mode.

Subsequent milestones address typed argument evaluation, temporal context,
transport and missing decoder families against their upstream package inventory.
Do not hide unresolved differences by changing expected Go results.

## Concrete Steps


Work from the repository root. Preserve the captured audit artifacts. Add
regression tests before implementation and run the targeted test filter; expect
failure before production changes. Re-run the audit's isolated probe runner to
measure the change against all 2,154 rows, restoring its temporary source edits.

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib typed_pb_conversion -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-datatype --lib
    make lint
    git diff --check

## Validation and Acceptance


A UINT64 maximum source must remain positive when cast to real. Invalid decimal
input must error, warn, or be ignored according to the statement flags. Decimal
result production must retain target precision/scale and unsigned semantics.
JSON conversion must inspect its kind rather than parse serialized JSON text.
Existing passing audit rows must not regress. New Rust-only tests do not trigger
Bazel preparation; re-evaluate if Go or Bazel metadata changes.

## Idempotence and Recovery


Tests are repeatable. Temporary audit probes restore original files in a finally
block; do not run their source-writing runner concurrently with edits. Preserve
unrelated branch updates when pulling and pushing; never force push.

## Artifacts and Notes


The baseline evidence and complete enum inventory are in the dated audit folder.
Append exact validation outcomes and remaining differences here after testing.

## Interfaces and Dependencies


Use existing FieldType, ConversionContext, Columns and EvalError types. Conversion
producers must return their typed error alongside the produced value so the
caller can apply statement policy once. No additional external dependencies or
new behavior beyond Go are authorized.

## Validation receipt, 2026-09-28


The persistent regression failed before production edits and passes afterward.
The exact commands, from the repository root, were:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib typed_pb_conversion -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-datatype --lib
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib cast -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib hybrid_arithmetic_and_binary_literal_casts_match_go_sql -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib json_schema_valid_resolves_file_and_http_references -- --test-threads=1
    python3 /private/tmp/run-pb-systematic-rust.py
    python3 /private/tmp/compare-pb-audit.py
    make lint
    git diff --check

The two temporary scripts are the preserved audit harnesses in the dated audit
folder; copy them to /private/tmp dropping their final .txt suffix for replay.
Their collector tests pass; the comparator still reports the remaining gaps.
Source files were restored after every temporary probe. The existing audit
captures were not overwritten. The Go reference source remains unchanged from
the master reference recorded in that audit.

The expression baseline failures are EXP overflow's expected error variant,
vectorized operator duration FSP, and partial STR_TO_DATE handling. The fourth
sandbox failure (HTTP schema reference listener) passes outside the sandbox.
The remaining session failures are assignment temporal diagnostic wording,
temporal IN conversion, nondeterministic Cartesian output ordering, and zero-date
warning wording. Each also fails with the unchanged branch. No test expectations
were relaxed to hide them.

The lint command initially could not download its pinned tool in the network
sandbox; rerunning with network access passed. No Go source, imports, dependencies
or Bazel metadata changed, so make bazel_prepare was not required. The final Rust
files were formatted with rustfmt --edition 2021 --config skip_children=true.

Modified production surfaces are cast.rs and scalar_function/pb_builtin.rs in
tidb-expr, and datum_convert.rs, decimal/mod.rs and lib.rs in tidb-datatype.
The request-level regression is in tidb-unistore's cophandler/eval_context.rs.
Broader diagnostic-code/warning-text completeness, temporal casts, typed argument
evaluation, request clock/transport, missing signatures, and benchmarks remain
unverified or open. The package completion gate remains unchecked.

Revision note: the implementation now routes native numeric casts through the
same typed conversion boundary and removes the obsolete decimal producer. Native
hybrid types and binary literals retain Go's numeric signature selection.
