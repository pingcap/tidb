# Preserve Go typed protobuf evaluation and metadata

This living ExecPlan follows PLANS.md. It repairs existing expression execution;
it does not claim completion of an upstream Go package transcreation.

## Purpose / Big Picture


A protobuf signature must retain Go's typed argument evaluation, statement error
policy, full string field type, and constructor collation metadata. Strict
IfInt(1,"invalid",0) and RealIsNull("invalid") must error, binary VarString(2)
casts must truncate "abcd" to "ab", and IfNullString literals must carry
coercibility 4 (the collation precedence of a literal), not 2 (a column).

## Progress


- [x] Confirm four failing Rust probes and passing Go reference probes.
- [x] Pull integration branch (f71f7a59d5); identify shared ownership boundaries.
- [x] Preserve regression tests and repair typed control/NULL evaluation.
- [x] Preserve full string field types and initialize protobuf coercibility.
- [x] Validate, review, and prepare a commit for hparser-integration.

## Context and Orientation


Rust's shared wire decoder is rust/crates/tidb-expr/src/distsql_builtin.rs.
The selected PbBuiltin kernel in scalar_function/pb_builtin.rs evaluates its
arguments; scalar_function.rs currently supplies a generic final conversion.
Go's references are pkg/expression/{distsql_builtin,builtin_control,builtin_op,
builtin_cast,collation}.go at master 4b2221cddf. The reference archive at
/private/tmp/tidb-go-structural-20260927 has identical relevant files.

String production is the operation enforcing field length and reporting
truncation before optional binary padding. tidb-datatype's datum_convert.rs
already implements it; expose a contextual entry point to reuse it without
introducing charset transformations from general Datum.ConvertTo.

## Milestones and Plan of Work


First keep the four probes from /private/tmp/pb-followup-audit-tests.rs in the
existing unistore request-context suite and rerun them before implementation.
Retain EvalType in Case/If/IfNull/IsNull kernels, evaluate conditions as Int,
and evaluate only the selected branch in its signature domain. Keep lazy
control flow, NULLs, and request warnings/errors intact.

Next share contextual string production from tidb-datatype with protobuf casts.
Preserve target type and length, use byte lengths for binary strings, and pad
only fixed binary fields. Check max_allowed_packet before allocating padding.
Use existing source rendering, including unsigned carriers and YEAR zero.
Derive collation with the generated wire signature name in from_pb, matching
Go's newDistSQLFunctionBySig, and retain wire charset/collation for evaluation.

The typed decoder also exposes a missing Go builder step: wrap non-integer
CASE/IF/AND/OR conditions with the appropriate IsTrueWithNull signature before
encoding. Add Real/Decimal truth signatures and preserve the existing Int path
in pushdown_catalog.rs, without inserting lossy casts to Int.

Finally extend family, strict/warning, NULL/lazy, padding/truncation, and metadata
regressions; run the targeted commands below and make lint. Review and publish
the resulting existing-package bug fixes, without declaring full package parity.

## Decision Log


- Preserve typed domains in kernels instead of converting generic outputs after
  evaluation. Default conversion flags cannot preserve statement error policy.
  2026-09-28.
- Reuse the native datatype string producer with diagnostics; do not translate
  a complete wire field type into the narrower SQL AST cast descriptor.
  2026-09-28.
- Use generated enum names for Go's collation dispatch. Do not invent SQL-name
  aliases for protobuf signatures or overwrite the wire comparison collation.
  2026-09-28.

## Surprises & Discoveries


All four audit probes fail in Rust and pass in Go. Rust returned Int(0) for both
strict conversion-error cases, "abcd" for the bounded binary cast, and
coercibility 2 instead of 4. Audit evidence is /private/tmp/pb-followup-audit.md.
Warning-mode probes additionally exposed String literals decoded as KindBytes;
Go SetBytesAsString retains KindString. Fixing the literal decoder preserves
both native typed conversion errors and their warning/ignore alternatives.
The pushed conditional test then exposed the missing wrapWithIsTrue builder:
its old comment confused args[i].GetType() after wrapping with the original
argument type. The shared builder now supplies the Go truth wrapper.

## Validation and Acceptance


Run from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-datatype --lib datum_convert -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

From /private/tmp/tidb-go-structural-20260927, use the failpoint wrapper:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-next-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustFollowupPBBoundaries$' -count=1

Require four formerly failing regressions to pass, correct warning/error policy,
unchanged lazy evaluation, and the same result metadata as Go. Rust-only edits
need no bazel_prepare. No benchmark or full-package parity claim is made.

## Idempotence and Recovery


Keep temporary probes outside the checkout when not used. Restore temporary
baseline source changes with finally guards. Do not edit Rust sources during a
Cargo build. Pull before publication and preserve concurrent changes.

## Outcomes & Retrospective


The identified contracts now follow Go: selected typed argument domains and
request error policy are retained; String wire literals remain string datums;
string casts retain full target type and length, with Go's separate fixed binary
padding and allocation guard; and the common constructor initializes coercibility
using generated enum names. Non-integer CASE/IF/AND/OR conditions now receive
Go's typed truth wrapper in the builder, rather than relying on generic truth
conversion in the decoder.

The original four regressions failed before the fix. Warning-mode extension
initially failed with an unsupported integer-domain error, exposing the wrong
String datum kind; it passes after repairing the literal decoder. The existing
server fractional-CASE integration test failed with the typed decoder and passed
after the builder repair. Final focused checks pass:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1`: 77 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1`: 17 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib control_conditions_use_go_typed_truth_wrappers -- --test-threads=1`: passed, covering real/decimal/string wrappers, integer bypass, fractional truth and NULL.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-datatype --lib datum_convert -- --test-threads=1`: 32 passed.
- `cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets`: passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1`: 3 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all wide_scan_selection_source -- --test-threads=1`: 16 passed.
- `GOTOOLCHAIN=go1.25.12 make lint`: passed, including protobuf schema checks.
- `git diff --check`: passed.

The final Go reference command, run from the archive named above, was:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-next-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRust(FollowupPBBoundaries|TypedStringFields|TypedConditionBuilders)$' -count=1

It passed, including the binary-field strict/warn/ignore matrix and Go builder
signature checks. The wrapper returned its failpoint refcount to 0. Git-metadata
diagnostics are expected from this archive without .git. Relevant Go source
files, including expression.go, have no diff from archive revision 8936d7bdcb
to origin/master 4b2221cddf. Temporary logs use /private/tmp/pb-typed-*.log.

Changed files:

- rust/crates/tidb-datatype/src/datum_convert.rs
- rust/crates/tidb-datatype/src/lib.rs
- rust/crates/tidb-expr/src/cast.rs
- rust/crates/tidb-expr/src/distsql_builtin.rs
- rust/crates/tidb-expr/src/pushdown_catalog.rs
- rust/crates/tidb-expr/src/scalar_function.rs
- rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs
- rust/crates/tidb-unistore/src/cophandler/eval_context.rs
- rust/crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs (corrected the stale explanation of Go's wrapper)
- rust/docs/planner/pb-typed-contract-execplan.md

Compatibility changes are intentional: strict conversions previously suppressed
by generic evaluation now fail, warning/ignore modes retain Go's value and warning
behavior, bounded strings truncate correctly, and coercibility metadata changes
from the column default to Go's derived value. Fixed binary padding is checked
against max_allowed_packet before allocation. No new dependency or generated-code
edit is involved; no Go/Bazel files changed, so bazel_prepare was unnecessary.

Complete upstream package inventory/test translation, full-suite parity, SQL
workload validation, and sysbench/TPC-C/TPC-H/YCSB measurements remain unverified.
Existing unrelated full-suite failures recorded in earlier plans are not resolved
or reclassified by this work. This is an existing-package correctness checkpoint,
not a package-transcreation completion or a measured performance improvement.
Publication target is origin/hparser-integration with a normal fast-forward push.
