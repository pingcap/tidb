# Use one Go-shaped protobuf expression decoder

This living ExecPlan follows PLANS.md. It extends the expression and unistore
package ports; it does not claim completion of either whole upstream package.

## Purpose / Big Picture


An expression accepted as a child must be accepted inside any compatible parent.
Comparisons must evaluate both operands before applying NULL semantics, and wire
enum values must never silently become NULL because the local schema is partial.

## Progress


- [x] Reproduce three differences against Go master; refresh integration branch.
- [x] Preserve the failing regression tests and inventory existing decoder signatures.
- [x] Regenerate complete wire enums and reject unrecognized expression values.
- [x] Move Go-supported legacy signatures to the shared typed registry and remove the production legacy decoder.
- [x] Match Go comparison order and ValueList handling.
- [x] Validate and review the checkpoint; prepare the publication receipt.

## Context and Orientation


The prior audit is /private/tmp/pb-next-audit.md. Rust started from integration
commit 5ce0af8bdd and was fast-forwarded to 29a3cf369f before publication. Go master is origin/master, refreshed to 4b2221cddf.
The relevant Go sources are pkg/expression/distsql_builtin.go,
builtin_compare.go, builtin_other.go and builtin_time.go. Rust has a shared typed
registry in tidb-expr/src/scalar_function/pb_builtin.rs and a separate legacy
signature match in tidb-unistore/src/cophandler.rs. The latter can decode children
that the former cannot. The shared decoder lives in distsql_builtin.rs.

## Plan of Work


First retain the audited regressions and record the legacy signature inventory.
Extend the existing schema synchronizer to regenerate all three dispatch enums
from pinned upstream inputs; make descriptor validation require complete enums.
Handle unknown expression discriminants explicitly instead of the protobuf
getter's default. Implement Go's empty ValueList shortcut and decode list members
through the datum codec, rejecting untyped nonempty lists instead of reproducing Go’s nil-type panic.

Move the Go-supported legacy signature families into the shared typed registry,
reusing existing arithmetic, comparison, LIKE, IN and temporal kernels. Remove
scalar fallback decoding from production unistore. Keep legacy test fixtures
explicitly outside the production decoder until their individual tests migrate.
Do not add SQL-name dispatch. Comparison kernels evaluate both operands, preserve
unsigned metadata, then apply NULL semantics. Arithmetic and lazy control flow
retain their separate evaluation contracts.

Finish with focused unit and server pushdown tests, protocol checks, server
all-target compilation, make lint, and a clean diff review. Commit and push to
hparser-integration without force-pushing or claiming whole-package completion.

## Decision Log


- Chosen: one recursive shared decoder and enum-selected kernels. Adding GtReal
  alone would leave the same nesting bug for the other legacy families.
  2026-09-28.
- Go's decoder rejects several specialized signedness arithmetic signatures that
  the old Rust fallback admitted. Preserve Go's rejection rather than add support
  outside the reference. 2026-09-28.
- Current Go panics when nonempty ValueList constants lack field types during
  collation derivation. Preserve codec expansion but reject untyped arguments explicitly;
  do not reproduce a panic or silently return NULL. Empty lists explicitly produce
  FALSE in Go. Validate this distinction separately. 2026-09-28.

## Surprises & Discoveries


The audited nested IfInt(GtReal(1,0),1,0) fails with an unsupported child signature.
EqInt(NULL, strict-invalid-cast) returns NULL instead of the cast error. Empty
ValueList uses wire number 151, absent from Rust's schema, and returns NULL instead
of FALSE. The old schema check passes because ExprType is declared partial.

## Validation and Acceptance


Run from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-proto --test tipb_selection_expression_source
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

The three audited regressions must fail before and pass after. Add cross-family
nesting coverage and both operand orders for comparisons; prove lazy control flow
still skips unused branches. Exercise recognized unsupported and unknown wire IDs.
The prior Go overlay /private/tmp/pb-next-overlay.json verifies the three source
contracts using tools/check/failpoint-go-test.sh. Use the failpoint wrapper for
reference tests and confirm cleanup. Rust-only changes do not require Bazel prep.

## Idempotence and Recovery


Generate schema inputs using the synchronizer; never edit generated Rust outputs.
Keep temporary baseline source restoration guarded by finally blocks. Preserve
concurrent work and fetch before publication. Do not run benchmark workloads or
claim speedups without measurements.

## Outcomes & Retrospective


Implemented and validated the decoder checkpoint. The production scalar fallback
is removed. All 218 former legacy dispatch entries were inventoried: 203 now
resolve through the shared typed registry; the remaining 15 are specialized
PlusInt/MinusInt/IntDivideInt variants absent from Go getSignatureByPB and are
explicitly covered by refusal tests. This count describes dispatch coverage, not
proof of all builtin semantics or a completed Go package port.

The initial three coprocessor regressions failed before implementation, as did
unknown ExprType and ValueList enum tests. They now pass. Additional discriminator and unsigned-multiply regressions were
verified to fail with their respective fixes removed, then restored and passed.
A controlled schema
mutation deleting ValueList was rejected by both schema scripts, then restored.
Relevant Go expression sources have no diff from reference 8936d7bdcb to refreshed
master 4b2221cddf. The Go reference tests pass and failpoint refcount returns to 0.

Validation commands and results (repository root unless specified):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1`: 16 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1`: 70 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-proto --test tipb_selection_expression_source`: 4 passed.
- `cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets`: passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1`: 3 passed (host access required by embedded-store memory detection).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib date_add -- --test-threads=1`: 3 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib like -- --test-threads=1`: 55 passed, 9 pre-existing ignored tests.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all wide_scan_selection_source -- --test-threads=1`: 16 passed.
- `python3 rust/scripts/sync-tipb-scalar-func-sig.py`: all three complete dispatch enums match the pinned module.
- `python3 rust/scripts/check-tipb-proto-projection.py`: 44 messages and 9 enums match the pinned module.
- `GOTOOLCHAIN=go1.25.12 make lint`: passed, including both schema checks.
- In `/private/tmp/tidb-go-structural-20260927`: `GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-next-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustNextPBBoundaries$' -count=1`: passed. The archive emits harmless Git-metadata diagnostics because it has no .git directory.
- `git diff --check`: passed.

Files changed are the shared expression decoder, typed protobuf kernel registry,
shared scalar LIKE/date helpers and date return-type helper; the unistore decoder
and request-context regression suite; the TiPB source projection and wire tests;
both schema scripts; and this plan. No Go/Bazel source or module changes were made,
so bazel_prepare was not required.

Compatibility: unknown enum discriminants now error instead of defaulting to
NULL/table scan; empty packed lists produce FALSE; nonempty untyped lists return
an explicit error where current Go panics. Fifteen Go-rejected numeric variants
are no longer accepted by the old fallback. Existing valid nested signatures use
one shared execution path. Legacy manually constructed evaluator fixtures remain.

Risks and limits: complete upstream package inventories/test translations are
still absent, and these results do not establish full expression or unistore
package parity. No full-suite claim is made; prior unrelated failures remain
recorded in pb-boundaries-execplan.md. Sysbench/TPC-C/TPC-H/YCSB benchmarks and
RealTiKV were not run, so no performance improvement is claimed. Moving legacy
wire calls through the shared row evaluator can change allocation costs.

Changed files:

- `rust/crates/tidb-expr/src/distsql_builtin.rs`
- `rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs`
- `rust/crates/tidb-expr/src/scalar_function.rs`
- `rust/crates/tidb-expr/src/rewriter/result_type.rs`
- `rust/crates/tidb-unistore/src/cophandler.rs`
- `rust/crates/tidb-unistore/src/cophandler/eval_context.rs`
- `rust/crates/tidb-proto/proto/select.proto`
- `rust/crates/tidb-proto/tests/tipb_selection_expression_source.rs`
- `rust/scripts/sync-tipb-scalar-func-sig.py`
- `rust/scripts/check-tipb-proto-projection.py`
- `rust/docs/planner/pb-recursive-decoder-execplan.md`

Publication target: `origin/hparser-integration`, using a normal fast-forward
push after committing this validated checkpoint.
