# Projection and positional regexp structural audit

This audit extends the earlier grammar/policy audit into projection execution
and the positional regexp family. No production code changed. These are
reproduced structural findings and seed evidence, not a completed Go-package
transcreation or an exhaustive parity claim.

## Reference and method

Rust: `c8d2a9757630bdf8afd9684971003a570e387b36`, after fetching and fast-forwarding
`hparser-integration`. Go master: `12b639a1161cd5a60126a47277f5ad14c320fd4a`.
The Go execution archive remains `8936d7bdcb` at
`/private/tmp/tidb-go-structural-20260927`, using Go 1.25.14. Its `pkg/expression`,
`pkg/types`, `pkg/util/mock`, and `pkg/sessionctx/stmtctx` sources have no diff
against this master. This equivalence is limited to those paths.

`source-inventory.json` records all tracked artifacts in those Go subtrees with
Git blob identities. Subpackages are separate completion units. It does not
claim that every production, generated/platform variant, original test/support
artifact, fixture, integration decision or required gate has been ported.

Two new differential matrices exercise paths absent from the earlier PB audit:

- **19,740 positional regexp cases**, through Go's actual builtin factory/scalar
  evaluator and Rust's native ScalarFunction evaluator. Inputs include valid and
  invalid UTF-8 byte strings, 12 patterns, positions, occurrences, replacements,
  NULLs and failing patterns. Text arguments are explicitly utf8mb4_bin typed;
  invalid bytes are preserved in the datum rather than silently normalized.
- **120 integer-to-decimal shapes**, combining source/target signedness, widths
  10/30, scale 2, five integer carriers, and strict/ignore/warning policies.
  Each executes through Go scalar, vector, projection-on and projection-off
  routes: 480 observations. Rust adds its dedicated decimal fast path: 600
  observations. Go uses BuildCastFunction and InitFromPBFlagAndTz; Rust uses the
  corresponding typed column/function shape and explicit context settings.

The SQL probe additionally reaches the Rust Session and client error conversion.
The Go side is expression/evaluator execution, not a full SQL server connection.

## Confirmed findings

### 1. Projection has competing conversion implementations

For signed BIGINT `9223372036854775807` cast to DECIMAL(10,2) in warning mode,
all four Go routes return `99999999.99` and warning 1690. Rust scalar and
projection-off do the same, but its numeric batch and projection-on return
error 1690 with no warning. Ignore mode similarly errors on those batch routes
instead of returning the clamped value without a warning.

This is observable through normal Rust SQL:

```sql
create table audit_decimal(a bigint);
insert into audit_decimal values(9223372036854775807);
set tidb_enable_vectorized_expression=1;
select cast(a as decimal(10,2)) from audit_decimal; -- Rust: error 1690
set tidb_enable_vectorized_expression=0;
select cast(a as decimal(10,2)) from audit_decimal; -- Rust: 99999999.99
show warnings; -- Rust: warning 1690
```

The batch path in `tidb-expr/src/scalar_function.rs::eval_numeric_batch_values`
uses `convert_numeric_datum`, which returns `DatumConversion.error` immediately.
Go's `builtinCastIntAsDecimalSig.vecEvalDecimal` sends the error from
`ProduceDecWithSpecifiedTp` through `errCtx(ctx).HandleError`, just like its
scalar signature. A type conversion context does not replace the statement's
error policy. A root repair must share the typed conversion/error lifecycle
across scalar, vector and projection entrypoints.

### 2. Decimal fast path overrides a correct general batch result

For signed integer carrier -1 and an unsigned DECIMAL(30,2) target, Go returns
`18446744073709551615.00` on all four routes. Go's explicit non-UNION cast reads
unsigned carriers when either source or target is unsigned. Rust's general
numeric batch agrees, but its scalar, decimal fast path and both projection
settings return `-1.00`. The signed minimum has the same defect.

`vec_eval_cast_int_as_decimal` in `scalar_function.rs` reads signedness only
from the source. `tidb-expr/src/evaluator.rs::EvaluatorSuite::run` selects this
path before checking `enable_vectorized_expression`; thus even disabling
vectorization does not prevent this fast path from running. The general batch
implementation already checks both source and target flags, but the suite
preempts it. A local fix to only the general batch kernel would never fix the
actual projection result.

The fast path accepts 60 of the 120 shapes; six accepted observations disagree
with Go. They are additional route observations of the same signedness defect,
not six independent bugs. Unsigned target metadata is exercised directly;
this audit does not establish which SQL planner rewrites produce every such
shape, and does not equate ordinary SQL DECIMAL casts with UNSIGNED targets or
Go's distinct UNION cast behavior.

Across the four comparable routes there are **72 differing rows in 29 shapes**:
11 scalar, 22 batch, 28 projection-on, 11 projection-off. All 72 differ in
result/state, and 24 also differ in warning count/content. Go's four routes have
identical results, errors and warnings for every shape. Neither implementation
panics in the final decimal matrix. Error-message equivalence when both return
an error is not included in the difference count.

### 3. Positional regexp has a different byte-string boundary from REGEXP_LIKE

Case `9608`: REGEXP_SUBSTR on bytes `ff61`, pattern `.`, position 1, occurrence 1
returns byte `ff` in Go. Rust rejects the input as invalid UTF-8.
Case `1926`: REGEXP_REPLACE on `a`, pattern `.`, replacement byte `ff`, position 1,
occurrence 0 returns `ff` in Go; Rust rejects the replacement.

The previous fix gave REGEXP_LIKE a Go-compatible decoding path, but
`tidb-expr/src/builtin_ext/regexp.rs::{required_string,optional_string}` still
calls `coerce_str`, rejecting invalid bytes. Captures and replacement output are
also tied to Rust `&str`. Go matches byte strings, preserves original bytes in
substring/replacement output, and separately computes character positions.
Replacing the whole input with lossy UTF-8 is not a root fix: the first example
would return `efbfbd` instead of the original `ff`. Matching, original-byte
ranges, character positions and replacement bytes need one consistent contract.

### 4. Shared regexp validation changes signature-specific behavior

Case `41`: `REGEXP_INSTR('', '.', 2, 1, 0)` returns 0 in Go and errors in Rust.
Case `361`: the same input and position with `a*` returns 2 in Go and errors in
Rust. Go INSTR permits a positive position beyond an empty string, while the
Rust `trim_at` helper imposes one shared rule on INSTR, SUBSTR and REPLACE.
This mismatch is also reproduced through Rust SQL.

Case `19272`: `REGEXP_SUBSTR('a', '', NULL, 1, NULL)` errors on the empty pattern
in Go but returns NULL in Rust. INSTR and REPLACE have analogous cases. Go checks
the empty pattern immediately after evaluating it. Rust delays compilation
until after optional arguments and returns early on their NULLs. The SQL probe
confirms the simpler three-/four-argument forms have the same behavior.

These are per-signature control-flow contracts. Shared parsing/matching kernels
are appropriate, but generic argument collection and a single validator must
not replace each Go signature's validation and early-return order.

## Positional matrix accounting and exclusions

There are **5,116 differing rows**. They partition into:

- **3,128 runtime result/state differences** without Go panics or factory errors.
  These include 2,881 Go values and 217 Go NULLs rejected by Rust, plus 30 Go
  errors suppressed into Rust NULLs.
- **1,964 Go panics**, chiefly byte/rune slicing on invalid text and absent
  capture substitutions. Rust does not panic. These are excluded from Rust
  behavioral defect counts; a fix must not reproduce unsafe panics.
- **24 Go factory rejections** associated with binary collation inferred for
  NULL-typed arguments. Rust's directly constructed ScalarFunction returns NULL.
  They expose a builder-boundary difference but are not counted as equivalent
  runtime inputs or established planner-generated SQL bugs.

Among the 3,128 runtime differences, 222 use only valid UTF-8: 192 INSTR
empty-input position cases and 30 empty-pattern/NULL-order cases. The other
2,906 involve invalid text/pattern/replacement bytes. The counts overlap
parameter combinations; they are not independent root causes. Error text/code
is captured but is not compared where both sides error. No Rust panics occurred.
The already documented regexp 1139-to-1105 error mapping remains visible in
SQL output, but is not counted as a new finding here.

## Evidence and exact validation commands

All raw matrices and difference lists are compressed losslessly. Positional
inputs encode NULL as `n`, integer as `i<decimal>`, and strings as `s<hex>`.
Output TSVs encode values as `value:<hex>`; errors retain hex-encoded details.
Batch rows include shape key, route, result, error, warning count and warnings.
`sql-rust.tsv` records the native SQL observations. `verify.txt` checks hashes,
case identities, both comparison totals and Go route consistency.

From the repository root:

```sh
python3 rust/docs/planner/projection-positional-audit-20260929/verify.txt
python3 rust/docs/planner/projection-positional-audit-20260929/verify.txt --prepare
python3 /private/tmp/build-positional-cases.py
python3 /private/tmp/run-positional-rust.py
python3 /private/tmp/compare-positional.py
python3 /private/tmp/run-batch-cast.py
python3 /private/tmp/compare-batch-cast.py
python3 /private/tmp/run-regexp-sql.py
```

The Rust runners execute:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib audit_positional_boundaries -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib audit_unsigned_decimal_paths -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --test all audit_regexp_sql_boundary -- --test-threads=1
```

Run fresh Go collection from `/private/tmp/tidb-go-structural-20260927`:

```sh
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/positional-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustPositionalAudit$' -count=1 -v
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/batch-cast-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustBatchCastAudit$' -count=1 -v
```

The failpoint runner skill/workflow was used; both final Go runs disabled
failpoints and returned refcount zero. The direct Go vector oracle calls
`Vectorized()` before VecEvalDecimal, as the evaluator does, to initialize the
buffer allocator. An initial probe omitted that preflight and panicked; those
observations were replaced, not reported as Go defects. Temporary Rust probes
initially needed import/setter/decimal serialization corrections before compiling.
Final harnesses and successful log excerpts are preserved. Capture tests passing
means observations were recorded, not that parity passed.

The runners insert probes temporarily and restore source in `finally`; use a
clean checkout without concurrent edits to those files. The prepare command
writes only the listed temporary artifacts; adjust archive paths on another host.

Only audit documents/evidence changed. No new `make lint` run is claimed. Both
required locked server builds must pass: `cd rust && cargo build --locked -p
tidb-server` in the pre-commit hook and again immediately before push.

Not verified: complete package parity, every planner-generated type/UNION shape,
all multi-row warning/error ordering, every regexp/collation/input, full Go SQL
server execution, distributed transport, or sysbench/TPCC/TPCH/YCSB performance.
This audit has no runtime effect. The findings remain correctness and
compatibility risks, particularly optimization-dependent results; it makes no
measured performance claim.
