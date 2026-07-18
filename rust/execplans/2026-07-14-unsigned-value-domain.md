# Make unsigned integers a first-class Rust runtime value

This ExecPlan is a living document. Keep `Progress`, `Decision Log`, and
`Validation` current while the implementation proceeds.

## Purpose

The current `Value::Int(i64)` domain cannot represent TiDB's UInt64 runtime
contracts. It already blocks `LAST_INSERT_ID`, `sql_select_limit`, unsigned
column bounds, `BIT_COUNT`, and mixed signed/unsigned comparison. Worse, the
Go differential helper labels `KindUint64` as signed `INT`, hiding drift.

The target is a source-compatible value domain:

```rust
pub enum Value {
    Int(i64),
    UInt(u64),
    Str(String),
    Decimal(Decimal),
    Float(f64),
    Null,
}
```

Signedness belongs in the runtime value, not in a side boolean and not in a
string rendering workaround.

## Progress

- [x] Audited Go signed/unsigned semantics, existing Rust assumptions, and
  differential-oracle masking.
- [x] Add `Value::UInt` and compiler-drive every exhaustive `tidb-expr`
  match to a deliberate behavior.
- [x] Correct literals, formatting, decimal/float promotion, comparisons,
  equality, and bit semantics in the value-domain layer.
- [x] Correct the Go differential labeler and regenerate affected checked
  corpus outputs, including an explicit UInt64 boundary corpus.
- [x] Port `LAST_INSERT_ID` end-to-end: statement-status promotion, both
  function forms, `@@last_insert_id`/`@@identity` UInt readback, rollback
  persistence, error-path persistence, and source corpus.
- [ ] Propagate unsigned values through the remaining executor paths. Session
  state, ordering, strict `INT`/`BIGINT` DML coercion/storage, and bounded
  `AUTO_INCREMENT` allocation are done; other integer widths, non-strict
  conversion, custom auto-ID increment/offset, and unsigned columns beyond
  this seed slice remain.
- [x] Port `sql_select_limit`: UInt64 default/readback, source clamping and
  rollback behavior, and implicit LIMIT injection only when SELECT/set-op has
  no explicit LIMIT.
- [ ] Port the remaining source-derived unsigned acceptance set, including
  unsigned DML columns.

## Decision Log

- Decision: do not port `LAST_INSERT_ID` or `sql_select_limit` with signed
  wraps or string readback.
  Rationale: both are source-defined UInt64 contracts; either workaround
  would make the seed observably diverge from TiDB.

- Decision: centralize only domain-neutral integer primitives in `value.rs`:
  mixed comparison, bit interpretation, decimal conversion, and float
  conversion. Owning operations retain their own coercion/error policies.
  Rationale: `CAST AS UNSIGNED`, `LAST_INSERT_ID(expr)`, and column assignment
  have different TiDB validation/warning behavior and must not share a broad
  signed-to-unsigned coercion helper.

- Decision: unary minus of an unsigned literal does not wrap into `UInt`.
  It returns `Int` when its magnitude fits (including `-2^63`) and exact
  `Decimal` above that range.
  Rationale: direct Go oracle evaluation returns
  `DEC:-18446744073709551615` for `-18446744073709551615`.

- Decision: model `LAST_INSERT_ID(expr)` with a current-statement pending
  value and promote it at the next top-level statement boundary, rather than
  mutating its readable value immediately.
  Rationale: `pkg/executor/select.go:1224-1229` copies `LastInsertID` into
  `PrevLastInsertID` while preparing the following statement. Direct `gorun`
  confirms `SELECT LAST_INSERT_ID(5), LAST_INSERT_ID()` returns `5|0`, then a
  following statement reads `5`.

- Decision: keep `AUTO_INCREMENT` cursor state out of `Table` and all
  transaction/savepoint snapshots, retaining only an immutable resolved column
  index in the table schema.
  Rationale: Go allocates before duplicate/FK outcomes and rollback never
  returns an ID. A separate `TableKey -> Option<u64>` cursor makes gaps,
  `u64::MAX` exhaustion, truncate reset, and rename/drop lifecycle explicit
  rather than special-casing snapshot restoration.

- Decision: keep `LAST_INSERT_ID(expr)` coercion local to its builtin:
  integers preserve raw bits, decimal/float inputs use source `EvalInt`
  rounding, and strings consume only a leading signed integer run.
  Rationale: `gorun` confirms `1.5` becomes `2`, `'-1.9'` becomes
  `u64::MAX`, and `'1e2tail'` becomes `1`; sharing a generic UInt cast would
  incorrectly apply another feature's coercion or warning policy.

## Ownership

| Owner | Exclusive scope |
| --- | --- |
| Value steward | `tidb-expr` value/literal/ops/cast core and datatype decimal conversion |
| Executor steward | session, DML range validation, order/aggregate/window propagation |
| Evidence steward | `goeval`, differential labels, corpus and ledger migration |
| Feature agents | Builtins only after central `UInt` helpers compile |

## Implementation

First add `UInt(u64)` and an internal `Integer` helper in `tidb-expr`.
Mixed comparison must rank negative `Int` below every `UInt`; non-negative
`Int` compares as `u64`. Bit operators use raw bits. Decimal and float
conversion accept both variants without silently wrapping.

Then change SQL integer literal evaluation to parse through `u64`: values up
to `i64::MAX` remain `Int`; larger valid literals become `UInt`; values above
`u64::MAX` error. Update every exhaustive `Value` match under compiler
guidance; an omitted match is not permission to coerce through string.

Then repair `goeval` so Go `KindUint64` emits `UINT:<decimal>`, regenerate
only affected golden artifacts, and add source-derived mixed-domain tests.

Finally propagate the domain into session values, result labels, ordering,
aggregates/windows, DML assignment/range checking, and unsigned columns.
`LAST_INSERT_ID` has consumed the session-status seam and `sql_select_limit`
has consumed the top-level implicit-LIMIT seam. Unsigned columns stay a
distinct later wave.

## Validation

The acceptance set includes Go `TestLastInsertID`, sysvar `TestLastInsertID`,
`TestBitCount`, source-backed mixed signed/unsigned comparison and null-safe
equality, cast/bit/shift/IN/order/aggregate/window tests, unsigned-column
insertion bounds, and differential output that distinguishes `INT` from
`UINT`.

At each coherent sub-milestone run focused crate tests, strict Clippy, the
affected differential corpus, and `go_test_ledger --check`. Before claiming a
completed domain migration, run the workspace WIP ring and ensure no unsigned
test is still recorded as blocked for the signed-only value reason.

### 2026-07-14 phase-1 evidence

- `cargo check -j 12 -p tidb-expr`
- `cargo test -j 12 -p tidb-expr -q`
- `cargo test -j 12 -p difftest --test expr_diff -- --nocapture`
- Direct `go run ./difftests/goeval` probes for literal, cast, unary-minus,
  bitwise, and mixed signed/unsigned comparison boundaries.

### 2026-07-14 phase-2 scalar closure: BIT_COUNT

- `pkg/expression/builtin_other_test.go:32 TestBitCount` is mapped as
  `COVERED` by `difftests/corpus/expr/bit_count_source.txt` and the focused
  `string_fn::bit_count_matches_go_source_vectors` regression test. The
  source test's signed, float, UInt64-max, invalid-string, and NULL vectors
  are all represented. The direct source rows additionally pin the ETInt
  string-cast boundaries: `2^63` preserves one set bit through the unsigned
  cast, positive overflow clamps to `UINT64_MAX`, negative overflow clamps to
  `INT64_MIN`, and a malformed UTF-8 suffix does not erase an ASCII prefix.
- The production fragment is intentionally `PARTIAL`: the pure Datum path is
  source-faithful for these value boundaries, while function-class construction,
  vectorized execution, and statement warning state still belong to the shared
  evaluator-context workstream.
- The required failpoint wrapper attempted the exact upstream test but the
  local Go arm64 linker crashed in `cmd/link/internal/arm64.gensymlate`
  before execution; wrapper cleanup restored failpoint refcount to zero.
- `go_test_ledger --write` and `--check` pass after the phase-3 source-anchor
  update; the generated inventory remains the required verification source.

### 2026-07-14 phase-3 session closure: LAST_INSERT_ID

- `LAST_INSERT_ID(expr)` now records a pending `u64` in the statement session
  state; `Database::run` promotes it at the next statement boundary. The
  reader form and `@@last_insert_id`/`@@identity` read only the promoted value.
- The table corpus covers same-statement separation, `-1 -> u64::MAX`, a
  max-UInt literal, decimal/string coercion, NULL, rollback, a later
  evaluation error, and the session-only GLOBAL boundary. It is generated
  from `gorun` and mapped to the three source test anchors in the ledger.

### 2026-07-14 phase-4 session/planner closure: sql_select_limit

- `sql_select_limit` is a session `u64`, with `u64::MAX` retained as the
  source default/no-limit sentinel and as a `Value::UInt` sysvar readback.
  Negative assignments normalize to zero; oversized/fractional inputs fail
  before replacing a prior valid setting; it is nontransactional across
  rollback.
- `Database::run_query` applies the cap only to an outer SELECT or set
  operation with no explicit LIMIT. An explicit outer LIMIT remains decisive;
  limits inside a nested/individual set-op term retain their existing scope.
  The corpus covers query, set-op, error, and global-read boundaries. The
  executor deliberately rejects mutable GLOBAL settings because no shared
  global-variable store exists.

### 2026-07-14 phase-5 DML closure: INT/BIGINT unsigned storage

- `ColumnType { name, unsigned }` remains the only schema source. The shared
  DML coercion funnel now classifies `INT`/`BIGINT` and stores signed values as
  `Value::Int` and unsigned values as `Value::UInt`, so primary-key equality,
  ORDER BY, and downstream expressions preserve the numeric domain.
- The strict assignment path covers VALUES/SET/SELECT insert sources, omitted
  defaults, single/multi-table UPDATE, source width boundaries, decimal/string
  half-away rounding, and DOUBLE ties-to-even rounding. It rejects negatives
  and out-of-range values before a failed row/update mutates storage. The
  seed deliberately does not claim non-strict `sql_mode` saturation/warnings
  or TINYINT/SMALLINT/MEDIUMINT coercion yet.

### 2026-07-14 phase-6 executor closure: bounded AUTO_INCREMENT

- `AUTO_INCREMENT` schema is exactly one declared `INT`/`INTEGER`/`BIGINT`
  column (signed or unsigned), recorded as immutable catalog metadata. Its
  allocator is a separate nontransactional `TableKey -> Option<u64>` map:
  `NULL`/zero INSERT requests allocate, explicit positive INSERT/UPDATE/ON
  DUPLICATE values rebase, `u64::MAX` becomes an explicit exhausted state,
  and failed/ignored conflicts still consume as Go does.
- CREATE's `AUTO_INCREMENT = N` table option uses Go's legacy signed
  `TableInfo.AutoIncID` carrier (`pkg/ddl/create_table.go:952-953`): zero or
  raw UInt64 values above `i64::MAX` begin at 1. This is intentionally not
  conflated with the distinct unsigned ALTER TABLE rebase path.
- The table corpus covers table start, multi-row first-ID status, plain-error
  vs IGNORE status, duplicate gaps, explicit rebase, rollback gaps, UPDATE
  rebase/zero behavior, and TRUNCATE/RENAME/DROP lifecycle. The deliberately
  bounded unported seam is non-default `auto_increment_increment` and
  `auto_increment_offset`; it remains a session-variable implementation task,
  not an inferred allocator default.

### 2026-07-14 phase-2 scalar closure: CRC32

- `pkg/expression/builtin_math_test.go:543 TestCRC32` is mapped as
  `PARTIAL` by `difftests/corpus/expr/crc32_source.txt` and the focused
  `tests::math::crc32_matches_go_utf8_source_vectors` regression test. The
  production function, helper, and test now share the `math_fn` source owner;
  the full UTF-8 scalar slice preserves `UINT` result labels.
- The two GBK connection-charset vectors remain outside the current UTF-8
  `Value::Str` domain. They require an executor-owned charset value layer,
  not a lossy scalar workaround.

### 2026-07-14 phase-2 scalar closure: ABS

- `pkg/expression/builtin_math_test.go:35 TestAbs` is mapped as `COVERED`
  by `difftests/corpus/expr/abs_source.txt` and the focused
  `tests::abs_source_vectors_preserve_uint` regression. Its UInt input
  remains `UINT:1`, rather than entering the signed `ABS` path.

### 2026-07-14 phase-2 scalar closure: LEAST/GREATEST

- `pkg/expression/builtin_compare_test.go:286 TestGreatestLeastFunc` is
  mapped as `PARTIAL` by
  `difftests/corpus/expr/greatest_least_unsigned_source.txt` and the focused
  `tests::greatest_least_source_vectors_preserve_mixed_integer_result_domain`
  regression. A mixed signed/UInt integer argument list promotes its result
  to DECIMAL; an all-UInt list retains UInt.
- The remaining source rows depend on typed time/duration values, expression
  errors, or mixed string-numeric coercion, which are outside this scalar
  `Value` domain and are intentionally not imitated with string fallbacks.

### 2026-07-14 scalar closure: ANY_VALUE

- `pkg/expression/builtin_miscellaneous_test.go:240 TestAnyValue` is mapped
  as `COVERED` by `difftests/corpus/expr/any_value_source.txt` and the
  focused `tests::any_value_source_vectors_preserve_value_labels` regression.
  The complete source table is representable in the scalar `Value` domain;
  `ANY_VALUE` preserves its NULL, signed integer, float, and string labels.

### 2026-07-14 phase-2 scalar closure: unary minus

- `pkg/expression/builtin_op_test.go:30 TestUnary` is mapped as `COVERED` by
  `difftests/corpus/expr/unary_minus_source.txt` and the focused
  `tests::unary_minus_source_vectors_preserve_uint_overflow_domain`
  regression. The `Int::MIN` negation edge promotes to exact DECIMAL, so the
  source's double-minus vector cannot silently wrap back to `Int::MIN`.

### 2026-07-14 scalar closure: LIKE

- `pkg/expression/builtin_like_test.go:30 TestLike` is mapped as `COVERED`
  by `difftests/corpus/expr/like_source.txt` and the focused
  `tests::like_source_vectors_preserve_default_escape_semantics` regression.
  The complete source table confirms default backslash escaping, `%`/`_`
  matching, and byte-sensitive case behavior through the parser and evaluator.
