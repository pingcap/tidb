# TiDB FULL OUTER JOIN (Phase 1: HashJoin v1) Development Notes

Last updated: 2026-02-23

---

## 1. Goal

Add `FULL OUTER JOIN` support in TiDB with a safe and staged rollout.

Phase 1 target:

- Volcano planner path only
- Root HashJoin v1 only
- `FULL OUTER JOIN ... ON ...` only
- Feature-gated by `tidb_enable_full_outer_join` (default `OFF`)

---

## 2. Scope and Non-Goals

### In Scope (Phase 1)

- Parser/AST support for `FULL OUTER JOIN`
- Logical and physical plan support for `FullOuterJoin`
- Correct nullability behavior for output schema
- Correct predicate handling for preserved rows on both sides
- HashJoin v1 executor support for:
  - matched rows
  - unmatched left rows
  - unmatched right rows
  - `=` and `<=>` join key semantics

### Out of Scope (Phase 1)

- `NATURAL FULL OUTER JOIN`
- `FULL OUTER JOIN ... USING (...)`
- Cascades planner support
- HashJoin v2 support
- MergeJoin support
- IndexJoin support
- TiFlash MPP / coprocessor pushdown for full join

---

## 3. Why This Is Not "Just Add One Joiner"

TiDB HashJoin v1 was originally structured around a single preserved side (`LEFT OUTER` or `RIGHT OUTER`).

`FULL OUTER JOIN` requires both sides to be preserved:

- unmatched left rows must be output
- unmatched right rows must be output

That requires more than one new joiner type:

- side-specific filter semantics in ON conditions
- accurate build-side match tracking
- probe-side unmatched output in addition to build-side tail output

As a result, this feature touches parser, planner, optimizer rules, and executor together.

---

## 4. SQL Semantics Baseline

### 4.1 Nullability

In full join output, both sides can produce NULL-extended rows.
Therefore, all output columns from both sides must be nullable.

### 4.2 ON condition classification

- `EqualConditions`: two-side column equality conditions extracted as join keys (`=` or `<=>`)
- `LeftConditions` / `RightConditions`: one-side ON filters
- `OtherConditions`: remaining two-side or complex conditions

For full join, one-side ON filters are preserved-side filters and must not be pushed down as child selections.

### 4.3 `=` vs `<=>`

- `=`: NULL join keys do not match
- `<=>`: `NULL <=> NULL` is true, so NULL keys can match

In hash join logic, this is represented by per-key null-safe behavior (`IsNullEQ`).

---

## 5. Key Design Decisions (Phase 1)

### 5.1 Syntax policy

Only `FULL OUTER JOIN` is recognized as full join syntax.
`FULL JOIN` remains interpreted under MySQL-compatible alias behavior.

Reason: `FULL` is an unreserved keyword and can be ambiguous as an identifier/alias.

### 5.2 Planner safety gate

If parser/ast support lands before full planner+executor semantics are complete,
the planner must fail fast for full join instead of silently degrading to inner join.

Current behavior:

- switch OFF: `ErrNotSupportedYet("FULL OUTER JOIN")`
- cascades ON: `ErrNotSupportedYet("FULL OUTER JOIN with cascades planner")`

### 5.3 Build/probe direction and execution path

Planner may still enumerate build-side directions for cost.
Executor keeps full join on the normal HashJoin v1 path (not the `UseOuterToBuild=true` outer-specific path),
then explicitly handles unmatched outputs for both sides.

### 5.4 Two-joiner strategy in executor

To avoid broad joiner interface changes, full join behavior is implemented by composing existing joiner behaviors:

- a build-side outer joiner for build unmatched tail output
- a probe-side outer joiner for probe unmatched output

Matched-row handling and build match bitmap updates are derived from actual post-filter matches
(not just hash bucket hit existence).

### 5.5 Full-join side filters in executor

One-side ON filters are represented as dedicated side filters:

- `BuildFilter`
- `ProbeFilter`

This avoids overloading a single `OuterFilter` semantics that is tied to one-side outer joins.

---

## 6. Implementation Summary

### 6.1 Parser / AST

- `FULL OUTER JOIN` parsing and AST join type support added
- `FULL JOIN` shorthand intentionally not introduced for full join semantics

### 6.2 Planner

- Logical join type includes `FullOuterJoin`
- Full join nullability behavior corrected (both sides nullable)
- Predicate pushdown preserves one-side ON conditions at join level
- Join reorder and outer-join simplification/conversion paths guarded for full join semantics
- Physical plan constrained to root HashJoin for full join
- MPP/ToPB full join path blocked

### 6.3 Executor (HashJoin v1)

- Full join matched and unmatched semantics implemented
- Build/probe side filters applied in full join path
- Build-side match tracking and tail unmatched output in place
- Probe-side unmatched output in place
- `=` and `<=>` key semantics covered
- Spill path covered by tests

### 6.4 Feature switch

System variable:

- Name: `tidb_enable_full_outer_join`
- Scope: `GLOBAL | SESSION`
- Type: boolean
- Default: `OFF` (`false`)

Planner gate enforces this switch before allowing full join planning.

---

## 7. Current Status

Completed:

- Parser/AST support for `FULL OUTER JOIN`
- Planner fail-fast safety and proper full-join planning path
- Logical/physical full-join semantics (including nullable output)
- Predicate behavior for full join
- Root HashJoin-only restriction for full join
- HashJoin v1 full-join executor behavior
- Safety handling in join reorder / simplify / convert rules
- Unit and integration coverage for key full-join cases
- `tidb_enable_full_outer_join` switch with default OFF

Remaining (Phase 1 out-of-scope by design):

- `NATURAL FULL OUTER JOIN`
- `FULL OUTER JOIN ... USING (...)`
- Cascades planner full-join support
- HashJoin v2 / MergeJoin / IndexJoin / MPP full-join support

---

## 8. Test Strategy and Validation Principles

### 8.1 Correctness-first principles

- No silent fallback from full join to inner join
- Preserve both sides for unmatched output
- Keep behavior consistent for `=` and `<=>`
- Ensure one-side ON filters do not break preserved-side semantics

### 8.2 Recommended minimum coverage

- Basic matched + left-unmatched + right-unmatched
- Duplicate keys (many-to-many)
- NULL-key behavior for `=` and `<=>`
- `OtherConditions` fully filtering matches in a bucket
- One-side ON filters retained in join operator
- Spill scenario
- Plan-shape checks (full join chooses only supported physical path)

### 8.3 Oracle-style verification idea

For SQL-level validation, compare full join results against a rewrite-based equivalent query plan where feasible.
This provides stronger evidence than hardcoding only one expected result set in some unit scenarios.

---

## 9. Risk Checklist

- Predicate pushdown mis-handling of preserved-side filters
- Nullability propagation mistakes (NotNull flags not fully cleared)
- Accidental full join pushdown to unsupported engines
- Incorrect NULL handling for `<=>` join keys
- Memory/performance sensitivity with large duplicate-key fanout

---

## 10. Practical Review Checklist

When reviewing future full-join changes, verify:

1. Full join does not silently degrade to inner join.
2. Both sides can emit unmatched rows.
3. Side filters stay in join semantics (not child pushdown).
4. `=` and `<=>` semantics both remain correct.
5. Unsupported paths still fail fast (`USING`, `NATURAL`, cascades, unsupported engines).
6. Tests cover at least one spill and one NULL-sensitive case.
